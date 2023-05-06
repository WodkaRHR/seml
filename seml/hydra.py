from typing import Callable, Optional, List
from functools import wraps
import datetime
import traceback as tb
import sys
import logging
from dataclasses import dataclass
import json

from omegaconf import DictConfig, OmegaConf

from seml.settings import SETTINGS
from seml.observers import create_mongodb_observer
from seml.database import get_collection

States = SETTINGS.STATES

@dataclass
class SemlConfig:
    overwrite: str | None = None
    db_collection: str | None = None
    command: str | None = None

def seml_observe_hydra(observers: Optional[List]=None) -> Callable:
    """ Uses sacred.observer instances to observe a hydra experiment that runs outside of Sacred.
    Note that not all callbacks will be triggered by this wrapper, in particular only the following
    will be invoked if appropriate:
    - `started_event`
    - `completed_event`
    - `interrupted_event`
    - `failed_event`
    
    By default, a `MongoObserver` instance will always track the experiment. If it is not present
    in the list, it will be created by default.
    
    Use this decorator as follow to wrap your hydra main function.
    ```python
    @hydra.main(version_base=None)
    @seml_observe_hydra(observers=[...])
    def main(cfg: DictConfig):
        ...
    ```
    
    The decorator connects to the MongoDB database used by `seml` via the `.seml` subconfiguration
    of the hydra configuration which is automatically created by `seml` when submitting experiments.
    If the configuration is not present, the experiment will not be observed. 
    
    
    """
    from omegaconf import DictConfig, OmegaConf
    from sacred.observers import MongoObserver
    
    def make_decorator(func: Callable) -> Callable:
        @wraps(func)
        def decorator(cfg: DictConfig):
            if 'seml' not in cfg or cfg.seml is None or cfg.seml.overwrite is None:
                logging.warn('Main function decorated with hydra seml observer, but seml (experiment) not specified. Function will not be observed.')
                result = func(cfg)
                return result
            
            _id = cfg.seml.overwrite
            nonlocal observers
            if observers is None:
                observers = []
            # The MongoObserver should always be present
            if not any(isinstance(observer, MongoObserver) for observer in observers):
                observers = [create_mongodb_observer(cfg.seml.db_collection, overwrite=cfg.seml.overwrite)] + observers
                
            # The config instance to be saved in MongoDB should be the original one submitted by seml, not `cfg`,
            # which is the OmegaConf built by Hydra
            collection = get_collection(cfg.seml.db_collection)
            run = collection.find_one({"_id": int(cfg.seml.overwrite)})
            
            failed_observers = []
            def observer_call_catch_exceptions(observer, method, *args, **kwargs):
                """ Catches exceptions in the observer call. """
                if observers in failed_observers:
                    return
                try:
                    getattr(observer, method)(*args, **kwargs)
                except Exception as e:
                    failed_observers.append(observer)
                    logging.warning(
                        f"An error ocurred in the '{observer}' observer: {e}\n{e.with_traceback}"
                    )
                    tb.print_tb(e.__traceback__)
            try:
                # `started_event`
                for observer in observers:
                    observer_call_catch_exceptions(
                        observer,
                        'started_event',
                        ex_info = {
                            'base_dir' : '', 
                            'sources' : [],
                            },
                        command = run['seml'].get('command', ''),
                        host_info = {},
                        meta_info = {
                            },
                        config=run['config'],
                        start_time = datetime.datetime.utcnow(),
                        _id=_id,
                    )
                    
                result = func(cfg)
                
                # `completed_event`
                for observer in observers:
                    observer_call_catch_exceptions(
                        observer,
                        'completed_event',
                        stop_time = datetime.datetime.utcnow(),
                        result = result,
                    )
                return result
                
            except (KeyboardInterrupt) as e:
                for observer in observers:
                    observer_call_catch_exceptions(
                        observer,
                        'interrupted_event',
                        interrupt_time = datetime.datetime.utcnow(),
                        status = States.INTERRUPTED[0],
                    )
                raise e
            except BaseException as e:
                for observer in observers:
                    observer_call_catch_exceptions(
                        observer,
                        'failed_event',
                        fail_time = datetime.datetime.utcnow(),
                        fail_trace = tb.format_exception(*sys.exc_info())
                    )
                raise e
            finally:
                # wait for all observers
                for observer in observers:
                    observer.join()              
        return decorator
    
    return make_decorator   

def experiment_set_hydra_config(config: DictConfig, db_collection_name: str | None, 
                         experiment_id: int | None):
    """Sets the `hydra_config` attribute in the experiments MongoDB entry

    Args:
        config (DictConfig): the hydra config, does not have to be resolved but can
        db_collection_name (str | None): which database collection to use
        experiment_id (int | None): which experiment id
    """
    if db_collection_name is None or experiment_id is None:
        logging.warn(f'Can not set hydra config to experiment in MongoDB as no experiment is given')
        return
    
    config = OmegaConf.to_container(config, resolve=True)
    # config = json.loads(json.dumps(config)) # If we can serialize to json, we can serialize to bson
    
    updates = {
        'hydra_config' : config
    }
    if len(updates):
        collection = get_collection(db_collection_name)
        result = collection.update_one({"_id": int(experiment_id)}, {'$set' : updates})
        if result.matched_count != result.modified_count:
            logging.warn(f'Setting hydra config in seml experiments matched {result.matched_count} with id {experiment_id} in collection '
                         f'{db_collection_name} but modified {result.modified_count}')
        if result.matched_count == 0:
            logging.error(f'Did not find any seml experiment with {experiment_id} in collection {db_collection_name} to update with hydra config.')
        if result.modified_count != 1:
            logging.error(f'Modified {result.modified_count} (more than one) experiment with hydra config.')

    
    
    