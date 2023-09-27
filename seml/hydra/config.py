from omegaconf import OmegaConf
import ast
from pathlib import Path
import textwrap
from os import PathLike
from typing import Sequence, List, Dict, Tuple
from seml.errors import ConfigError
from seml.utils import flatten as flatten_config, unflatten, working_directory
import multiprocessing
import sys, importlib
import logging
from seml.settings import SETTINGS

def run_imports_from_file(path: PathLike):
    """ Runs all import statements from the abstract syntax tree of a python script

    Args:
        path (PathLike): the filepath of the script to run imports of
    """
    with open(path) as fh:        
       root = ast.parse(fh.read(), path)
    
    for node in ast.walk(root):
        if isinstance(node, (ast.Import, ast.ImportFrom)):
            exec(ast.unparse(node))
            
def detect_hydra_arguments_from_main_decorator(path: PathLike, config_path: PathLike | None = None,
                                               config_name: str | None = None, version_base=None) -> Tuple[PathLike | None, str | None, str | None]:
    """Very hacky function that detects the arguments that `hydra.main` decorator is called with

    Args:
        path (PathLike): the path of the main python script of the experiment

    Returns:
        Tuple[PathLike | None, str | None, str | None]: config_path, config_name and version_base arguments
    """
    import hydra
    # Hacky way: Temporarily replace `hydra.main` with an exception that returns its arguments and
    # execute the executable, which should throw upon calling `hydra.main`. From this exception
    # we can infer the arguments
    with open(path) as fh: 
        
        class __HydraDetectArgumentsFromDecoratorException(Exception):
            ...
            
        _hydra_main_backup = hydra.main
        content = textwrap.dedent("""
        import hydra
        
        def __hydra_main(config_path=None, config_name=None, version_base=None):
            raise _HydraDetectArgumentsFromDecoratorException([config_path, config_name, version_base])
        
        hydra.main = __hydra_main
        """) + fh.read()
        try:
            exec(content, {'_HydraDetectArgumentsFromDecoratorException' : __HydraDetectArgumentsFromDecoratorException})
            raise RuntimeError(f'Running {path} did not call `hydra.main`')
        except __HydraDetectArgumentsFromDecoratorException as e:
            config_path_detected, config_name_detected, version_base_detected = e.args[0]
        hydra.main = _hydra_main_backup
        
        if config_name is None:
            config_name = config_name_detected
        elif config_name != config_name_detected:
            raise ConfigError(f"'hydra' configuration specifies 'config_name' : '{config_name}' but the exectuable {path}"
                              f" is called with '{config_name_detected}'")
            
        if config_path is None:
            config_path = config_path_detected
        elif config_path != config_path_detected:
            raise ConfigError(f"'hydra' configuration specifies 'config_path' : '{config_path}' but the exectuable {path}"
                              f" is called with '{config_path_detected}'")
            
        if version_base is None:
            version_base = version_base_detected
        elif version_base != version_base_detected:
            raise ConfigError(f"'hydra' configuration specifies 'version_base' : '{version_base}' but the exectuable {path}"
                              f" is called with '{version_base_detected}'")
        
        return Path(config_path), config_name, version_base

def _resolve_hydra_configs(directory: PathLike, executable: PathLike,
                         overrides_per_config: Sequence[List[str]], result: Dict):
    from hydra import compose, initialize_config_dir
    with working_directory(directory):
        # run_imports_from_file(executable) # This makes sure that the base configurations are registered in hydra's ConfigStore singleton
        # This runs the script once, so resolvers will be registered and structure config dataclasses imported
        config_path, config_name, version_base = detect_hydra_arguments_from_main_decorator(executable)
        
        with working_directory(Path(executable).parent):
            with initialize_config_dir(str(config_path.absolute()), version_base=version_base):
                configs = []
                for overrides in overrides_per_config:
                    flat_keys = [override.split('=')[0] for override in overrides]
                    cfg = compose(config_name=config_name, overrides=overrides)
                    try:
                        cfg = OmegaConf.to_container(cfg, resolve=True, throw_on_missing=True, enum_to_str=True)
                    except Exception as e:
                        print('Error for overrides', overrides)
                        raise e
                    cfg = {k : v for k, v in flatten_config(cfg).items() if k in flat_keys}
                    configs.append(unflatten(cfg))
                    
    result['configs'] = configs
    result['config_path'] = str(Path(executable).parent / config_path)

def resolve_hydra_configs(directory: PathLike, executable: PathLike,
                         configs: Sequence[Dict]) -> Tuple[List[Dict], str]:
    """Takes configurations and resolves them using hydra to get absolute values after interpolation takes
    place.

    Args:
        directory (PathLike): the working directory. Hydra config paths are relative to this directory
        executable (PathLike): the executable script
        configs (Dict): a sequence of flat dicts, each of which represents one configuration parsed from seml

    Returns:
        List[Dict]: the configurations after resolving by hydra, with no interpolation values and only absolute values
        PathLike: Path to the configuration files. These need to be added to the source files.
    """
    logging.info('Resolving hydra configs, this will launch your experiment without calling the `hydra.main` decorated function and can take some time.')
    # We run this in a new process to not tamper with the current context and resolve the configurations in an isolated context
    # e.g. for OmegaConf's resolvers it is important, that they are not registered multiple times, so we actually *need* a separate context
    # when running the hydra framework around the exectuable
    overrides_per_config, group_overrides_per_config = zip(*[config_to_hydra_overrides(config) for config in configs])
    
    manager = multiprocessing.Manager()
    ret_list = manager.dict()
    proc = multiprocessing.Process(target=_resolve_hydra_configs, args=(directory, executable, overrides_per_config, ret_list))
    proc.start()
    proc.join()
    configs = [config | group_overrides for config, group_overrides in zip(ret_list['configs'], group_overrides_per_config) ]
    return configs, ret_list['config_path']
    

def config_to_hydra_overrides(config: Dict) -> Tuple[List[str], Dict]:
    """transforms a (non flat) configuration to a sequence of overrides for the hydra CLI

    Args:
        config (Dict): the config to transform

    Returns:
        List[str]: the sequence of hydra CLI overrides
        List[str]: the sequence of hydra CLI group overrides
    """
    result = []
    result_group_arguments = {}
    for key, value in flatten_config(config).items():
        if f'{SETTINGS.HYDRA_GROUP_ARGUMENTS_PREFIX}.' in key:
            result_group_arguments[key] = value
            key = key.replace(f'{SETTINGS.HYDRA_GROUP_ARGUMENTS_PREFIX}.', '')
        result.append(f'{key}={value}')
    return result, result_group_arguments
    

    
    