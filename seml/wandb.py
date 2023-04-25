from seml.database import get_collection
import logging

# Note that this requires `wandb` to be installed

def experiment_set_wandb(run: 'wandb_sdk.wandb_run.Run', db_collection_name: str | None, 
                         experiment_id: int | None,
                         set_dir: bool = True, set_entity: bool = True,
                         set_group: bool = True, set_id : bool = True,
                         set_mode: bool = True, set_name: bool = True,
                         set_notes: bool = True, set_path: bool = True,
                         set_project: bool = True, set_resumed: bool = True,
                         set_start_time: bool = True, set_sweep_id: bool = True,
                         set_tags: bool = True, set_url: bool = True):
    """ Updates the experiment in the database with attributes from the currently active wandb run. 
    This allows for easier matching of wandb and seml experiments. """
    if db_collection_name is None or experiment_id is None:
        logging.warn(f'Can not set wandb run information to seml database as no seml experiment is specified')
        return
    if run is None:
        raise RuntimeError(f'Can not set wandb run information to seml database, as `run` is `None`')
    updates = {}
    if set_dir:
        updates['run.dir'] = run.dir
    if set_entity:
        updates['run.entity'] = run.entity
    if set_group:
        updates['run.group'] = run.group
    if set_id:
        updates['run.id'] = run.id
    if set_mode:
        updates['run.mode'] = run.mode
    if set_name:
        updates['run.name'] = run.name
    if set_notes:
        updates['run.notes'] = run.notes
    if set_path:
        updates['run.path'] = run.path
    if set_project:
        updates['run.project'] = run.project
    if set_resumed:
        updates['run.resumed'] = run.resumed
    if set_start_time:
        updates['run.start_time'] = run.start_time
    if set_sweep_id:
        updates['run.sweep_id'] = run.sweep_id
    if set_tags:
        updates['run.tags'] = run.tags
    if set_url:
        updates['run.url'] = run.url
    if len(updates):
        collection = get_collection(db_collection_name)
        result = collection.update_one({"_id": int(experiment_id)}, {'$set' : updates})
        if result.matched_count != result.modified_count:
            logging.warn(f'Setting wandb to seml experiments matched {result.matched_count} with id {experiment_id} in collection '
                         f'{db_collection_name} but modified {result.modified_count}')
        if result.matched_count == 0:
            logging.error(f'Did not find any seml experiment with {experiment_id} in collection {db_collection_name} to update wandb.')
        if result.modified_count != 1:
            logging.error(f'Modified {result.modified_count} (more than one) experiment with current wandb.')
            