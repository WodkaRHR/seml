import hydra
from hydra.core.config_store import ConfigStore
from omegaconf import DictConfig, OmegaConf
from seml.hydra import seml_observe_hydra
from dataclasses import dataclass
from enum import StrEnum, Enum, unique

@unique
class BarType(StrEnum):
    LOL = 'loll'

@dataclass
class TestCfg:
    foo: BarType = BarType.LOL
    
cs = ConfigStore.instance()
cs.store(name='base_test', node=TestCfg, group='test')

OmegaConf.register_new_resolver('eval', eval)

def print_config(config: DictConfig) -> None:
    content = OmegaConf.to_yaml(config, resolve=True)
    print(content)

@hydra.main(config_path='hydra_config', config_name='config', version_base=None)
@seml_observe_hydra()
def main(config: DictConfig) -> float:
    OmegaConf.resolve(config)
    print_config(config)
    return 1.0

if __name__ == '__main__':
    main()