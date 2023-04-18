import hydra
from omegaconf import DictConfig, OmegaConf
from seml.hydra import seml_observe_hydra

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