# import hydra
# from omegaconf import DictConfig, OmegaConf
# import rich
# from rich.syntax import Syntax
# from hydra import utils
# import os

def print_config(config: DictConfig) -> None:
    content = OmegaConf.to_yaml(config, resolve=True)
    rich.print(Syntax(content, "yaml"))

@hydra.main(config_path='hydra_config', config_name='config', version_base=None)
def main(config: DictConfig) -> float:
    OmegaConf.resolve(config)
    print_config(config)
    print('foo')
    print(os.getcwd())
    print(utils.get_original_cwd())
    return .1


if __name__ == '__main__':
    main()