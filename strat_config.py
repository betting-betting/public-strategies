import toml
import pandas as pd

configs = toml.load(r'strat_config.toml')

config_df = pd.DataFrame(columns = ['strategy','direction','size'])

config_df['strategy'] = [key for (key,value) in configs.items()]
config_df['direction'] = [list(value.keys())[0].upper() for (key,value) in configs.items()]
config_df['size'] = [list(value.values())[0] for (key,value) in configs.items()]
