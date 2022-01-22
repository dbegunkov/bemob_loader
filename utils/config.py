import pathlib
import os
import configparser


class Config:

    @classmethod
    def initialiseConfig(cls):
        config = configparser.ConfigParser()
        script_path = pathlib.Path(__file__).parent.absolute()
        config.read(os.path.join(script_path, '../config/settings.ini'))
        return config
