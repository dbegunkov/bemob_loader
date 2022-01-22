import logging
import os
import sys
import datetime
import pathlib
from logging.handlers import RotatingFileHandler


class Logger:

    @classmethod
    def initialiseLogging(cls, fileName, config):
        cls.loggerOutput = config.get('LOGGING', 'type')
        cls.loggerLevel = 10 if config.get('LOGGING', 'level') is None else config.get('LOGGING', 'level')
        cls.loggerFormatter = '%(asctime)s %(filename)s:%(lineno)d %(levelname)s\t%(message)s'

        if cls.loggerOutput == 'file':
            path = os.path.join(pathlib.Path(__file__).parent.absolute(), "..", "logs", fileName)
            handler_rotation = RotatingFileHandler(path, maxBytes=10 * 1024 * 1024, backupCount=10)
            cls.handler = handler_rotation
        else:
            cls.handler = logging.StreamHandler(sys.stdout)
        cls.handler.setLevel(cls.loggerLevel)
        cls.handler.setFormatter(logging.Formatter(cls.loggerFormatter))

    @classmethod
    def addLogger(cls, logger):
        logger.setLevel(logging.DEBUG)
        logger.addHandler(cls.handler)

    @classmethod
    def addLoggers(cls, loggers):
        for logger in loggers:
            logger.setLevel(logging.DEBUG)
            logger.addHandler(cls.handler)

    @classmethod
    def logErrorAndContinue(cls):
        return

    @classmethod
    def logErrorAndStop(cls, timer):
        cls.stopTimer(timer)
        raise ValueError()

    timer = {}

    @classmethod
    def startTimer(cls, instance=0):
        cls.timer[instance] = datetime.datetime.now()

    @classmethod
    def stopTimer(cls, instance=0):
        cls.timer[instance] = (datetime.datetime.now() - cls.timer[instance]).total_seconds()
        return cls.timer[instance]

    @classmethod
    def readTimer(cls, instance=0):
        return cls.timer[instance]

    @classmethod
    def printTimers(cls):
        timersResult = 'Timers: '
        for timer in cls.timer:
            timersResult += str(timer) + ' = ' + str(cls.timer[timer]) + 's, '
        timersResult = timersResult[:-2]
        return timersResult