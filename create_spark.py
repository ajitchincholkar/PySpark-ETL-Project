from pyspark.sql import SparkSession
import logging.config

logging.config.fileConfig('Properties/configuration/logging.config')

logger = logging.getLogger('Create_spark')


def get_spark_object(envn, appName):
    try:
        logger.info('get_spark_object method started')
        if envn == 'DEV':
            master = 'local'

        else:
            master = 'Yarn'

        logger.info('Master is {}'.format(master))

        spark = SparkSession.builder.master(master).appName(appName).getOrCreate()

    except Exception as e:
        logger.error(f'An error occurred in get_spark_object ', str(e))
        raise

    else:
        logger.info('Spark object created...')
    return spark
