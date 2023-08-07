import logging.config
from pyspark.sql.functions import *
from pyspark.sql.types import *

logging.config.fileConfig('Properties/configuration/logging.config')

logger = logging.getLogger('Save_data')


def save_to_disk(df, format, file_path, split_no, headerReq, compression_type):
    try:
        logger.warning('save_to_disk method execution started...')

        df.coalesce(split_no).write.mode("overwrite") \
            .format(format) \
            .option('header', headerReq) \
            .save(file_path, compression=compression_type)

    except Exception as e:
        logger.warning('An error occurred at save_to_disk method ', str(e))
        raise

    else:
        logger.warning('save_to_disk method executed successfully.')
