import logging.config
from pyspark.sql.functions import *

logging.config.fileConfig('Properties/configuration/logging.config')

loggers = logging.getLogger('Validate')


def get_current_date(spark):
    try:
        loggers.warning('Started the get_current_date method.')
        output = spark.sql("SELECT current_date")
        loggers.warning("Validating spark object with current date - " + str(output.collect()))

    except Exception as e:
        loggers.error('An error occurred in get_current_date ', str(e))
        raise

    else:
        loggers.warning('Validation Done, go forward...')


def print_schema(df, dfName):
    try:
        loggers.warning(f'print_schema method executing {dfName}')

        sch = df.schema.fields

        for i in sch:
            loggers.info(f'\t{i}')

    except Exception as e:
        loggers.error('An error occurred at print_schema - ', str(e))
        raise

    else:
        loggers.info('print_schema done, do forward...')


def check_for_null(df, dfName):
    try:
        loggers.info(f'check_for_null method executing for {dfName}')

        check_null_df = df.select([count(when(isnan(c) | col(c).isNull(), c)).alias(c) for c in df.columns])

    except Exception as e:
        loggers.error('An error occurred in check_for_null ', str(e))

    else:
        loggers.warning('check_for_null executed successfully.')

        return check_null_df


def display_df(df, dfName):
    df_show = df.show()

    return df_show


def df_count(df, dfName):
    try:
        loggers.warning(f'Here to count the records in {dfName}')

        df_c = df.count()

    except Exception as e:
        raise

    else:
        loggers.warning(f'Number of records present in {df} are : {df_c}')

    return df_c

