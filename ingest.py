import logging.config

logging.config.fileConfig('Properties/configuration/logging.config')

logger = logging.getLogger('Ingest')


def load_files(spark, file_dir, file_format, header, inferSchema):
    try:
        logger.warning('load_files method started...')

        if file_format == 'parquet':
            df = spark.read.format(file_format) \
                .load(file_dir)

        elif file_format == 'csv':
            df = spark.read.format(file_format) \
                .option('header', header) \
                .option('inferSchema', inferSchema) \
                .load(file_dir)

    except Exception as e:
        logger.error('An error occurred at load_files ', str(e))
        raise

    else:
        logger.warning(f'Dataframe created successfully which is of {file_format}')

    return df
