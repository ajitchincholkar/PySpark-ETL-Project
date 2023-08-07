import os
import sys
import get_env_variables as gev
from create_spark import get_spark_object
from validate import get_current_date, print_schema, check_for_null, display_df, df_count
from ingest import load_files
from data_processing import data_clean
from data_transformation import *
from save_data import save_to_disk
import logging
import logging.config

logging.config.fileConfig('Properties/configuration/logging.config')


def main():
    global file_dir, header, inferSchema
    try:
        logging.info('I am in the main method')

        logging.info('Calling spark object')
        spark = get_spark_object(gev.envn, gev.appName)

        logging.info('Validating spark object')
        get_current_date(spark)

        for file in os.listdir(gev.src_olap):
            print(f"File is {file}")

            file_dir = gev.src_olap + '\\' + file
            print(file_dir)

            if file.endswith('.parquet'):
                file_format = 'parquet'
                header = 'NA'
                inferSchema = 'NA'
            elif file.endswith('.csv'):
                file_format = 'csv'
                header = gev.header
                inferSchema = gev.inferSchema

        logging.info(f'Reading a {file_format} file.')

        df_city = load_files(spark=spark, file_dir=file_dir, file_format=file_format, header=header,
                             inferSchema=inferSchema)

        logging.info('Displaying the dataframe df_city')
        display_df(df_city, 'df_city')

        logging.info('Validating the dataframe...')
        df_count(df_city, 'df_city')

        logging.info('Checking for files in oltp...')

        for file in os.listdir(gev.src_oltp):
            print(f"File is {file}")

            file_dir = gev.src_oltp + '\\' + file
            print(file_dir)

            if file.endswith('.parquet'):
                file_format = 'parquet'
                header = 'NA'
                inferSchema = 'NA'
            elif file.endswith('.csv'):
                file_format = 'csv'
                header = gev.header
                inferSchema = gev.inferSchema

        logging.info(f'Reading a {file_format} file.')

        df_presc = load_files(spark=spark, file_dir=file_dir, file_format=file_format, header=header,
                              inferSchema=inferSchema)

        logging.info('Displaying the dataframe df_presc')
        display_df(df_presc, 'df_presc')

        logging.info('Validating the dataframe...')
        df_count(df_presc, 'df_presc')

        logging.info('Implementing data_processing methods...')
        df_city_sel, df_presc_sel = data_clean(df_city, df_presc)

        display_df(df_city_sel, 'df_city_sel')
        display_df(df_presc_sel, 'df_presc_sel')

        logging.info('Validating schema for dataframes.')

        print_schema(df_city_sel, 'df_city_sel')
        print_schema(df_presc_sel, 'df_presc_sel')

        logging.info('Checking for null values after processing')

        check_df = check_for_null(df_presc_sel, 'df_presc')
        display_df(check_df, 'df_presc')

        logging.info('Data transformation executing...')

        df_report_1 = data_report1(df_city_sel, df_presc_sel)

        logging.info('Displaying df_report_1')
        display_df(df_report_1, 'data_report_1')

        logging.info('Displaying df_report_2')
        df_report_2 = data_report2(df_presc_sel)
        display_df(df_report_2, 'data_report_2')

        logging.info('Saving the data reports to the output folder...')
        city_path = gev.city_path
        save_to_disk(df_report_1, 'orc', city_path, 1, False, 'snappy')

        presc_path = gev.presc_path
        save_to_disk(df_report_2, 'parquet', presc_path, 2, False, 'snappy')

        logging.info('Data reports successfully saved in the output folder.')

    except Exception as e:
        logging.error("An error occurred when calling main ", str(e))
        sys.exit(1)


if __name__ == '__main__':
    main()
    logging.info('Application done')
