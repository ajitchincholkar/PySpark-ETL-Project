import logging.config
from pyspark.sql.functions import *
from pyspark.sql.types import *

logging.config.fileConfig('Properties/configuration/logging.config')

logger = logging.getLogger('Data_processing')


def data_clean(df1, df2):
    try:
        logger.warning('data_clean method started...')

        logger.warning('Selecting required columns and converting some of the columns into upper case.')

        df_city_sel = df1.select(upper(col('city')).alias('city'), df1.state_id,
                                 upper(df1.state_name).alias('state_name'),
                                 upper(df1.county_name).alias('county_name'), df1.population, df1.zips)

        logger.warning('Working on OLTP dataset, selecting couple of columns and renaming...')

        df_presc_sel = df2.select(df2.npi.alias('presc_id'), df2.nppes_provider_last_org_name.alias('presc_lname'),
                                  df2.nppes_provider_first_name.alias('presc_fname'),
                                  df2.nppes_provider_city.alias('presc_city'),
                                  df2.nppes_provider_state.alias('presc_state'),
                                  df2.specialty_description.alias('presc_specialty'),
                                  df2.drug_name, df2.total_claim_count, df2.total_day_supply, df2.total_drug_cost,
                                  df2.years_of_exp)

        logger.warning('Adding a new column to df_presc_sel')

        df_presc_sel = df_presc_sel.withColumn('country_name', lit('USA'))

        logger.warning('Converting years_of_exp dtype to int')

        df_presc_sel = df_presc_sel.withColumn('years_of_exp', regexp_replace(col('years_of_exp'), r"^=", ""))
        df_presc_sel = df_presc_sel.withColumn('years_of_exp', col('years_of_exp').cast('int'))

        logger.warning('Concat first and last name.')

        df_presc_sel = df_presc_sel.withColumn('presc_fullname', concat_ws(" ", 'presc_fname', 'presc_lname'))

        logger.warning('Dropping the presc_fname and presc_lname')

        df_presc_sel = df_presc_sel.drop('presc_lname', 'presc_fname')

        logger.warning('Dropping the null values from the respective columns.')

        df_presc_sel = df_presc_sel.dropna(subset='presc_id')
        df_presc_sel = df_presc_sel.dropna(subset='drug_name')

        logger.warning('Filling the null values in total_claim_count with mean.')
        mean_claim_count = df_presc_sel.select(mean(col('total_claim_count'))).collect()[0][0]
        df_presc_sel = df_presc_sel.fillna(mean_claim_count, 'total_claim_count')

        logger.warning('Successfully dropped the null values')

    except Exception as e:
        logger.error('An error occurred at data_clean method ', str(e))

    else:
        logger.warning('data_clean method executed, go forward...')

        return df_city_sel, df_presc_sel
