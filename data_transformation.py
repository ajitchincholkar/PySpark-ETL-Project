import logging.config
from pyspark.sql.window import Window
from pyspark.sql.functions import *
from pyspark.sql.types import *

logging.config.fileConfig('Properties/configuration/logging.config')

logger = logging.getLogger('Data_transformation')


def data_report1(df1, df2):
    try:
        logger.warning('Processing data_report1 method...')

        logger.warning(f'Calculating total zip counts in {df1}')

        df_city_split = df1.withColumn('zipcounts', size(split(col('zips'), ' ')))

        logger.warning('Calculating distinct prescribers and total claim count')

        df_presc_grp = df2.groupBy(df2.presc_state, df2.presc_city) \
            .agg(countDistinct('presc_id').alias('presc_counts'),
                 sum('total_claim_count').alias('total_claims'))

        logger.warning('Do not report a city if no prescriber assigned to it')
        logger.warning('Joining df_city_sel and df_presc_grp')

        df_city_join = df_city_split.join(df_presc_grp, (df_city_split.state_id == df_presc_grp.presc_state) &
                                          (df_city_split.city == df_presc_grp.presc_city), 'inner')

        df_final = df_city_join.select('city', 'state_name', 'county_name', 'population', 'zipcounts', 'presc_counts') \
            .sort(col('presc_counts').desc())

    except Exception as e:
        logger.error(('An error occurred during data_report1 ', str(e)))
        raise

    else:
        logger.warning('data_report1 successfully executed, go forward...')

    return df_final


def data_report2(df2):
    try:
        logger.warning('Executing data_report2 method...')

        logger.warning('Executing the task ::: Consider the prescribers only from 20 to 50 years_of_exp and rank the '
                       'prescribers based on their total_claim_count for each state')

        wspec = Window.partitionBy('presc_state').orderBy(col('total_claim_count').desc())

        df_presc_report = df2.select('presc_id', 'presc_fullname', 'presc_state', 'country_name', 'years_of_exp',
                                     'total_claim_count', 'total_day_supply', 'total_drug_cost') \
            .filter((df2.years_of_exp >= 20) & (df2.years_of_exp <= 50)) \
            .withColumn('dense_rank', dense_rank().over(wspec)) \
            .filter(col('dense_rank') <= 5)

    except Exception as e:
        logger.error('An error occurred during data_report2 method ', str(e))
        raise

    else:
        logger.warning('data_report2 method executed successfully, go forward...')

    return df_presc_report
