"some functions to help working with bq"

from google.cloud import bigquery
import logging
from google.oauth2 import service_account

# logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def get_bq_cli(project, sa_json_file=None, scopes=['https://www.googleapis.com/auth/cloud-platform']):
    if sa_json_file:
        credentials = service_account.Credentials.from_service_account_file(
            sa_json_file, scopes=scopes)
        print(credentials.service_account_email)
    else:
        credentials = None

    return bigquery.Client(credentials=credentials, project=project)


def get_schema(table_id, bq_client):
    table_obj = bq_client.get_table(table_id)
    table_schema = {obj.name.lower():
                    {"name": obj.name,
                     "field_type": obj.field_type,
                     "is_nullable": obj.is_nullable}
                    for obj in table_obj.schema}
    return table_schema


def build_select_sql(list_of_cols, source, condition=None):
    new_line = '\n'
    tab = '\t'
    return f"""SELECT
{tab}{f', {new_line}{tab}'.join(list_of_cols)}
FROM {source}""" + (f"""WHERE {condition};""" if condition else ";")


# WRITE_TRUNCATE!
def write_truncate_query_to_tab(sql, destination_table_id, bq_client):
    job_config = bigquery.QueryJobConfig(destination=destination_table_id)
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
    query_job = bq_client.query(sql, job_config=job_config)
    query_job.result()
    logger.info(f"Query results loaded to the table {destination_table_id}")
    return None


def copy_table_sql(source_table_id, destination_table_id, bq_client):
    source_table_schema = get_schema(source_table_id, bq_client)
    destination_table_schema = get_schema(destination_table_id, bq_client)
    # compare schema
    missing_in_source = []
    diffrent_data_type = []
    list_of_cols = []

    logger.debug(f"check if field is missing")
    for field in destination_table_schema.keys():
        # check if field is missing
        if source_table_schema.get(field, None):
            logger.debug(f"Field {field} exists in source")
            # check if schema match
            if destination_table_schema[field]["field_type"] != source_table_schema[field]["field_type"]:
                logger.warning(f"Field {field} have diffrent datatype in source; destination type {destination_table_schema[field]['field_type']}; source type {source_table_schema[field]['field_type']}")
                diffrent_data_type.append(field)
                #  TODO check if casting is proper!
                # now sql get faild if we will try for example cast string to int.
                list_of_cols.append(f"CAST({field} AS {source_table_schema[field]['field_type']}) AS {field}")
            else:
                list_of_cols.append(field)
        else:
            logger.warning(f"Field {field} not exists in source")
            missing_in_source.append(field)
            list_of_cols.append(f"CAST(NULL AS {destination_table_schema[field]['field_type']}) AS {field}")
    # check if new table contain all fields from source
    logger.debug(f"check if new table contain all fields from source")
    for field in source_table_schema.keys():
        if destination_table_schema.get(field, None):
            logger.debug(f"Field {field} exist in target table")
        else:
            logger.error(f"Field {field} not exists in target table! Need to be fixed")
            return None
    return build_select_sql(list_of_cols, source_table_id), missing_in_source, diffrent_data_type

