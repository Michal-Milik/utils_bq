import bq_utils

project = "project"
destination_dataset = f'{project}.test_dataset.{{}}'

tables = ['test_table']

# SERVICE_ACCOUNT_FILE = 'google-sa-uat.json'
SERVICE_ACCOUNT_FILE = 'google-sa-prod.json'
SCOPES = ['https://www.googleapis.com/auth/cloud-platform',
          'https://www.googleapis.com/auth/bigquery',
          # additional scope need to be added to accesss drive with gsheets
          'https://www.googleapis.com/auth/drive']

# use credentials from json file
bq_client = bq_utils.get_bq_cli(project,
                                sa_json_file=SERVICE_ACCOUNT_FILE,
                                scopes=SCOPES)
# use your credentials
# bq_client = bq_utils.get_bq_cli(project, scopes=SCOPES)


# copy whole table
sql_format = """
select * from
`{}`
"""

for tab in tables:
    sql = sql_format.format(destination_dataset.format(tab))
    print(sql)
    # destination table will have same project, dataset
    # and table name with suffix _ps -> persist copy
    destination = destination_dataset.format(tab+"_pc")
    print(destination)
    bq_utils.write_truncate_query_to_tab(sql, destination, bq_client)
