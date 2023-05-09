import bq_utils
import json
import os
from ruamel.yaml import YAML
from ruamel.yaml.scalarstring import walk_tree


yaml = YAML()
yaml.default_flow_style = False

SCOPES = ['https://www.googleapis.com/auth/cloud-platform', 'https://www.googleapis.com/auth/bigquery']

# bq cli project - costs
project = 'project'

table_id_pattern = 'project.dataset.{tab}'

# you can use service accout:
# bq_client = bq_utils.get_bq_cli(project, sa_json_file=SERVICE_ACCOUNT_FILE, scopes=SCOPES)
bq_client = bq_utils.get_bq_cli(project, scopes=SCOPES)

list_of_views = ['table_1', 'table_2']

json_view_def = {
    "view": {
      "query": "",
      "useLegacySql": False}
    }

root_path = os.getcwd()

# JSON def
for table in list_of_views:
    sql = bq_utils.build_table_view(table_id_pattern.format(tab=table), bq_client)
    # print(sql)
    temp_json = json_view_def.copy()
    temp_json["view"]["query"] = sql

    with open(os.path.join(root_path, "view_to_add/")+f"{table.replace('pt_', 'vw_')}.json", "w+") as json_file:
        json.dump(temp_json, json_file, indent=2)

# YAML def
for table in list_of_views:
    new_line = "\n"
    tab = "\t"
    sql = bq_utils.build_table_view(table_id_pattern.format(tab=table), bq_client, new_line=new_line, tab=tab)
    print(sql)
    temp_json = json_view_def.copy()
    temp_json["view"]["query"] = sql
    walk_tree(temp_json)
    with open(os.path.join(root_path, "view_to_add/")+f"{table.replace('pt_', 'vw_')}.yaml", "w+") as yaml_file:
        yaml.dump(temp_json, yaml_file)
