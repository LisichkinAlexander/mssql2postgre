"""
Module working with program settings
"""

import os
import re
import json
import jsonschema

def read_json_settings(file_name: str) -> dict:
    """ Read setting json file """
    json_file = os.path.dirname(__file__) + "/" + file_name
    if not os.path.exists(json_file):
        raise Exception("No setting json file mssql2postgre.json")
    with open(json_file, 'r', encoding="utf-8") as file:
        json_data = json.load(file)

    # Validate JSON data using python
    schema_file = os.path.dirname(__file__) + "/schema.json"
    if not os.path.exists(json_file):
        raise Exception("No schema json file")
    with open(schema_file, 'r', encoding="utf-8") as file:
        schema_data = json.load(file)
    jsonschema.validate(instance=json_data, schema=schema_data)

    # Read sql files
    dict_sql = {}
    for key, value in json_data.items():
        if isinstance(value, str) and value.endswith(".sql"):
            sql_file = os.path.dirname(__file__) + "/SQL/" + value
            with open(sql_file, 'r', encoding="utf-8") as file:
                sql = file.read()
            dict_sql[key + "_sql"] = sql
        elif isinstance(value, str) and "@OS." in value:
            matches = re.findall(r"(?P<os_variable>\@OS\.(\w|\_)+)", value)
            if matches:
                for match in matches:
                    variable = match[0].replace('@OS.', '')
                    os_variable = os.environ.get(variable)
                    if os_variable:
                        value = value.replace(match[0], os_variable)
                        dict_sql[key] = value

    json_data.update(dict_sql)
    return json_data

def strtobool (val):
    """Convert a string representation of truth to true (1) or false (0).
    True values are 'y', 'yes', 't', 'true', 'on', and '1'; false values
    are 'n', 'no', 'f', 'false', 'off', and '0'.  Raises ValueError if
    'val' is anything else.
    """
    if isinstance(val, bool):
        return val
    if isinstance(val, str):
        val = val.lower()
        if val in ('y', 'yes', 't', 'true', 'on', '1'):
            return 1
        elif val in ('n', 'no', 'f', 'false', 'off', '0'):
            return 0
        else:
            raise ValueError(f"invalid truth value {val}")
    else:
        raise ValueError(f"invalid truth value {val}")

def get_bool_setting(json_data: dict, name: str, def_value = False)->bool:
    """ Get boolean setting """
    if name in json_data:
        return strtobool(json_data[name])
    else:
        return def_value

def main():
    raise SystemError("This file cannot be operable")

if __name__ == "__main__":
    main()
