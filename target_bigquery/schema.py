"""
the purpose of this module is to convert JSON schema to BigQuery schema.
"""

from google.cloud.bigquery import SchemaField
import re

METADATA_FIELDS = {
    "_time_extracted": {"type": ["null", "string"], "format": "date-time", "bq_type": "timestamp"},
    "_time_loaded": {"type": ["null", "string"], "format": "date-time", "bq_type": "timestamp"}
}


def cleanup_record(schema, record):
    """
    Clean up / prettify field names, make sure they match BigQuery naming conventions.

    :param JSON record generated by the tap and piped into target-bigquery
    :param bq_schema: JSON schema generated by the tap and piped into target-bigquery
    :return: JSON record/data, where field names are cleaned up / prettified.
    """
    if not isinstance(record, dict) and not isinstance(record, list):
        return record

    elif isinstance(record, list):
        nr = []
        for item in record:
            nr.append(cleanup_record(schema, item))
        return nr

    elif isinstance(record, dict):
        nr = {}
        for key, value in record.items():
            nkey = bigquery_transformed_key(key)
            nr[nkey] = cleanup_record(schema, value)
        return nr

    else:
        raise Exception(f"unhandled instance of record: {record}")


def bigquery_transformed_key(key):
    """
    Clean up / prettify field names, make sure they match BigQuery naming conventions.
    
    Fields must:
        • contain only 
            -letters, 
            -numbers, and 
            -underscores, 
        • start with a 
            -letter or 
            -underscore, and
        • be at most 300 characters long

    :param key: JSON field name
    :return: cleaned up JSON field name
    """
    remove_list = [" ",
                   "!",
                   "\"",
                   "#",
                   "$",
                   "%",
                   "&",
                   "'",
                   "(",
                   ")",
                   "*",
                   "+",
                   ",",
                   "-",
                   ".",
                   "/",
                   ":",
                   ";",
                   "<",
                   "=",
                   ">",
                   "?",
                   "@",
                   "\\",
                   "]",
                   "^",
                   "`",
                   "|",
                   "}",
                   "~"]

    for c in remove_list:
        key = key.replace(c, "_")

    if re.match(r"^\d", key):
        key = "_" + key

    return key


def prioritize_one_data_type_from_multiple_ones_in_any_of(field_property):
    """
    :param field_property: JSON field property, which has anyOf and multiple data types
    :return: one BigQuery SchemaField field_type, which is prioritized

    Simplification step removes anyOf columns from original JSON schema.

    There's one instance when original JSON schema has no anyOf, but anyOf gets added:

    original JSON schema:

     "simplification_stage_adds_anyOf": {
      "type": [
        "null",
        "integer",
        "string"
      ]
    }

     This is a simplified JSON schema where anyOf got added during
     simplification stage:

      {'simplification_stage_added_anyOf': {
            'anyOf': [
                {
                    'type': [
                        'integer',
                        'null'
                    ]
                },
                {
                    'type': [
                        'string',
                        'null'
                    ]
                }
            ]
        }
        }

    The VALUE of this dictionary will be the INPUT for this function.

    This simplified case needs to be handled.

    Prioritization needs to be applied:
        1) STRING
        2) FLOAT
        3) INTEGER
        4) BOOLEAN

    OUTPUT of the function is one JSON data type with the top priority
    """

    prioritization_dict = {"string": 1,
                           "number": 2,
                           "integer": 3,
                           "boolean": 4,
                           "object": 5,
                           "array": 6,
                           }

    any_of_data_types = {}

    for i in range(0, len(field_property['anyOf'])):
        data_type = field_property['anyOf'][i]['type'][0]

        any_of_data_types.update({data_type: prioritization_dict[data_type]})

    # return key with minimum value, which is the highest priority data type
    # https://stackoverflow.com/questions/268272/getting-key-with-maximum-value-in-dictionary
    return min(any_of_data_types, key=any_of_data_types.get)


def convert_field_type(field_property):
    """
    :param field_property: JSON field property
    :return: BigQuery SchemaField field_type
    """

    conversion_dict = {"string": "STRING",
                       "number": "FLOAT",
                       "integer": "INTEGER",
                       "boolean": "BOOLEAN",
                       "date-time": "TIMESTAMP",
                       "date": "DATE",
                       "time": "TIME",
                       "object": "RECORD",
                       "array": "RECORD",
                       "bq-geography": "GEOGRAPHY",
                       "bq-decimal": "DECIMAL",
                       "bq-bigdecimal": "BIGDECIMAL"
                       }

    if "anyOf" in field_property:

        prioritized_data_type = prioritize_one_data_type_from_multiple_ones_in_any_of(field_property)

        field_type_bigquery = conversion_dict[prioritized_data_type]

    elif field_property["type"][0] == "string" and "format" in field_property:

        field_type_bigquery = conversion_dict[field_property["format"]]

    elif (("items" in field_property) and ("properties" not in field_property["items"])):

        field_type_bigquery = conversion_dict[field_property['items']['type'][0]]

    else:

        field_type_bigquery = conversion_dict[field_property["type"][0]]

    return field_type_bigquery


def determine_field_mode(field_name, field_property):
    """
    :param field_name: one nested JSON field name
    :param field_property: one nested JSON field property
    :return: BigQuery SchemaField mode
    """
    if "items" in field_property:

        field_mode = 'REPEATED'

    else:

        field_mode = 'NULLABLE'

    return field_mode


def replace_nullable_mode_with_required(schema_field_input):
    schema_field_updated = SchemaField(name=schema_field_input.name,
                                       field_type=schema_field_input.field_type,
                                       mode='REQUIRED',
                                       description=schema_field_input.description,
                                       fields=schema_field_input.fields,
                                       policy_tags=schema_field_input.policy_tags)

    return schema_field_updated


def build_field(field_name, field_property):
    """
    :param field_name: one nested JSON field name
    :param field_property: one nested JSON field property
    :return: one BigQuery nested SchemaField
    """

    if not ("items" in field_property and "properties" in field_property["items"]) and not (
            "properties" in field_property):

        return (SchemaField(name=bigquery_transformed_key(field_name),
                            field_type=convert_field_type(field_property),
                            mode=determine_field_mode(field_name, field_property),
                            description=None,
                            fields=(),
                            policy_tags=None)
                )

    elif ("items" in field_property and "properties" in field_property["items"]) or ("properties" in field_property):

        processed_subfields = []

        # https://www.w3schools.com/python/ref_dictionary_get.asp
        for subfield_name, subfield_property in field_property.get("properties",
                                                                   field_property.get("items", {}).get("properties")
                                                                   ).items():
            processed_subfields.append(build_field(subfield_name, subfield_property))

        return (SchemaField(name=bigquery_transformed_key(field_name),
                            field_type=convert_field_type(field_property),
                            mode=determine_field_mode(field_name, field_property),
                            description=None,
                            fields=processed_subfields,
                            policy_tags=None)
                )


def build_schema(schema, key_properties=None, add_metadata=True, force_fields={}):
    """
    :param schema: input simplified JSON schema
    :param key_properties: JSON schema fields which will become required BigQuery column
    :param add_metadata: do we want BigQuery metadata columns (e.g., when data was uploaded?)
    :param force_fields: You can force a field to a desired data type via force_fields flag.
            Use case example:
            tap facebook field "date_start" from stream ads_insights_age_and_gender is being passed as string to BQ,
            which contradicts tap catalog file, where we said it's a date. force_fields fixes this issue.
    :return: a list of BigQuery SchemaFields, which represents one BigQuery table
    """

    global required_fields

    required_fields = set(key_properties) if key_properties else set()

    schema_bigquery = []

    for field_name, field_property in schema.get("properties", schema.get("items", {}).get("properties", {})).items():

        if field_name in force_fields:

            next_field = (
                SchemaField(field_name, force_fields[field_name]["type"],
                            force_fields[field_name].get("mode", "nullable"),
                            force_fields[field_name].get("description", None), ())
            )

        else:

            next_field = build_field(field_name, field_property)

            if field_name in required_fields:
                next_field = replace_nullable_mode_with_required(next_field)

        schema_bigquery.append(next_field)

    if add_metadata:

        for field_name in METADATA_FIELDS:
            schema_bigquery.append(SchemaField(name=field_name,
                                               field_type=METADATA_FIELDS[field_name]["bq_type"],
                                               mode='NULLABLE',
                                               description=None,
                                               fields=(),
                                               policy_tags=None)
                                   )

    return schema_bigquery


def format_record_to_schema(record, bq_schema):
    """
    Purpose:
        Singer tap outputs two things: JSON schema and JSON record/data.
        Sometimes tap outputs data, where type doesn't match schema produced by the tap.
        This function makes sure that the data matches the schema.

    RECORD is not included into conversion_dict - it is done on purpose. RECORD is handled recursively.

    :param JSON record generated by the tap and piped into target-bigquery
    :param bq_schema: JSON schema generated by the tap and piped into target-bigquery
    :return: JSON record/data, where the data types match JSON schema
    """

    conversion_dict = {"BYTES": bytes,
                       "STRING": str,
                       "TIME": str,
                       "TIMESTAMP": str,
                       "DATE": str,
                       "DATETIME": str,
                       "FLOAT": float,
                       "NUMERIC": float,
                       "BIGNUMERIC": float,
                       "INTEGER": int,
                       "BOOLEAN": bool,
                       "GEOGRAPHY": str,
                       "DECIMAL": str,
                       "BIGDECIMAL": str
                       }

    if isinstance(record, list):
        new_record = []
        for r in record:
            if isinstance(r, dict):
                r = format_record_to_schema(r, bq_schema)
                new_record.append(r)
            else:
                raise Exception(f"unhandled instance of list object in record: {r}")
        return new_record
    elif isinstance(record, dict):
        rc = record.copy()
        for k, v in rc.items():
            if k not in bq_schema:
                record.pop(k)
            elif v is None:
                pass
            elif bq_schema[k].get("fields"):
                # mode: REPEATED, type: NULLABLE || mode: REPEATED: type: REPEATED
                record[k] = format_record_to_schema(record[k], bq_schema[k]["fields"])
            elif bq_schema[k].get("mode") == "REPEATED":
                # mode: REPEATED, type: [any]
                record[k] = [conversion_dict[bq_schema[k]["type"]](vi) for vi in v]
            else:
                record[k] = conversion_dict[bq_schema[k]["type"]](v)
    return record
