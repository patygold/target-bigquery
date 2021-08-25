import singer
import json
import copy
import os

import logging

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

from target_bigquery.schema import build_schema, prioritize_one_data_type_from_multiple_ones_in_any_of, \
    convert_field_type

from tests.schema_old import build_schema_old

from target_bigquery.simplify_json_schema import simplify
from tests import unittestcore

from tests.utils import convert_list_of_schema_fields_to_list_of_lists, compare_old_vs_new_schema_conversion

from target_bigquery.validate_json_schema import validate_json_schema_completeness

from tests.rsc.schemas.input_json_schemas import *

from tests.rsc.schemas.input_json_schemas_shopify import *

list_of_schema_inputs = [test_schema_collection_anyOf_problem_column,
                         schema_nested_1,
                         schema_nested_1_subset_items_problem,
                         schema_nested_2,
                         schema_nested_3_shopify,
                         shopify_orders_fixed,
                         # shopify_customers, #old schema.py fails on this in my test. New one works
                         shopify_custom_collections,
                         # shopify_abandoned_checkouts_fixed, #old schema.py fails on this in my test. New one works
                         shopify_products,
                         shopify_transactions,
                         # shopify_metafields_malformed, # not valid schema
                         shopify_metafields_fixed,
                         shopify_order_refunds,
                         shopify_collects
                         ]


class TestSchemaConversion(unittestcore.BaseUnitTest):

    def setUp(self):
        super(TestSchemaConversion, self).setUp()

    def test_flat_schema(self):

        schema_0_input = schema_simple_1

        msg = singer.parse_message(schema_0_input)

        schema_1_simplified = simplify(msg.schema)

        schema_2_built_new_method = build_schema(schema_1_simplified, key_properties=msg.key_properties,
                                                 add_metadata=True)

        schema_3_built_old_method = build_schema_old(msg.schema, key_properties=msg.key_properties, add_metadata=True)

        for f in schema_2_built_new_method:
            if f.name == "id":
                self.assertEqual(f.field_type.upper(), "STRING")

            elif f.name == "name":
                self.assertEqual(f.field_type.upper(), "STRING")

            elif f.name == "value":
                self.assertEqual(f.field_type.upper(), "INTEGER")

            elif f.name == "ratio":
                self.assertEqual(f.field_type.upper(), "FLOAT")

            elif f.name == "timestamp":
                self.assertEqual(f.field_type.upper(), "TIMESTAMP")

            elif f.name == "date":
                self.assertEqual(f.field_type.upper(), "DATE")

            elif f.name == "geo":
                self.assertEqual(f.field_type.upper(), "GEOGRAPHY")

    def test_prioritize_one_data_type_from_multiple_ones_in_any_of_string(self):

        test_input = {
            'anyOf': [
                {
                    'type': [
                        'integer',
                        'null'
                    ]
                },
                {
                    'type': [
                        'boolean',
                        'null'
                    ]
                },
                {
                    'type': [
                        'string',
                        'null'
                    ]
                }
                ,
                {
                    'type': [
                        'number',
                        'null'
                    ]
                }
            ]
        }

        prioritized_data_type = prioritize_one_data_type_from_multiple_ones_in_any_of(test_input)

        assert prioritized_data_type == "string"

        converted_data_type = convert_field_type(test_input)

        assert converted_data_type == "STRING"

    def test_prioritize_one_data_type_from_multiple_ones_in_any_of_float(self):

        test_input = {
            'anyOf': [
                {
                    'type': [
                        'number',
                        'null'
                    ]
                },
                {
                    'type': [
                        'integer',
                        'null'
                    ]
                }
                ,
                {
                    'type': [
                        'boolean',
                        'null'
                    ]
                }
            ]
        }

        prioritized_data_type = prioritize_one_data_type_from_multiple_ones_in_any_of(test_input)

        assert prioritized_data_type == "number"

        converted_data_type = convert_field_type(test_input)

        assert converted_data_type == "FLOAT"

    def test_prioritize_one_data_type_from_multiple_ones_in_any_of_integer(self):

        test_input = {
            'anyOf': [

                {
                    'type': [
                        'integer',
                        'null'
                    ]
                }
                ,
                {
                    'type': [
                        'boolean',
                        'null'
                    ]
                }
            ]
        }

        prioritized_data_type = prioritize_one_data_type_from_multiple_ones_in_any_of(test_input)

        assert prioritized_data_type == "integer"

        converted_data_type = convert_field_type(test_input)

        assert converted_data_type == "INTEGER"

    def test_one_nested_schema_1(self):

        schema_0_input = schema_nested_1

        msg = singer.parse_message(schema_0_input)

        schema_1_simplified = simplify(msg.schema)

        schema_2_built_new_method = build_schema(schema_1_simplified, key_properties=msg.key_properties,
                                                 add_metadata=True)

        schema_3_built_old_method = build_schema_old(msg.schema, key_properties=msg.key_properties, add_metadata=True)

        # are results of the two methods above identical?
        assert schema_2_built_new_method == schema_3_built_old_method

        for f in schema_2_built_new_method:
            if f.name in ("date_start", "date_stop"):
                self.assertEqual(f.field_type.upper(), "TIMESTAMP")

    def test_one_nested_schema_2(self):

        schema_0_input = schema_nested_2

        msg = singer.parse_message(schema_0_input)

        schema_1_simplified = simplify(msg.schema)

        schema_2_built_new_method = build_schema(schema_1_simplified, key_properties=msg.key_properties,
                                                 add_metadata=True)

        schema_3_built_old_method = build_schema_old(msg.schema, key_properties=msg.key_properties, add_metadata=True)

        # are results of the two methods above identical? ignore order of columns and case
        schema_built_new_method_sorted = convert_list_of_schema_fields_to_list_of_lists(schema_2_built_new_method)

        schema_built_old_method_sorted = convert_list_of_schema_fields_to_list_of_lists(schema_3_built_old_method)

        assert schema_built_new_method_sorted == schema_built_old_method_sorted

    def test_several_nested_schemas(self):

        for next_schema_input in list_of_schema_inputs:
            schema_0_input = next_schema_input

            msg = singer.parse_message(schema_0_input)

            schema_1_simplified = simplify(msg.schema)

            schema_2_built_new_method = build_schema(schema_1_simplified, key_properties=msg.key_properties,
                                                     add_metadata=True)

            schema_3_built_old_method = build_schema_old(msg.schema, key_properties=msg.key_properties,
                                                         add_metadata=True)

            # are results of the two methods above identical? ignore order of columns and case
            schema_built_new_method_sorted = convert_list_of_schema_fields_to_list_of_lists(schema_2_built_new_method)

            schema_built_old_method_sorted = convert_list_of_schema_fields_to_list_of_lists(schema_3_built_old_method)

            assert schema_built_new_method_sorted == schema_built_old_method_sorted

    def test_several_nested_schemas_amazon(self):

        compare_old_vs_new_schema_conversion(os.path.join(os.path.join(
            os.path.join(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'tests'), 'rsc'),
            'schemas'), 'input_json_schemas_amazon.json'))

    def test_several_nested_schemas_asana(self):

        """
        for some reason recharge product stream schema conversion fails on the old schema.py
        works with the new schema conversion
        """

        compare_old_vs_new_schema_conversion(os.path.join(os.path.join(
            os.path.join(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'tests'), 'rsc'),
            'schemas'), 'input_json_schemas_asana.json'), exclude_stream='workspaces')

    def test_several_nested_schemas_asana_workspaces_new_method(self):

        catalog = json.load(open(os.path.join(os.path.join(
            os.path.join(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'tests'), 'rsc'),
            'schemas'), 'input_json_schemas_asana.json')))

        for next_schema_input in catalog['streams']:

            if next_schema_input['tap_stream_id'] == 'workspaces':  # old conversion fails here

                validate_json_schema_completeness(next_schema_input)

                schema_0_input = copy.deepcopy(next_schema_input)

                schema_0_input.update({"type": "SCHEMA"})

                schema_0_input = str(schema_0_input)

                schema_0_input = schema_0_input.replace("\'", "\"").replace("True", "true").replace("False",
                                                                                                    "false").replace(
                    "None", "null")

                msg = singer.parse_message(schema_0_input)

                schema_1_simplified = simplify(msg.schema)

                schema_2_built_new_method = build_schema(schema_1_simplified, key_properties=msg.key_properties,
                                                         add_metadata=True)

                # old conversion fails on asana workspaces
                # schema_3_built_old_method = build_schema_old(msg.schema, key_properties=msg.key_properties, add_metadata=True)

                # are results of the two methods above identical? ignore order of columns and case
                schema_built_new_method_sorted = convert_list_of_schema_fields_to_list_of_lists(
                    schema_2_built_new_method)

                assert schema_built_new_method_sorted

            # TODO: check data types in this test and subsequent ones

    def test_several_nested_schemas_bing_ads(self):

        compare_old_vs_new_schema_conversion(os.path.join(os.path.join(
            os.path.join(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'tests'), 'rsc'),
            'schemas'), 'input_json_schemas_bing_ads.json'))

    def test_several_nested_schemas_facebook(self):

        compare_old_vs_new_schema_conversion(os.path.join(os.path.join(
            os.path.join(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "tests"), "rsc"),
            "schemas"), "input_json_schemas_facebook.json"))

    def test_several_nested_schemas_google_search_console(self):

        compare_old_vs_new_schema_conversion(os.path.join(os.path.join(
            os.path.join(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "tests"), "rsc"),
            "schemas"), "input_json_schemas_google_search_console.json"))

    def test_several_nested_schemas_hubspot(self):

        compare_old_vs_new_schema_conversion(os.path.join(os.path.join(
            os.path.join(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "tests"), "rsc"),
            "schemas"), "input_json_schemas_hubspot.json"))

    def test_several_nested_schemas_klaviyo(self):

        compare_old_vs_new_schema_conversion(os.path.join(os.path.join(
            os.path.join(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "tests"), "rsc"),
            "schemas"), "input_json_schemas_klaviyo.json"))

    def test_several_nested_schemas_klaviyo_field_names_contain_chars_not_valid_in_bigquery(self):

        catalog_schema_file = os.path.join(os.path.join(
            os.path.join(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'tests'), 'rsc'),
            'schemas'), 'input_json_schemas_klaviyo_field_names_contain_chars_invalid_in_bigquery.json')

        catalog = json.load(open(catalog_schema_file))

        for next_schema_input in catalog['streams']:

            if next_schema_input['stream'] != "only_test_this_stream_skip_others":
                continue

            # make sure stream doesn't have empty (undefined object {}) type or properties
            validate_json_schema_completeness(next_schema_input)

            # clean up schema formatting
            schema_0_input = copy.deepcopy(next_schema_input)

            if "type" not in schema_0_input.keys():
                schema_0_input.update({"type": "SCHEMA"})

            if "key_properties" not in schema_0_input.keys():
                schema_0_input.update({"key_properties": "Id"})

            schema_0_input = str(schema_0_input)

            schema_0_input = schema_0_input.replace("\'", "\"").replace("True", "true").replace("False",
                                                                                                "false").replace("None",
                                                                                                                 "null")

            # convert schema using old vs. new method
            msg = singer.parse_message(schema_0_input)

            schema_1_simplified = simplify(msg.schema)

            schema_2_built_new_method = build_schema(schema_1_simplified, key_properties=msg.key_properties,
                                                     add_metadata=True)

            schema_3_built_old_method = build_schema_old(msg.schema, key_properties=msg.key_properties,
                                                         add_metadata=True)

            assert schema_3_built_old_method[0].name == "$dollarsign"
            assert schema_3_built_old_method[1].name == "contains space"
            assert schema_3_built_old_method[2].name == "contains:colon"
            assert schema_3_built_old_method[3].name == "contains&ampersand"

            assert schema_2_built_new_method[0].name == "_dollarsign"
            assert schema_2_built_new_method[1].name == "contains_space"
            assert schema_2_built_new_method[2].name == "contains_colon"
            assert schema_2_built_new_method[3].name == "contains_ampersand"

    def test_several_nested_schemas_mailchimp(self):

        compare_old_vs_new_schema_conversion(os.path.join(os.path.join(
            os.path.join(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "tests"), "rsc"),
            "schemas"), "input_json_schemas_mailchimp_fixed.json"))

    def test_several_nested_schemas_recharge(self):

        """
        for some reason recharge product stream schema conversion fails on the old schema.py
        works with the new schema conversion
        """

        compare_old_vs_new_schema_conversion(os.path.join(os.path.join(
            os.path.join(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "tests"), "rsc"),
            "schemas"), "input_json_schemas_recharge.json"), exclude_stream='products')

    def test_several_nested_schemas_recharge_products_new_method(self):

        catalog = json.load(open(os.path.join(os.path.join(
            os.path.join(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "tests"), "rsc"),
            "schemas"), "input_json_schemas_recharge.json")))

        for next_schema_input in catalog['streams']:

            if next_schema_input['tap_stream_id'] == 'products':  # old conversion fails here

                validate_json_schema_completeness(next_schema_input)

                schema_0_input = copy.deepcopy(next_schema_input)

                schema_0_input.update({"type": "SCHEMA"})

                schema_0_input = str(schema_0_input)

                schema_0_input = schema_0_input.replace("\'", "\"").replace("True", "true").replace("False",
                                                                                                    "false").replace(
                    "None", "null")

                msg = singer.parse_message(schema_0_input)

                schema_1_simplified = simplify(msg.schema)

                schema_2_built_new_method = build_schema(schema_1_simplified, key_properties=msg.key_properties,
                                                         add_metadata=True)

                # old conversion fails on asana workspaces
                # schema_3_built_old_method = build_schema_old(msg.schema, key_properties=msg.key_properties, add_metadata=True)

                # are results of the two methods above identical? ignore order of columns and case
                schema_built_new_method_sorted = convert_list_of_schema_fields_to_list_of_lists(
                    schema_2_built_new_method)

                assert schema_built_new_method_sorted

    def test_several_nested_schemas_salesforce(self):

        compare_old_vs_new_schema_conversion(os.path.join(os.path.join(
            os.path.join(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "tests"), "rsc"),
            "schemas"), "input_json_schemas_salesforce.json"))

    def test_several_nested_schemas_shopify(self):

        compare_old_vs_new_schema_conversion(os.path.join(os.path.join(
            os.path.join(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "tests"), "rsc"),
            "schemas"), "input_json_schemas_shopify.json"))

    def test_several_nested_schemas_quickbooks(self):

        compare_old_vs_new_schema_conversion(os.path.join(os.path.join(
            os.path.join(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "tests"), "rsc"),
            "schemas"), "input_json_schemas_quickbooks.json"))
