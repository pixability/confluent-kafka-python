#!/usr/bin/env python
#
# Copyright 2016 Confluent Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#


#
# derived from https://github.com/verisign/python-confluent-schemaregistry.git
#

import struct

import unittest

from tests.avro import data_gen
from confluent_kafka.avro.serializer.message_serializer import MessageSerializer
from confluent_kafka.avro.serializer.specific_record_message_deserializer import SpecificRecordMessageDeserializer
from tests.avro.mock_schema_registry_client import MockSchemaRegistryClient
from confluent_kafka import avro


class TestMessageSerializer(unittest.TestCase):
    def setUp(self):
        # need to set up the serializer
        self.client = MockSchemaRegistryClient()
        self.ms = MessageSerializer(self.client)

    def assertMessageIsSame(self, message, expected, schema_id):
        self.assertTrue(message)
        self.assertTrue(len(message) > 5)
        magic, sid = struct.unpack('>bI', message[0:5])
        self.assertEqual(magic, 0)
        self.assertEqual(sid, schema_id)
        decoded = self.ms.decode_message(message)
        self.assertTrue(decoded)
        self.assertEqual(decoded, expected)

    def test_encode_with_schema_id(self):
        adv = avro.loads(data_gen.ADVANCED_SCHEMA)
        basic = avro.loads(data_gen.BASIC_SCHEMA)
        subject = 'test'
        schema_id = self.client.register(subject, basic)

        records = data_gen.BASIC_ITEMS
        for record in records:
            message = self.ms.encode_record_with_schema_id(schema_id, record)
            self.assertMessageIsSame(message, record, schema_id)

        subject = 'test_adv'
        adv_schema_id = self.client.register(subject, adv)
        self.assertNotEqual(adv_schema_id, schema_id)
        records = data_gen.ADVANCED_ITEMS
        for record in records:
            message = self.ms.encode_record_with_schema_id(adv_schema_id, record)
            self.assertMessageIsSame(message, record, adv_schema_id)

    def test_encode_record_with_schema(self):
        topic = 'test'
        basic = avro.loads(data_gen.BASIC_SCHEMA)
        subject = 'test-value'
        schema_id = self.client.register(subject, basic)
        records = data_gen.BASIC_ITEMS
        for record in records:
            message = self.ms.encode_record_with_schema(topic, basic, record)
            self.assertMessageIsSame(message, record, schema_id)

    def test_decode_none(self):
        """"null/None messages should decode to None"""
        self.assertIsNone(self.ms.decode_message(None))

    def test_decode_with_schema(self):
        topic = 'test_specific'

        schema_v1 = avro.loads(data_gen.load_schema_file('evolution_schema_v1.avsc'))
        schema_v2 = avro.loads(data_gen.load_schema_file('evolution_schema_v2.avsc'))

        dsv1 = SpecificRecordMessageDeserializer(self.client, value_schema=schema_v1)
        dsv2 = SpecificRecordMessageDeserializer(self.client, value_schema=schema_v2)

        record_v1 = {"name": "suzyq", "age": 27}
        record_v2 = dict(record_v1)
        record_v2['gender'] = 'NONE'

        encoded_v1 = self.ms.encode_record_with_schema(topic, schema_v1, record_v1)
        decoded_v1_v1 = dsv1.decode_message(encoded_v1, is_key=False)
        self.assertDictEqual(record_v1, decoded_v1_v1)
        decoded_v1_v2 = dsv2.decode_message(encoded_v1, is_key=False)
        self.assertDictEqual(record_v2, decoded_v1_v2)

        encoded_v2 = self.ms.encode_record_with_schema(topic, schema_v2, record_v2)
        decoded_v2_v2 = dsv2.decode_message(encoded_v2, is_key=False)
        self.assertDictEqual(record_v2, decoded_v2_v2)
        decoded_v2_v1 = dsv1.decode_message(encoded_v2, is_key=False)
        self.assertDictEqual(record_v1, decoded_v2_v1)

    def hash_func(self):
        return hash(str(self))
