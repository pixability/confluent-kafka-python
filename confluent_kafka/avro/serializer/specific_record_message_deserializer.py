import sys
import struct

import avro
import avro.io

from confluent_kafka.avro import ClientError
from confluent_kafka.avro.serializer import (MAGIC_BYTE, ContextStringIO, SerializerError)


class SpecificRecordMessageDeserializer(object):
    """
    A helper class that can deserialize messages for a given
    pair of key and value schemas
    """

    def __init__(self, registry_client, value_schema, key_schema=None):
        """

        :param registry_client:
        :param value_schema:
        :param key_schema:
        """
        self.registry_client = registry_client
        self.value_schema = value_schema
        self.key_schema = key_schema
        self.id_to_decoder_func = {}

    def _get_decoder_func(self, schema_id, is_key):
        if schema_id in self.id_to_decoder_func:
            return self.id_to_decoder_func[schema_id]

        # fetch from schema reg
        try:
            schema = self.registry_client.get_by_id(schema_id)
        except ClientError as e:
            raise SerializerError("unable to fetch schema with id %d: %s" % (schema_id, str(e)))

        if schema is None:
            raise SerializerError("unable to fetch schema with id %d" % schema_id)

        if sys.version_info[0] < 3:
            avro_reader = avro.io.DatumReader(readers_schema=self.key_schema if is_key else self.value_schema,
                                              writers_schema=schema)
        else:
            avro_reader = avro.io.DatumReader(reader_schema=self.key_schema if is_key else self.value_schema,
                                              writer_schema=schema)

        def decoder(p):
            bin_decoder = avro.io.BinaryDecoder(p)
            return avro_reader.read(bin_decoder)

        self.id_to_decoder_func[schema_id] = decoder
        return self.id_to_decoder_func[schema_id]

    def decode_message(self, message, is_key):
        """
        Decode a message from kafka that has been encoded for use with
        the schema registry.
        @:param: message
        """

        if message is None:
            return None

        if len(message) <= 5:
            raise SerializerError("message is too small to decode")

        with ContextStringIO(message) as payload:
            magic, schema_id = struct.unpack('>bI', payload.read(5))
            if magic != MAGIC_BYTE:
                raise SerializerError("message does not start with magic byte")
            decoder_func = self._get_decoder_func(schema_id, is_key)
            return decoder_func(payload)
