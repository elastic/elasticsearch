/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.StringHelper;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.hash.Murmur3Hasher;
import org.elasticsearch.common.hash.MurmurHash3;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.util.ByteUtils;
import org.elasticsearch.search.DocValueFormat;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Calculates a hash value over all the fields matching the routing path content.
 */
public class RoutingHasher {

    private static final int SEED = 0;

    public static final int MAX_ROUTING_FIELDS = 512;

    private static final int MAX_HASH_LEN_BYTES = 2;

    static {
        assert MAX_HASH_LEN_BYTES == StreamOutput.putVInt(new byte[2], hashLen(MAX_ROUTING_FIELDS), 0);
    }

    private RoutingHasher() {}  // No instance allowed.

    /**
     * Here we build the hash of the routing values using a similarity function so that we have a result
     * with the following pattern:
     *
     * hash128(concatenate(routing field names)) +
     * foreach(routing field value, limit = MAX_ROUTING_FIELDS) { hash32(routing field value) } +
     * hash128(concatenate(routing field values))
     *
     * The idea is to be able to place 'similar' values close to each other.
     */
    public static BytesReference build(RoutingPathFields routingFields) {
        var routingValues = routingFields.routingValues();
        Murmur3Hasher hasher = new Murmur3Hasher(SEED);

        // NOTE: hash all routing field names
        int numberOfFields = Math.min(MAX_ROUTING_FIELDS, routingValues.size());
        int len = hashLen(numberOfFields);
        // either one or two bytes are occupied by the vint since we're bounded by #MAX_ROUTING_FIELDS
        byte[] hash = new byte[MAX_HASH_LEN_BYTES + len];
        int index = StreamOutput.putVInt(hash, len, 0);

        hasher.reset();
        for (final BytesRef name : routingValues.keySet()) {
            hasher.update(name.bytes);
        }
        index = writeHash128(hasher.digestHash(), hash, index);

        // NOTE: concatenate all routing field value hashes up to a certain number of fields
        int startIndex = index;
        for (final List<BytesReference> values : routingValues.values()) {
            if ((index - startIndex) >= 4 * numberOfFields) {
                break;
            }
            assert values.isEmpty() == false : "routing values are empty";
            final BytesRef routingValue = values.get(0).toBytesRef();
            ByteUtils.writeIntLE(
                StringHelper.murmurhash3_x86_32(routingValue.bytes, routingValue.offset, routingValue.length, SEED),
                hash,
                index
            );
            index += 4;
        }

        // NOTE: hash all routing field allValues
        hasher.reset();
        for (final List<BytesReference> values : routingValues.values()) {
            for (BytesReference v : values) {
                hasher.update(v.toBytesRef().bytes);
            }
        }
        index = writeHash128(hasher.digestHash(), hash, index);

        return new BytesArray(hash, 0, index);
    }

    private static int hashLen(int numberOfFields) {
        return 16 + 16 + 4 * numberOfFields;
    }

    private static int writeHash128(final MurmurHash3.Hash128 hash128, byte[] buffer, int index) {
        ByteUtils.writeLongLE(hash128.h1, buffer, index);
        index += 8;
        ByteUtils.writeLongLE(hash128.h2, buffer, index);
        index += 8;
        return index;
    }

    public static Object encode(final BytesRef bytesRef) {
        return base64Encode(bytesRef);
    }

    private static String base64Encode(final BytesRef bytesRef) {
        byte[] bytes = new byte[bytesRef.length];
        System.arraycopy(bytesRef.bytes, bytesRef.offset, bytes, 0, bytesRef.length);
        return Strings.BASE_64_NO_PADDING_URL_ENCODER.encodeToString(bytes);
    }

    public static Map<String, Object> decodeAsMap(BytesRef bytesRef) {
        try (StreamInput input = new BytesArray(bytesRef).streamInput()) {
            return decodeAsMap(input);
        } catch (IOException ex) {
            throw new IllegalArgumentException("Routing field cannot be deserialized.", ex);
        }
    }

    public static Map<String, Object> decodeAsMap(StreamInput in) {
        try {
            int size = in.readVInt();
            Map<String, Object> result = new LinkedHashMap<>(size);

            for (int i = 0; i < size; i++) {
                String name = null;
                try {
                    name = in.readSlicedBytesReference().utf8ToString();
                } catch (AssertionError ae) {
                    throw new IllegalArgumentException("Error parsing routing field: " + ae.getMessage(), ae);
                }

                int type = in.read();
                switch (type) {
                    case (byte) 's' -> {
                        // parse a string
                        try {
                            result.put(name, in.readSlicedBytesReference().utf8ToString());
                        } catch (AssertionError ae) {
                            throw new IllegalArgumentException("Error parsing routing field: " + ae.getMessage(), ae);
                        }
                    }
                    case (byte) 'l' -> // parse a long
                        result.put(name, in.readLong());
                    case (byte) 'u' -> { // parse an unsigned_long
                        Object ul = DocValueFormat.UNSIGNED_LONG_SHIFTED.format(in.readLong());
                        result.put(name, ul);
                    }
                    case (byte) 'd' -> // parse a double
                        result.put(name, in.readDouble());
                    case (byte) 'b' -> // parse a boolean
                        result.put(name, in.read() == 't');
                    default -> throw new IllegalArgumentException("Cannot parse [" + name + "]: Unknown type [" + type + "]");
                }
            }
            return result;
        } catch (IOException | IllegalArgumentException e) {
            throw new IllegalArgumentException("Error while decoding routing values: " + e.getMessage(), e);
        }
    }
}
