/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.StringHelper;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.hash.Murmur3Hasher;
import org.elasticsearch.common.hash.MurmurHash3;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.util.ByteUtils;
import org.elasticsearch.search.DocValueFormat;

import java.io.IOException;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Tracks dimension fields and uses their values to build a byte array containing a representative hash value.
 */
public class DimensionHasher {

    private static final Base64.Encoder BASE64_ENCODER = Base64.getUrlEncoder().withoutPadding();

    private static final int SEED = 0;

    public static final int MAX_DIMENSIONS = 512;

    private static final int MAX_HASH_LEN_BYTES = 2;

    static {
        assert MAX_HASH_LEN_BYTES == StreamOutput.putVInt(new byte[2], hashLen(MAX_DIMENSIONS), 0);
    }

    private static int hashLen(int numberOfDimensions) {
        return 16 + 16 + 4 * numberOfDimensions;
    }

    /**
     * Here we build the hash of the tsid using a similarity function so that we have a result
     * with the following pattern:
     *
     * hash128(catenate(dimension field names)) +
     * foreach(dimension field value, limit = MAX_DIMENSIONS) { hash32(dimension field value) } +
     * hash128(catenate(dimension field values))
     *
     * The idea is to be able to place similar groups of dimension values close to each other.
     */
    public static BytesReference build(RoutingDimensions routingDimensions) {
        var dimensions = routingDimensions.dimensions();
        Murmur3Hasher hasher = new Murmur3Hasher(SEED);

        // NOTE: hash all dimension field names
        int numberOfDimensions = Math.min(MAX_DIMENSIONS, dimensions.size());
        int len = hashLen(numberOfDimensions);
        // either one or two bytes are occupied by the vint since we're bounded by #MAX_DIMENSIONS
        byte[] bytes = new byte[MAX_HASH_LEN_BYTES + len];
        int index = StreamOutput.putVInt(bytes, len, 0);

        hasher.reset();
        for (final RoutingDimensions.Dimension dimension : dimensions) {
            hasher.update(dimension.name().bytes);
        }
        index = writeHash128(hasher.digestHash(), bytes, index);

        // NOTE: concatenate all dimension value hashes up to a certain number of dimensions
        int startIndex = index;
        for (final RoutingDimensions.Dimension dimension : dimensions) {
            if ((index - startIndex) >= 4 * numberOfDimensions) {
                break;
            }
            final BytesRef value = dimension.value().toBytesRef();
            ByteUtils.writeIntLE(StringHelper.murmurhash3_x86_32(value.bytes, value.offset, value.length, SEED), bytes, index);
            index += 4;
        }

        // NOTE: hash all dimension field values
        hasher.reset();
        for (final RoutingDimensions.Dimension dimension : dimensions) {
            hasher.update(dimension.value().toBytesRef().bytes);
        }
        index = writeHash128(hasher.digestHash(), bytes, index);

        return new BytesArray(bytes, 0, index);
    }

    private static int writeHash128(final MurmurHash3.Hash128 hash128, byte[] buffer, int tsidHashIndex) {
        ByteUtils.writeLongLE(hash128.h1, buffer, tsidHashIndex);
        tsidHashIndex += 8;
        ByteUtils.writeLongLE(hash128.h2, buffer, tsidHashIndex);
        tsidHashIndex += 8;
        return tsidHashIndex;
    }

    static Object encode(StreamInput in) {
        try {
            return base64Encode(in.readSlicedBytesReference().toBytesRef());
        } catch (IOException e) {
            throw new IllegalArgumentException("Unable to read tsid");
        }
    }

    public static Object encode(final BytesRef bytesRef) {
        return base64Encode(bytesRef);
    }

    private static String base64Encode(final BytesRef bytesRef) {
        byte[] bytes = new byte[bytesRef.length];
        System.arraycopy(bytesRef.bytes, bytesRef.offset, bytes, 0, bytesRef.length);
        return BASE64_ENCODER.encodeToString(bytes);
    }

    public static Map<String, Object> decodeAsMap(BytesRef bytesRef) {
        try (StreamInput input = new BytesArray(bytesRef).streamInput()) {
            return decodeAsMap(input);
        } catch (IOException ex) {
            throw new IllegalArgumentException("Dimension field cannot be deserialized.", ex);
        }
    }

    public static Map<String, Object> decodeAsMap(StreamInput in) throws IOException {
        int size = in.readVInt();
        Map<String, Object> result = new LinkedHashMap<>(size);

        for (int i = 0; i < size; i++) {
            String name = null;
            try {
                name = in.readSlicedBytesReference().utf8ToString();
            } catch (AssertionError ae) {
                throw new IllegalArgumentException("Error parsing keyword dimension: " + ae.getMessage(), ae);
            }

            int type = in.read();
            switch (type) {
                case (byte) 's' -> {
                    // parse a string
                    try {
                        result.put(name, in.readSlicedBytesReference().utf8ToString());
                    } catch (AssertionError ae) {
                        throw new IllegalArgumentException("Error parsing keyword dimension: " + ae.getMessage(), ae);
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
                default -> throw new IllegalArgumentException("Cannot parse [" + name + "]: Unknown type [" + type + "]");
            }
        }
        return result;
    }
}
