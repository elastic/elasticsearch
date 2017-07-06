/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.test;

import org.elasticsearch.common.CheckedBiConsumer;
import org.elasticsearch.common.CheckedFunction;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;

import static org.junit.Assert.assertEquals;

/**
 * Base class for testing round trips across the serialization protocol.
 */
public abstract class RoundTripTestUtils {
    private RoundTripTestUtils () {
        // Only static utilities here
    }

    public static <T> void assertRoundTrip(T example, CheckedBiConsumer<T, DataOutput, IOException> encode,
            CheckedFunction<DataInput, T, IOException> decode) throws IOException {
        T once = roundTrip(example, encode, decode);
        assertEquals(example, once);
        T twice = roundTrip(once, encode, decode);
        assertEquals(example, twice);
        assertEquals(once, twice);
    }

    public static <T> T roundTrip(T example, CheckedBiConsumer<T, DataOutput, IOException> encode,
            CheckedFunction<DataInput, T, IOException> decode) throws IOException {
        try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            encode.accept(example, new DataOutputStream(out));
            try (InputStream in = new ByteArrayInputStream(out.toByteArray())) {
                T decoded = decode.apply(new DataInputStream(in));
                assertEquals("should have emptied the stream", 0, in.available());
                return decoded;
            }
        }
    }
}
