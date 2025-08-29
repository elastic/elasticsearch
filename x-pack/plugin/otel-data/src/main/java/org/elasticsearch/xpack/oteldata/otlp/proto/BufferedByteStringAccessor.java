/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.oteldata.otlp.proto;

import com.google.protobuf.ByteString;

import org.elasticsearch.cluster.routing.TsidBuilder;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;

/**
 * A utility class that uses a shared {@code byte[]} buffer to convert {@link ByteString} values to byte arrays.
 * This avoids frequent allocations of byte arrays in {@link ByteString#toByteArray()}.
 * Note that due to the use of a shared buffer, this class is not thread-safe.
 */
public class BufferedByteStringAccessor {

    private static final int DEFAULT_INITIAL_SIZE = 128;

    private byte[] bytes = new byte[DEFAULT_INITIAL_SIZE];

    /**
     * Adds a string dimension to the given {@link TsidBuilder} using the provided dimension name and {@link ByteString} value.
     * The value is converted to a byte array using a shared buffer and added to the builder.
     *
     * @param tsidBuilder the builder to which the dimension will be added
     * @param dimension   the name of the dimension to add
     * @param value       the value of the dimension as a {@link ByteString}
     */
    public void addStringDimension(TsidBuilder tsidBuilder, String dimension, ByteString value) {
        if (value.isEmpty()) {
            // Ignoring invalid values
            // According to the spec https://opentelemetry.io/docs/specs/otel/common/#attribute:
            // The attribute key MUST be a non-null and non-empty string.
            return;
        }
        tsidBuilder.addStringDimension(dimension, toBytes(value), 0, value.size());
    }

    /**
     * Writes a UTF-8 encoded value to the provided {@link XContentBuilder} using {@link XContentBuilder#utf8Value}.
     * This uses a shared byte array to avoid allocations.
     *
     * @param value the value to write
     */
    public void utf8Value(XContentBuilder builder, ByteString value) throws IOException {
        builder.utf8Value(toBytes(value), 0, value.size());
    }

    /*
     * Not exposed as a public method to avoid risks of leaking a reference to the reused byte array.
     */
    private byte[] toBytes(ByteString byteString) {
        int size = byteString.size();
        if (bytes.length < size) {
            bytes = new byte[size];
        }
        byteString.copyTo(bytes, 0);
        return bytes;
    }
}
