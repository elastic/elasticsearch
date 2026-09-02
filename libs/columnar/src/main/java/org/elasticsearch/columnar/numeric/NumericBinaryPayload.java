/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar.numeric;

import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;

/**
 * Wire format for a document's numeric value(s) in a {@code BinaryDocValues} payload: each value a
 * little-endian 8-byte {@code long}, back to back, no count prefix (count = length / 8). Doubles are
 * carried as longs; values are never reordered.
 */
public final class NumericBinaryPayload {

    private static final VarHandle LONG = MethodHandles.byteArrayViewVarHandle(long[].class, ByteOrder.LITTLE_ENDIAN)
        .withInvokeExactBehavior();

    private NumericBinaryPayload() {}

    /** Packs {@code values[0..count)} as back-to-back longs into {@code out}, reusing its buffer. */
    public static BytesRef encode(long[] values, int count, BytesRefBuilder out) {
        out.grow(count * Long.BYTES);
        byte[] bytes = out.bytes();
        for (int i = 0; i < count; i++) {
            LONG.set(bytes, i * Long.BYTES, values[i]);
        }
        out.setLength(count * Long.BYTES);
        return out.get();
    }

    /**
     * Decodes a payload into {@code dest} (grown as needed) and returns the value count
     * ({@code payload.length / 8}). Values come back in the order they were written.
     *
     * <p>{@code dest} is a single-element array used only to pass the reusable buffer by mutable
     * reference: when the buffer must grow, the larger array is written back to {@code dest[0]} so the
     * caller sees it on the next call.
     */
    public static int decode(BytesRef payload, long[][] dest) {
        int count = payload.length / Long.BYTES;
        long[] values = dest[0];
        if (values.length < count) {
            values = new long[ArrayUtil.oversize(count, Long.BYTES)];
            dest[0] = values;
        }
        int base = payload.offset;
        for (int i = 0; i < count; i++) {
            values[i] = (long) LONG.get(payload.bytes, base + i * Long.BYTES);
        }
        return count;
    }
}
