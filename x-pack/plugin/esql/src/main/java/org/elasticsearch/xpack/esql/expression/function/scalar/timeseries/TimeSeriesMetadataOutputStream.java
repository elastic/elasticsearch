/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.timeseries;

import org.apache.lucene.util.ArrayUtil;
import org.elasticsearch.compute.operator.BreakingBytesRefBuilder;

import java.io.OutputStream;

/**
 * Writes time-series metadata into a {@link BreakingBytesRefBuilder}, which accounts every byte it allocates. Rejects output
 * that would grow past the maximum array length instead of letting the builder overflow.
 */
public final class TimeSeriesMetadataOutputStream extends OutputStream {
    private final BreakingBytesRefBuilder buffer;

    public TimeSeriesMetadataOutputStream(BreakingBytesRefBuilder buffer) {
        this.buffer = buffer;
    }

    @Override
    public void write(int value) {
        checkLength(1);
        buffer.append((byte) value);
    }

    @Override
    public void write(byte[] bytes, int offset, int length) {
        checkLength(length);
        buffer.append(bytes, offset, length);
    }

    private void checkLength(int additional) {
        if ((long) buffer.length() + additional > ArrayUtil.MAX_ARRAY_LENGTH) {
            throw new IllegalArgumentException("Time-series metadata exceeds the maximum array size");
        }
    }
}
