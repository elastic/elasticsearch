/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.planner;

import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.compute.operator.BreakingBytesRefBuilder;
import org.elasticsearch.index.mapper.TimeSeriesMetadataFieldBlockLoader.MetadataEncoder;
import org.elasticsearch.xpack.esql.core.util.TimeSeriesMetadata;

import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import java.util.Map;

/** Encodes indexed dimensions using the typed metadata representation, with circuit breaker accounting. */
final class TimeSeriesMetadataEncoder extends OutputStream implements MetadataEncoder {
    private final CircuitBreaker breaker;
    private final BreakingBytesRefBuilder buffer;

    TimeSeriesMetadataEncoder(CircuitBreaker breaker) {
        this.breaker = breaker;
        this.buffer = new BreakingBytesRefBuilder(breaker, "time-series metadata");
    }

    @Override
    public BytesRef encode(Map<String, List<String>> labels) throws IOException {
        long scratch = TimeSeriesMetadata.encodingScratchBytes();
        breaker.addEstimateBytesAndMaybeBreak(scratch, "time-series metadata encoding");
        try {
            buffer.clear();
            TimeSeriesMetadata.ofValues(labels).writeTo(this);
            return buffer.bytesRefView();
        } finally {
            breaker.addWithoutBreaking(-scratch);
        }
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

    @Override
    public void close() {
        buffer.close();
    }
}
