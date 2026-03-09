/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.compute.operator.BreakingBytesRefBuilder;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.tdigest.TDigestReadView;
import org.elasticsearch.xpack.core.analytics.mapper.EncodedTDigest;

import java.io.IOException;

/**
 * Can be used to deep copy or generate data for {@link TDigestHolder}s with correct memory accounting.
 */
public class BreakingTDigestHolder implements Releasable, Accountable {

    private static final String CIRCUIT_BREAKER_LABEL = "BreakingTDigestHolder";
    private static final long BASE_SIZE = TDigestHolder.RAM_BYTES + RamUsageEstimator.shallowSizeOfInstance(BreakingTDigestHolder.class);

    private final TDigestHolder holder;

    private final BreakingBytesStreamOutput encodedDigestBuffer;
    private final CircuitBreaker breaker;

    public static BreakingTDigestHolder create(CircuitBreaker breaker) {
        BreakingBytesStreamOutput buffer = null;
        boolean success = false;
        try {
            buffer = new BreakingBytesStreamOutput(breaker);
            TDigestHolder holder = new TDigestHolder();
            breaker.addEstimateBytesAndMaybeBreak(BASE_SIZE, CIRCUIT_BREAKER_LABEL);
            success = true;
            return new BreakingTDigestHolder(holder, buffer, breaker);
        } finally {
            if (success == false) {
                Releasables.close(buffer);
            }
        }
    }

    private BreakingTDigestHolder(TDigestHolder holder, BreakingBytesStreamOutput encodedDigestBuffer, CircuitBreaker breaker) {
        this.holder = holder;
        this.encodedDigestBuffer = encodedDigestBuffer;
        this.breaker = breaker;
    }

    public void set(TDigestReadView tdigest, double sum, double min, double max) {
        encodedDigestBuffer.clear();
        try {
            EncodedTDigest.encodeCentroids(tdigest.centroids(), encodedDigestBuffer);
        } catch (IOException e) {
            throw new IllegalStateException("Centroid encoding threw exception", e);
        }
        holder.reset(encodedDigestBuffer.bytesRefView(), min, max, sum, tdigest.size());
    }

    public void set(TDigestHolder tdigest) {
        encodedDigestBuffer.clear();
        BytesRef bytesToCopy = tdigest.getEncodedDigest();
        encodedDigestBuffer.writeBytes(bytesToCopy.bytes, bytesToCopy.offset, bytesToCopy.length);
        holder.reset(encodedDigestBuffer.bytesRefView(), tdigest.getMin(), tdigest.getMax(), tdigest.getSum(), tdigest.size());
    }

    public TDigestHolder accessor() {
        return holder;
    }

    @Override
    public long ramBytesUsed() {
        return BASE_SIZE + encodedDigestBuffer.ramBytesUsed();
    }

    @Override
    public void close() {
        breaker.addWithoutBreaking(-BASE_SIZE);
        Releasables.close(encodedDigestBuffer);
    }

    private static class BreakingBytesStreamOutput extends StreamOutput implements Releasable, Accountable {

        private final long SHALLOW_SIZE = RamUsageEstimator.shallowSizeOfInstance(BreakingBytesStreamOutput.class);

        private final BreakingBytesRefBuilder delegate;
        private final CircuitBreaker breaker;

        BreakingBytesStreamOutput(CircuitBreaker breaker) {
            this.breaker = breaker;
            this.delegate = new BreakingBytesRefBuilder(breaker, CIRCUIT_BREAKER_LABEL);
            breaker.addEstimateBytesAndMaybeBreak(SHALLOW_SIZE, CIRCUIT_BREAKER_LABEL);
        }

        void clear() {
            delegate.clear();
        }

        BytesRef bytesRefView() {
            return delegate.bytesRefView();
        }

        @Override
        public long position() {
            return delegate.length();
        }

        @Override
        public void writeByte(byte b) {
            delegate.append(b);
        }

        @Override
        public void writeBytes(byte[] b, int offset, int length) {
            delegate.append(b, offset, length);
        }

        @Override
        public void writeOptionalString(String str) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void writeString(String str) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void flush() {
            // no-op
        }

        @Override
        public void close() {
            breaker.addWithoutBreaking(-SHALLOW_SIZE);
            delegate.close();
        }

        @Override
        public void writeGenericString(String value) {
            throw new UnsupportedOperationException();
        }

        @Override
        public long ramBytesUsed() {
            return SHALLOW_SIZE + delegate.ramBytesUsed();
        }
    }
}
