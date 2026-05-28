/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.compute.operator.BreakingBytesRefBuilder;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.exponentialhistogram.CompressedExponentialHistogram;
import org.elasticsearch.exponentialhistogram.ExponentialHistogram;

import java.io.IOException;
import java.io.OutputStream;

/**
 * Holds an {@link ExponentialHistogram} in its encoded form using a circuit-breaking byte buffer
 * for proper memory accounting.
 * <p>
 * When set to a {@link CompressedExponentialHistogram}, the already-encoded bytes are copied directly
 * without re-encoding. For other {@link ExponentialHistogram} implementations, the histogram is encoded
 * into the compressed format first.
 * <p>
 * The stored histogram can be accessed as a {@link ExponentialHistogram} via {@link #accessor()}.
 */
public class BreakingExponentialHistogramHolder implements Releasable, Accountable {

    private static final String CIRCUIT_BREAKER_LABEL = "BreakingExponentialHistogramHolder";
    private static final long BASE_SIZE = RamUsageEstimator.shallowSizeOfInstance(BreakingExponentialHistogramHolder.class)
        + CompressedExponentialHistogram.SIZE;

    private final BreakingBytesRefOutputStream encodedHistogramBuffer;
    private final CompressedExponentialHistogram histogram;
    private final CircuitBreaker breaker;

    public static BreakingExponentialHistogramHolder create(CircuitBreaker breaker) {
        BreakingBytesRefOutputStream buffer = null;
        boolean success = false;
        try {
            buffer = new BreakingBytesRefOutputStream(breaker);
            breaker.addEstimateBytesAndMaybeBreak(BASE_SIZE, CIRCUIT_BREAKER_LABEL);
            var histogram = new CompressedExponentialHistogram();
            success = true;
            return new BreakingExponentialHistogramHolder(buffer, histogram, breaker);
        } finally {
            if (success == false) {
                Releasables.close(buffer);
            }
        }
    }

    private BreakingExponentialHistogramHolder(
        BreakingBytesRefOutputStream encodedHistogramBuffer,
        CompressedExponentialHistogram histogram,
        CircuitBreaker breaker
    ) {
        this.encodedHistogramBuffer = encodedHistogramBuffer;
        this.histogram = histogram;
        this.breaker = breaker;
    }

    /**
     * Sets this holder to a copy of the given histogram.
     */
    public void set(ExponentialHistogram incoming) {
        encodedHistogramBuffer.delegate.clear();
        try {
            CompressedExponentialHistogram.writeHistogramBytes(encodedHistogramBuffer, incoming);
        } catch (IOException e) {
            throw new IllegalStateException("Histogram encoding failed", e);
        }
        try {
            histogram.reset(
                incoming.zeroBucket().zeroThreshold(),
                incoming.valueCount(),
                incoming.sum(),
                incoming.min(),
                incoming.max(),
                encodedHistogramBuffer.delegate.bytesRefView()
            );
        } catch (IOException e) {
            throw new IllegalStateException("Histogram decoding failed", e);
        }
    }

    /**
     * Provides read access to the histogram this holder stores.
     */
    public ExponentialHistogram accessor() {
        return histogram;
    }

    @Override
    public long ramBytesUsed() {
        return BASE_SIZE + encodedHistogramBuffer.ramBytesUsed();
    }

    @Override
    public void close() {
        breaker.addWithoutBreaking(-BASE_SIZE);
        Releasables.close(encodedHistogramBuffer);
    }

    /**
     * {@link OutputStream} adapter backed by a {@link BreakingBytesRefBuilder} for circuit-breaking writes.
     */
    private static class BreakingBytesRefOutputStream extends OutputStream implements Releasable, Accountable {

        private static final long SHALLOW_SIZE = RamUsageEstimator.shallowSizeOfInstance(BreakingBytesRefOutputStream.class);

        private final BreakingBytesRefBuilder delegate;
        private final CircuitBreaker breaker;

        BreakingBytesRefOutputStream(CircuitBreaker breaker) {
            this.breaker = breaker;
            BreakingBytesRefBuilder delegate = new BreakingBytesRefBuilder(breaker, CIRCUIT_BREAKER_LABEL);
            boolean success = false;
            try {
                breaker.addEstimateBytesAndMaybeBreak(SHALLOW_SIZE, CIRCUIT_BREAKER_LABEL);
                success = true;
            } finally {
                if (success == false) {
                    Releasables.close(delegate);
                }
            }
            this.delegate = delegate;
        }

        @Override
        public void write(int b) {
            delegate.append((byte) b);
        }

        @Override
        public void write(byte[] b, int off, int len) {
            delegate.append(b, off, len);
        }

        @Override
        public long ramBytesUsed() {
            return SHALLOW_SIZE + delegate.ramBytesUsed();
        }

        @Override
        public void close() {
            breaker.addWithoutBreaking(-SHALLOW_SIZE);
            delegate.close();
        }
    }
}
