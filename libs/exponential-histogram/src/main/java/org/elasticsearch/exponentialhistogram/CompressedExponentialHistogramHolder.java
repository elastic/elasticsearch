/*
 * Copyright Elasticsearch B.V., and/or licensed to Elasticsearch B.V.
 * under one or more license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch B.V. licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 * This file is based on a modification of https://github.com/open-telemetry/opentelemetry-java which is licensed under the Apache 2.0 License.
 */

package org.elasticsearch.exponentialhistogram;

import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.core.Releasable;

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
public class CompressedExponentialHistogramHolder implements Releasable {

    private static final long BASE_SIZE = RamUsageEstimator.shallowSizeOfInstance(CompressedExponentialHistogramHolder.class)
        + CompressedExponentialHistogram.SIZE;

    private final BreakingBytesRefOutputStream encodedHistogramBuffer;
    private final CompressedExponentialHistogram histogram;
    private final ExponentialHistogramCircuitBreaker breaker;

    public static CompressedExponentialHistogramHolder create(ExponentialHistogramCircuitBreaker breaker) {
        BreakingBytesRefOutputStream buffer = null;
        boolean success = false;
        try {
            buffer = new BreakingBytesRefOutputStream(breaker);
            breaker.adjustBreaker(BASE_SIZE);
            var histogram = new CompressedExponentialHistogram();
            success = true;
            return new CompressedExponentialHistogramHolder(buffer, histogram, breaker);
        } finally {
            if (success == false) {
                if (buffer != null) {
                    buffer.close();
                }
            }
        }
    }

    private CompressedExponentialHistogramHolder(
        BreakingBytesRefOutputStream encodedHistogramBuffer,
        CompressedExponentialHistogram histogram,
        ExponentialHistogramCircuitBreaker breaker
    ) {
        this.encodedHistogramBuffer = encodedHistogramBuffer;
        this.histogram = histogram;
        this.breaker = breaker;
    }

    /**
     * Sets this holder to a copy of the given histogram.
     */
    public void set(ExponentialHistogram incoming) {
        encodedHistogramBuffer.clear();
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
                encodedHistogramBuffer.bytes
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

    /**
     * @return the estimated RAM usage of this holder including the encoded buffer.
     */
    public long ramBytesUsed() {
        return BASE_SIZE + encodedHistogramBuffer.ramBytesUsed();
    }

    @Override
    public void close() {
        breaker.adjustBreaker(-BASE_SIZE);
        encodedHistogramBuffer.close();
    }

    /**
     * {@link OutputStream} that manages its own {@code byte[]} with circuit breaker accounting.
     */
    static class BreakingBytesRefOutputStream extends OutputStream implements Releasable {

        private static final long SHALLOW_SIZE = RamUsageEstimator.shallowSizeOfInstance(BreakingBytesRefOutputStream.class)
            + RamUsageEstimator.shallowSizeOfInstance(BytesRef.class);

        private final ExponentialHistogramCircuitBreaker breaker;
        private final BytesRef bytes;

        BreakingBytesRefOutputStream(ExponentialHistogramCircuitBreaker breaker) {
            this.breaker = breaker;
            breaker.adjustBreaker(SHALLOW_SIZE + bytesArrayRamBytesUsed(0));
            this.bytes = new BytesRef();
        }

        @Override
        public void write(int b) {
            grow(bytes.length + 1);
            bytes.bytes[bytes.length++] = (byte) b;
        }

        @Override
        public void write(byte[] b, int off, int len) {
            grow(bytes.length + len);
            System.arraycopy(b, off, bytes.bytes, bytes.length, len);
            bytes.length += len;
        }

        void clear() {
            bytes.length = 0;
        }

        private void grow(int capacity) {
            int oldCapacity = bytes.bytes.length;
            if (oldCapacity >= capacity) {
                return;
            }
            int newCapacity = ArrayUtil.oversize(capacity, Byte.BYTES);
            breaker.adjustBreaker(bytesArrayRamBytesUsed(newCapacity));
            bytes.bytes = ArrayUtil.growExact(bytes.bytes, newCapacity);
            breaker.adjustBreaker(-bytesArrayRamBytesUsed(oldCapacity));
        }

        long ramBytesUsed() {
            return SHALLOW_SIZE + bytesArrayRamBytesUsed(bytes.bytes.length);
        }

        @Override
        public void close() {
            breaker.adjustBreaker(-ramBytesUsed());
        }

        private static long bytesArrayRamBytesUsed(long capacity) {
            return RamUsageEstimator.alignObjectSize(RamUsageEstimator.NUM_BYTES_ARRAY_HEADER + capacity);
        }
    }
}
