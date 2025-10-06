/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.search.aggregations.metrics;

import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.exponentialhistogram.BucketIterator;
import org.elasticsearch.exponentialhistogram.ExponentialHistogram;
import org.elasticsearch.exponentialhistogram.ExponentialHistogramBuilder;
import org.elasticsearch.exponentialhistogram.ExponentialHistogramCircuitBreaker;
import org.elasticsearch.exponentialhistogram.ExponentialHistogramMerger;
import org.elasticsearch.exponentialhistogram.ReleasableExponentialHistogram;
import org.elasticsearch.exponentialhistogram.ZeroBucket;

import java.io.IOException;

import static org.elasticsearch.exponentialhistogram.ExponentialHistogram.MAX_SCALE;
import static org.elasticsearch.exponentialhistogram.ExponentialHistogram.MIN_SCALE;

/**
 * Decorates {@link ExponentialHistogram} with custom serialization and implements commonly used functionality for aggregations.
 */
public class ExponentialHistogramState implements Releasable, Accountable {

    // OpenTelemetry SDK default, we might make this configurable later
    static final int MAX_HISTOGRAM_BUCKETS = 320;

    private static final long SHALLOW_SIZE = RamUsageEstimator.shallowSizeOfInstance(ExponentialHistogramState.class);

    private static final byte EMPTY_HISTOGRAM_MARKER_SCALE = Byte.MIN_VALUE;
    static {
        assert EMPTY_HISTOGRAM_MARKER_SCALE < MIN_SCALE;
    }

    private boolean closed = false;

    private final CircuitBreaker circuitBreaker;
    private ReleasableExponentialHistogram deserializedHistogram;
    private ExponentialHistogramMerger mergedHistograms;

    /**
     * Creates a new, empty state.
     * @param circuitBreaker the circuit breaker to use for tracking memory usage
     * @return a new, empty state
     */
    public static ExponentialHistogramState create(CircuitBreaker circuitBreaker) {
        return create(circuitBreaker, null);
    }

    // Visible for testing
    static ExponentialHistogramState create(CircuitBreaker circuitBreaker, ReleasableExponentialHistogram deserializedHistogram) {
        circuitBreaker.addEstimateBytesAndMaybeBreak(SHALLOW_SIZE, "exponential-histogram-state");
        return new ExponentialHistogramState(circuitBreaker, deserializedHistogram);
    }

    private ExponentialHistogramState(CircuitBreaker circuitBreaker, ReleasableExponentialHistogram deserializedHistogram) {
        this.circuitBreaker = circuitBreaker;
        this.deserializedHistogram = deserializedHistogram;
    }

    // Visible for testing
    ExponentialHistogram histogram() {
        if (mergedHistograms != null) {
            return mergedHistograms.get();
        }
        if (deserializedHistogram != null) {
            return deserializedHistogram;
        }
        return ExponentialHistogram.empty();
    }

    /**
     * @return true if this state does not contain any histogram data, e.g. it was just created without adding any histograms.
     */
    public boolean isEmpty() {
        return mergedHistograms == null && deserializedHistogram == null;
    }

    public void add(ExponentialHistogram histogram) {
        if (mergedHistograms == null) {
            if (deserializedHistogram == null) {
                mergedHistograms = ExponentialHistogramMerger.create(
                    MAX_HISTOGRAM_BUCKETS,
                    new ElasticCircuitBreakerWrapper(circuitBreaker)
                );
            } else {
                // do not upscale the deserialized histogram
                mergedHistograms = ExponentialHistogramMerger.createWithMaxScale(
                    MAX_HISTOGRAM_BUCKETS,
                    deserializedHistogram.scale(),
                    new ElasticCircuitBreakerWrapper(circuitBreaker)
                );
                mergedHistograms.add(deserializedHistogram);
                deserializedHistogram.close();
                deserializedHistogram = null;
            }
        }
        mergedHistograms.add(histogram);
    }

    public void write(StreamOutput out) throws IOException {
        if (isEmpty()) {
            out.writeByte(EMPTY_HISTOGRAM_MARKER_SCALE);
        } else {
            assert MIN_SCALE >= Byte.MIN_VALUE && MAX_SCALE <= Byte.MAX_VALUE;
            ExponentialHistogram histogram = histogram();
            out.writeByte((byte) histogram.scale());
            out.writeDouble(histogram.min());
            out.writeDouble(histogram.max());
            out.writeDouble(histogram.sum());
            writeZeroBucket(out, histogram.zeroBucket());
            out.writeVInt(histogram.negativeBuckets().bucketCount());
            out.writeVInt(histogram.positiveBuckets().bucketCount());
            writeBuckets(out, histogram.negativeBuckets());
            writeBuckets(out, histogram.positiveBuckets());
        }
    }

    private static void writeZeroBucket(StreamOutput out, ZeroBucket zb) throws IOException {
        out.writeVLong(zb.count());
        boolean zeroThresholdIndexBased = zb.isIndexBased();
        out.writeBoolean(zeroThresholdIndexBased);
        if (zeroThresholdIndexBased) {
            out.writeByte((byte) zb.scale());
            out.writeZLong(zb.index());
        } else {
            out.writeDouble(zb.zeroThreshold());
        }
    }

    private static void writeBuckets(StreamOutput out, ExponentialHistogram.Buckets buckets) throws IOException {
        // We write the buckets with delta-encoding of the indexes, where a delta of 1 is implicit.
        // This allows for a good and yet fast compression using vlongs.
        // We write the indices as negative values (except for the first index) to distinguish them from the counts
        // So for example, the following buckets:
        // Index:  3    4    5    7    8
        // Count: 10 | 20 | 30 | 40 | 50
        // Would be written as:
        // 3, 10, 20, 30, -2, 40, 50
        BucketIterator bucketIterator = buckets.iterator();
        if (bucketIterator.hasNext()) {
            long lastIndex = bucketIterator.peekIndex();
            out.writeZLong(lastIndex);
            out.writeVLong(bucketIterator.peekCount());
            bucketIterator.advance();
            while (bucketIterator.hasNext()) {
                long index = bucketIterator.peekIndex();
                long delta = index - lastIndex;
                assert delta > 0;
                if (delta > 1) {
                    out.writeZLong(-delta);
                }
                out.writeZLong(bucketIterator.peekCount());
                lastIndex = index;
                bucketIterator.advance();
            }
        }
    }

    public static ExponentialHistogramState read(CircuitBreaker breaker, StreamInput in) throws IOException {
        ExponentialHistogramCircuitBreaker histoBreaker = new ElasticCircuitBreakerWrapper(breaker);
        byte scale = in.readByte();
        if (scale == EMPTY_HISTOGRAM_MARKER_SCALE) {
            return create(breaker);
        } else {
            try (ExponentialHistogramBuilder builder = ExponentialHistogram.builder(scale, histoBreaker)) {
                builder.min(in.readDouble());
                builder.max(in.readDouble());
                builder.sum(in.readDouble());
                builder.zeroBucket(readZeroBucket(in));
                int negativeBucketCount = in.readVInt();
                int positiveBucketCount = in.readVInt();
                builder.estimatedBucketCount(negativeBucketCount + positiveBucketCount);
                readBuckets(in, negativeBucketCount, false, builder);
                readBuckets(in, positiveBucketCount, true, builder);
                return create(breaker, builder.build());
            }
        }
    }

    private static void readBuckets(StreamInput in, int bucketCount, boolean positive, ExponentialHistogramBuilder builder)
        throws IOException {
        if (bucketCount > 0) {
            long index = in.readZLong();
            long count = in.readVLong();
            if (positive) {
                builder.setPositiveBucket(index, count);
            } else {
                builder.setNegativeBucket(index, count);
            }
            for (int i = 1; i < bucketCount; i++) {
                long deltaOrCount = in.readZLong();
                if (deltaOrCount < 0) {
                    index += -deltaOrCount;
                    count = in.readZLong();
                } else {
                    index++;
                    count = deltaOrCount;
                }
                if (positive) {
                    builder.setPositiveBucket(index, count);
                } else {
                    builder.setNegativeBucket(index, count);
                }
            }
        }
    }

    private static ZeroBucket readZeroBucket(StreamInput in) throws IOException {
        long count = in.readVLong();
        boolean zeroThresholdIndexBased = in.readBoolean();
        if (zeroThresholdIndexBased) {
            byte scale = in.readByte();
            long index = in.readZLong();
            return ZeroBucket.create(index, scale, count);
        } else {
            double zeroThreshold = in.readDouble();
            return ZeroBucket.create(zeroThreshold, count);
        }
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof ExponentialHistogramState == false) {
            return false;
        }
        ExponentialHistogramState that = (ExponentialHistogramState) obj;
        if (this == that) {
            return true;
        }
        return this.histogram().equals(that.histogram());
    }

    @Override
    public int hashCode() {
        return histogram().hashCode();
    }

    @Override
    public void close() {
        if (closed == false) {
            closed = true;
            circuitBreaker.addWithoutBreaking(-SHALLOW_SIZE);
            if (mergedHistograms != null) {
                Releasables.close(mergedHistograms);
            }
            if (deserializedHistogram != null) {
                Releasables.close(deserializedHistogram);
            }
        }
    }

    @Override
    public long ramBytesUsed() {
        long bytes = SHALLOW_SIZE;
        if (mergedHistograms != null) {
            bytes += mergedHistograms.ramBytesUsed();
        }
        if (deserializedHistogram != null) {
            bytes += deserializedHistogram.ramBytesUsed();
        }
        return bytes;
    }

    private static class ElasticCircuitBreakerWrapper implements ExponentialHistogramCircuitBreaker {

        private final CircuitBreaker breaker;

        ElasticCircuitBreakerWrapper(CircuitBreaker breaker) {
            this.breaker = breaker;
        }

        @Override
        public void adjustBreaker(long bytesAllocated) {
            if (bytesAllocated > 0) {
                breaker.addEstimateBytesAndMaybeBreak(bytesAllocated, "exponential-histogram");
            } else {
                breaker.addWithoutBreaking(bytesAllocated);
            }
        }
    }
}
