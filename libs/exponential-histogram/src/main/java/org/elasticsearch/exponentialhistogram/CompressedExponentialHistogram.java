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

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.RamUsageEstimator;

import java.io.IOException;
import java.io.OutputStream;
import java.util.OptionalLong;

/**
 * Implementation of a {@link ExponentialHistogram} optimized for a minimal memory footprint.
 * <p>
 * While this implementation is optimized for a minimal memory footprint, it is still a fully compliant {@link ExponentialHistogram}
 * and can therefore be directly consumed for merging / quantile estimation without requiring any prior copying or decoding.
 *
 * Note that this histogram implementation stores the zero threshold as double value.
 * For that reason it is lossy if used for storing intermediate merge results, which can have the zero threshold represented
 * as a (scale, index) pair.
 */
public class CompressedExponentialHistogram extends AbstractExponentialHistogram {

    private static final long SHALLOW_SIZE = RamUsageEstimator.shallowSizeOfInstance(CompressedExponentialHistogram.class);

    private double zeroThreshold;
    private long valueCount;
    private double sum;
    private double min;
    private double max;
    private ZeroBucket lazyZeroBucket;

    private final CompressedHistogramData encodedData = new CompressedHistogramData();
    private final Buckets positiveBuckets = new Buckets(true);
    private final Buckets negativeBuckets = new Buckets(false);

    @Override
    public int scale() {
        return encodedData.scale();
    }

    @Override
    public ZeroBucket zeroBucket() {
        if (lazyZeroBucket == null) {
            long zeroCount = valueCount - negativeBuckets.valueCount() - positiveBuckets.valueCount();
            lazyZeroBucket = ZeroBucket.create(zeroThreshold, zeroCount);
        }
        return lazyZeroBucket;
    }

    @Override
    public double sum() {
        return sum;
    }

    @Override
    public long valueCount() {
        return valueCount;
    }

    @Override
    public double min() {
        return min;
    }

    @Override
    public double max() {
        return max;
    }

    @Override
    public ExponentialHistogram.Buckets positiveBuckets() {
        return positiveBuckets;
    }

    @Override
    public ExponentialHistogram.Buckets negativeBuckets() {
        return negativeBuckets;
    }

    /**
     * Resets this instance to decode the provided histogram data.
     *
     * @param zeroThreshold the zeroThreshold for the histogram, which needs to be stored externally
     * @param valueCount the total number of values the histogram contains, needs to be stored externally
     * @param sum the total sum of the values the histogram contains, needs to be stored externally
     * @param min the minimum of the values the histogram contains, needs to be stored externally.
     *            Must be {@link Double#NaN} if the histogram is empty, non-Nan otherwise.
     * @param max the maximum of the values the histogram contains, needs to be stored externally.
     *            Must be {@link Double#NaN} if the histogram is empty, non-Nan otherwise.
     * @param encodedHistogramData the encoded histogram bytes which previously where generated via
     * {@link #writeHistogramBytes(OutputStream, int, BucketIterator, BucketIterator)}.
     */
    public void reset(double zeroThreshold, long valueCount, double sum, double min, double max, BytesRef encodedHistogramData)
        throws IOException {
        lazyZeroBucket = null;
        this.zeroThreshold = zeroThreshold;
        this.valueCount = valueCount;
        this.sum = sum;
        this.min = min;
        this.max = max;
        encodedData.decode(encodedHistogramData);
        negativeBuckets.resetCachedData();
        positiveBuckets.resetCachedData();
    }

    /**
     * Serializes the given histogram, so that exactly the same data can be reconstructed via
     * {@link #reset(double, long, double, double, double, BytesRef)}.
     *
     * @param output the output to write the serialized bytes to
     * @param scale the scale of the histogram
     * @param negativeBuckets the negative buckets of the histogram, sorted by the bucket indices
     * @param positiveBuckets the positive buckets of the histogram, sorted by the bucket indices
     */
    public static void writeHistogramBytes(OutputStream output, int scale, BucketIterator negativeBuckets, BucketIterator positiveBuckets)
        throws IOException {
        CompressedHistogramData.write(output, scale, negativeBuckets, positiveBuckets);
    }

    @Override
    public long ramBytesUsed() {
        return SHALLOW_SIZE + ZeroBucket.SHALLOW_SIZE + 2 * Buckets.SHALLOW_SIZE + CompressedHistogramData.SHALLOW_SIZE;
    }

    private final class Buckets implements ExponentialHistogram.Buckets {

        private static final long SHALLOW_SIZE = RamUsageEstimator.shallowSizeOf(Buckets.class);

        private final boolean isForPositiveBuckets; // false if for negative buckets
        private long cachedValueCount;
        private long cachedMaxIndex;

        private Buckets(boolean isForPositiveBuckets) {
            this.isForPositiveBuckets = isForPositiveBuckets;
            resetCachedData();
        }

        private void resetCachedData() {
            cachedValueCount = -1;
            cachedMaxIndex = Long.MIN_VALUE;
        }

        private void computeCachedDataIfRequired() {
            if (cachedValueCount == -1) {
                cachedValueCount = 0;
                BucketIterator it = iterator();
                while (it.hasNext()) {
                    cachedMaxIndex = it.peekIndex();
                    cachedValueCount += it.peekCount();
                    it.advance();
                }
            }
        }

        @Override
        public CopyableBucketIterator iterator() {
            if (isForPositiveBuckets) {
                return new CompressedBucketsIterator(encodedData.positiveBucketsDecoder());
            } else {
                return new CompressedBucketsIterator(encodedData.negativeBucketsDecoder());
            }
        }

        @Override
        public OptionalLong maxBucketIndex() {
            computeCachedDataIfRequired();
            return cachedValueCount > 0 ? OptionalLong.of(cachedMaxIndex) : OptionalLong.empty();
        }

        @Override
        public long valueCount() {
            computeCachedDataIfRequired();
            return cachedValueCount;
        }

        private class CompressedBucketsIterator implements CopyableBucketIterator {

            private final CompressedHistogramData.BucketsDecoder decoder;

            CompressedBucketsIterator(CompressedHistogramData.BucketsDecoder delegate) {
                this.decoder = delegate;
            }

            @Override
            public CopyableBucketIterator copy() {
                return new CompressedBucketsIterator(decoder.copy());
            }

            @Override
            public final boolean hasNext() {
                return decoder.hasNext();
            }

            @Override
            public final long peekCount() {
                return decoder.peekCount();
            }

            @Override
            public final long peekIndex() {
                return decoder.peekIndex();
            }

            @Override
            public int scale() {
                return CompressedExponentialHistogram.this.scale();
            }

            @Override
            public final void advance() {
                decoder.advance();
            }
        }
    }
}
