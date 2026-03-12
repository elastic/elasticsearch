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

import java.util.List;

import static org.elasticsearch.exponentialhistogram.ExponentialHistogram.MAX_SCALE;

public class TDigestToExponentialHistogramConverter {

    /**
     * An iterator iterating over T-Digest centroids.
     * The centroids are not required to be unique, but must be sorted in ascending order by the {@link #value()}.
     */
    public interface CentroidIterator {
        /**
         * @return true if there is another centroid to process
         */
        boolean hasNext();

        /**
         * Must only be called if {@link #hasNext()} returned true.
         * @return the current centroid's value
         */
        double value();

        /**
         * Must only be called if {@link #hasNext()} returned true.
         * @return the current centroid's count
         */
        long count();

        /**
         * Advance to the next centroid.
         */
        void advance();

        /**
         * Creates a copy of this iterator that iterates over the centroids this iterator already skipped via {@link #advance()},
         * but in reverse order.
         */
        CentroidIterator reversedCopy();
    }

    public static class ArrayBasedCentroidIterator implements CentroidIterator {
        private final List<Double> centroids;
        private final List<Long> counts;
        private int index = 0;
        private int direction = 1;

        public ArrayBasedCentroidIterator(List<Double> centroids, List<Long> counts) {
            this.centroids = centroids;
            this.counts = counts;
        }

        @Override
        public boolean hasNext() {
            return index < centroids.size() && index >= 0;
        }

        @Override
        public double value() {
            return centroids.get(index);
        }

        @Override
        public long count() {
            return counts.get(index);
        }

        @Override
        public void advance() {
            index += direction;
        }

        @Override
        public CentroidIterator reversedCopy() {
            var result = new ArrayBasedCentroidIterator(centroids, counts);
            result.index = this.index - direction;
            result.direction = -this.direction;
            return result;
        }
    }

    public interface LazyConversion {
        /**
         * See {@link ZeroBucket#zeroThreshold()}.
         * @return the zero threshold of the converted histogram
         */
        double getZeroThreshold();

        /**
         * See {@link ZeroBucket#count()}.
         * @return the zero count of the converted histogram
         */
        long getZeroCount();

        /**
         * See {@link ExponentialHistogram#scale()}.
         * @return the scale of the converted histogram
         */
        int getScale();

        /**
         * The negative buckets of the output histogram.
         * May only be called once.
         * @return the negative buckets of the converted histogram
         */
        BucketIterator negativeBuckets();

        /**
         * The positive buckets of the output histogram.
         * May only be called once.
         * @return the positive buckets of the converted histogram
         */
        BucketIterator positiveBuckets();

    }

    /**
     * Convert the given centroids to an exponential histogram.
     * Sum, min, and max will be estimated.
     * @param centroids the T-Digest centroids to convert
     * @return the resulting exponential histogram
     */
    public static ReleasableExponentialHistogram convert(CentroidIterator centroids, ExponentialHistogramCircuitBreaker breaker) {
        LazyConversion conversion = convertLazy(centroids);
        ExponentialHistogramBuilder builder = ExponentialHistogram.builder(conversion.getScale(), breaker);
        builder.zeroBucket(ZeroBucket.create(conversion.getZeroThreshold(), conversion.getZeroCount()));

        BucketIterator negativeBuckets = conversion.negativeBuckets();
        while (negativeBuckets.hasNext()) {
            builder.setNegativeBucket(negativeBuckets.peekIndex(), negativeBuckets.peekCount());
            negativeBuckets.advance();
        }

        BucketIterator positiveBuckets = conversion.positiveBuckets();
        while (positiveBuckets.hasNext()) {
            builder.setPositiveBucket(positiveBuckets.peekIndex(), positiveBuckets.peekCount());
            positiveBuckets.advance();
        }
        return builder.build();
    }

    public static LazyConversion convertLazy(CentroidIterator centroids) {

        // skip negative centroids
        while (centroids.hasNext() && centroids.value() < 0) {
            centroids.advance();
        }
        CentroidIterator negativeCentroids = centroids.reversedCopy();

        long zeroCount = 0;
        while (centroids.hasNext() && centroids.value() == 0.0) {
            // we have a zero-centroid, which we'll map to the zero bucket
            zeroCount += centroids.count();
            centroids.advance();
        }

        long finalZeroCount = zeroCount;
        CentroidIterator positiveCentroids = centroids;

        return new LazyConversion() {
            @Override
            public double getZeroThreshold() {
                // Currently always 0.0, but let's keep that an implementation detail
                return 0.0;
            }

            @Override
            public long getZeroCount() {
                return finalZeroCount;
            }

            @Override
            public int getScale() {
                // Always MAX_SCALE right now, but let's keep that an implementation detail
                return MAX_SCALE;
            }

            boolean negativeBucketsConsumed = false;

            @Override
            public BucketIterator negativeBuckets() {
                if (negativeBucketsConsumed) {
                    throw new IllegalStateException("negativeBuckets() may only be called once");
                }
                negativeBucketsConsumed = true;
                return new CentroidToBucketsConvertingIterator(negativeCentroids, getScale());
            }

            boolean positiveBucketsConsumed = false;

            @Override
            public BucketIterator positiveBuckets() {
                if (positiveBucketsConsumed) {
                    throw new IllegalStateException("positiveBuckets() may only be called once");
                }
                positiveBucketsConsumed = true;
                return new CentroidToBucketsConvertingIterator(positiveCentroids, getScale());
            }
        };
    }

    private static class CentroidToBucketsConvertingIterator implements BucketIterator {

        private final int scale;

        private final CentroidIterator centroids;
        private long currentIndex;
        private long currentCount;
        private boolean hasNext;

        CentroidToBucketsConvertingIterator(CentroidIterator centroids, int scale) {
            this.centroids = centroids;
            this.scale = scale;
            advanceToNextBucket();
        }

        private void advanceToNextBucket() {
            currentCount = 0;
            currentIndex = 0;
            hasNext = false;
            while (centroids.hasNext()) {
                long countForCurrentCentroid = centroids.count();
                if (countForCurrentCentroid > 0) {

                    double centroidValue = centroids.value();
                    assert centroidValue != 0.0 : "zero centroids should have been handled separately";

                    long expBucketIndex = ExponentialScaleUtils.computeIndex(centroidValue, scale);
                    if (currentCount == 0) {
                        // this is the first centroid we're processing
                        hasNext = true;
                        currentCount = countForCurrentCentroid;
                        currentIndex = expBucketIndex;
                    } else {
                        // abort processing if the centroid maps to a different bucket, otherwise combine them
                        if (currentIndex == expBucketIndex) {
                            currentCount += countForCurrentCentroid;
                        } else {
                            return; // centroid falls into the next bucket
                        }
                    }
                    centroids.advance();
                } else {
                    // ignore zero-count centroids
                    centroids.advance();
                }
            }
        }

        @Override
        public boolean hasNext() {
            return hasNext;
        }

        @Override
        public long peekCount() {
            return currentCount;
        }

        @Override
        public long peekIndex() {
            return currentIndex;
        }

        @Override
        public void advance() {
            advanceToNextBucket();
        }

        @Override
        public int scale() {
            return scale;
        }
    }

}
