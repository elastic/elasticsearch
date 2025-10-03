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

import org.elasticsearch.core.Releasables;

import java.util.TreeMap;

/**
 * A builder for building a {@link ReleasableExponentialHistogram} directly from buckets.
 * Note that this class is not optimized regarding memory allocations or performance, so it is not intended for high-throughput usage.
 */
public class ExponentialHistogramBuilder {

    private final ExponentialHistogramCircuitBreaker breaker;

    private int scale;
    private ZeroBucket zeroBucket = ZeroBucket.minimalEmpty();
    private Double sum;
    private Double min;
    private Double max;

    private final TreeMap<Long, Long> negativeBuckets = new TreeMap<>();
    private final TreeMap<Long, Long> positiveBuckets = new TreeMap<>();

    ExponentialHistogramBuilder(int scale, ExponentialHistogramCircuitBreaker breaker) {
        this.breaker = breaker;
        this.scale = scale;
    }

    ExponentialHistogramBuilder(ExponentialHistogram toCopy, ExponentialHistogramCircuitBreaker breaker) {
        this(toCopy.scale(), breaker);
        zeroBucket(toCopy.zeroBucket());
        sum(toCopy.sum());
        min(toCopy.min());
        max(toCopy.max());
        BucketIterator negBuckets = toCopy.negativeBuckets().iterator();
        while (negBuckets.hasNext()) {
            setNegativeBucket(negBuckets.peekIndex(), negBuckets.peekCount());
            negBuckets.advance();
        }
        BucketIterator posBuckets = toCopy.positiveBuckets().iterator();
        while (posBuckets.hasNext()) {
            setPositiveBucket(posBuckets.peekIndex(), posBuckets.peekCount());
            posBuckets.advance();
        }
    }

    public ExponentialHistogramBuilder scale(int scale) {
        this.scale = scale;
        return this;
    }

    public ExponentialHistogramBuilder zeroBucket(ZeroBucket zeroBucket) {
        this.zeroBucket = zeroBucket;
        return this;
    }

    /**
     * Sets the sum of the histogram values. If not set, the sum will be estimated from the buckets.
     * @param sum the sum value
     * @return the builder
     */
    public ExponentialHistogramBuilder sum(double sum) {
        this.sum = sum;
        return this;
    }

    /**
     * Sets the min value of the histogram values. If not set, the min will be estimated from the buckets.
     * @param min the min value
     * @return the builder
     */
    public ExponentialHistogramBuilder min(double min) {
        this.min = min;
        return this;
    }

    /**
     * Sets the max value of the histogram values. If not set, the max will be estimated from the buckets.
     * @param max the max value
     * @return the builder
     */
    public ExponentialHistogramBuilder max(double max) {
        this.max = max;
        return this;
    }

    /**
     * Sets the given bucket of the positive buckets.
     * Buckets may be set in arbitrary order. If the bucket already exists, it will be replaced.
     *
     * @param index the index of the bucket
     * @param count the count of the bucket, must be at least 1
     * @return the builder
     */
    public ExponentialHistogramBuilder setPositiveBucket(long index, long count) {
        if (count < 1) {
            throw new IllegalArgumentException("Bucket count must be at least 1");
        }
        positiveBuckets.put(index, count);
        return this;
    }

    /**
     * Sets the given bucket of the negative buckets.
     * Buckets may be set in arbitrary order. If the bucket already exists, it will be replaced.
     *
     * @param index the index of the bucket
     * @param count the count of the bucket, must be at least 1
     * @return the builder
     */
    public ExponentialHistogramBuilder setNegativeBucket(long index, long count) {
        if (count < 1) {
            throw new IllegalArgumentException("Bucket count must be at least 1");
        }
        negativeBuckets.put(index, count);
        return this;
    }

    public ReleasableExponentialHistogram build() {
        FixedCapacityExponentialHistogram result = FixedCapacityExponentialHistogram.create(
            negativeBuckets.size() + positiveBuckets.size(),
            breaker
        );
        boolean success = false;
        try {
            result.resetBuckets(scale);
            result.setZeroBucket(zeroBucket);
            negativeBuckets.forEach((index, count) -> result.tryAddBucket(index, count, false));
            positiveBuckets.forEach((index, count) -> result.tryAddBucket(index, count, true));

            double sumVal = (sum != null)
                ? sum
                : ExponentialHistogramUtils.estimateSum(result.negativeBuckets().iterator(), result.positiveBuckets().iterator());
            double minVal = (min != null)
                ? min
                : ExponentialHistogramUtils.estimateMin(zeroBucket, result.negativeBuckets(), result.positiveBuckets()).orElse(Double.NaN);
            double maxVal = (max != null)
                ? max
                : ExponentialHistogramUtils.estimateMax(zeroBucket, result.negativeBuckets(), result.positiveBuckets()).orElse(Double.NaN);

            result.setMin(minVal);
            result.setMax(maxVal);
            result.setSum(sumVal);

            success = true;
        } finally {
            if (success == false) {
                Releasables.close(result);
            }
        }
        return result;
    }
}
