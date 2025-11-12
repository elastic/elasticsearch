/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.oteldata.otlp.datapoint;

import io.opentelemetry.proto.metrics.v1.ExponentialHistogramDataPoint;
import io.opentelemetry.proto.metrics.v1.HistogramDataPoint;

import org.elasticsearch.exponentialhistogram.ExponentialScaleUtils;

/**
 * Utility class to convert OpenTelemetry histogram data points into counts and centroid values
 * so that we can use it with the {@code histogram} field type.
 * This class provides methods to extract counts and centroid values from both
 * {@link ExponentialHistogramDataPoint} and {@link HistogramDataPoint}.
 * The algorithm is ported over from the OpenTelemetry collector's Elasticsearch exporter.
 * @see <a href="https://github.com/open-telemetry/opentelemetry-collector-contrib/tree/v0.132.0/exporter/elasticsearchexporter">
 * Elasticsearch exporter on GitHub
 * </a>
 */
class TDigestConverter {

    static <E extends Exception> void counts(ExponentialHistogramDataPoint dp, CheckedLongConsumer<E> counts) throws E {
        ExponentialHistogramDataPoint.Buckets negative = dp.getNegative();

        for (int i = negative.getBucketCountsCount() - 1; i >= 0; i--) {
            long count = negative.getBucketCounts(i);
            if (count != 0) {
                counts.accept(count);
            }
        }

        long zeroCount = dp.getZeroCount();
        if (zeroCount > 0) {
            counts.accept(zeroCount);
        }

        ExponentialHistogramDataPoint.Buckets positive = dp.getPositive();
        for (int i = 0; i < positive.getBucketCountsCount(); i++) {
            long count = positive.getBucketCounts(i);
            if (count != 0) {
                counts.accept(count);
            }
        }
    }

    /**
     * @see <a
     * href="https://github.com/open-telemetry/opentelemetry-collector-contrib/blob/v0.132.0/exporter/elasticsearchexporter/internal/exphistogram/exphistogram.go">
     * <code>ToTDigest</code> function
     * </a>
     */
    static <E extends Exception> void centroidValues(ExponentialHistogramDataPoint dp, CheckedDoubleConsumer<E> values) throws E {
        int scale = dp.getScale();
        ExponentialHistogramDataPoint.Buckets negative = dp.getNegative();

        int offset = negative.getOffset();
        for (int i = negative.getBucketCountsCount() - 1; i >= 0; i--) {
            long count = negative.getBucketCounts(i);
            if (count != 0) {
                double lb = -ExponentialScaleUtils.getUpperBucketBoundary(offset + i, scale);
                double ub = -ExponentialScaleUtils.getLowerBucketBoundary(offset + i, scale);
                values.accept(lb + (ub - lb) / 2);
            }
        }

        long zeroCount = dp.getZeroCount();
        if (zeroCount > 0) {
            values.accept(0.0);
        }

        ExponentialHistogramDataPoint.Buckets positive = dp.getPositive();
        offset = positive.getOffset();
        for (int i = 0; i < positive.getBucketCountsCount(); i++) {
            long count = positive.getBucketCounts(i);
            if (count != 0) {
                double lb = ExponentialScaleUtils.getLowerBucketBoundary(offset + i, scale);
                double ub = ExponentialScaleUtils.getUpperBucketBoundary(offset + i, scale);
                values.accept(lb + (ub - lb) / 2);
            }
        }
    }

    static <E extends Exception> void counts(HistogramDataPoint dp, CheckedLongConsumer<E> counts) throws E {
        for (int i = 0; i < dp.getBucketCountsCount(); i++) {
            long count = dp.getBucketCounts(i);
            if (count != 0) {
                counts.accept(count);
            }
        }
    }

    /**
     * @see <a
     * href="https://github.com/open-telemetry/opentelemetry-collector-contrib/tree/v0.132.0/exporter/elasticsearchexporter/internal/datapoints/histogram.go">
     * <code>histogramToValue</code> function
     * </a>
     */
    static <E extends Exception> void centroidValues(HistogramDataPoint dp, CheckedDoubleConsumer<E> values) throws E {
        int size = dp.getBucketCountsCount();
        for (int i = 0; i < size; i++) {
            long count = dp.getBucketCounts(i);
            if (count != 0) {
                double value;
                value = getCentroid(dp, i);
                values.accept(value);
            }
        }
    }

    public static double getCentroid(HistogramDataPoint dp, int bucketIndex) {
        double value;
        if (bucketIndex == 0) {
            // (-infinity, explicit_bounds[i]]
            value = dp.getExplicitBounds(bucketIndex);
            if (value > 0) {
                value /= 2;
            }
        } else if (bucketIndex == dp.getBucketCountsCount() - 1) {
            // (explicit_bounds[i], +infinity)
            value = dp.getExplicitBounds(bucketIndex - 1);
        } else {
            // [explicit_bounds[i-1], explicit_bounds[i])
            // Use the midpoint between the boundaries.
            value = dp.getExplicitBounds(bucketIndex - 1) + (dp.getExplicitBounds(bucketIndex) - dp.getExplicitBounds(bucketIndex - 1))
                / 2.0;
        }
        return value;
    }

    interface CheckedLongConsumer<E extends Exception> {
        void accept(long value) throws E;
    }

    interface CheckedDoubleConsumer<E extends Exception> {
        void accept(double value) throws E;
    }
}
