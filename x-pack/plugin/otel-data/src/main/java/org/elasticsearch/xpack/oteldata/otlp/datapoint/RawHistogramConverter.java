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
 * Converts OpenTelemetry histograms into Elasticsearch {@code histogram} values without midpoint approximation.
 */
class RawHistogramConverter {

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

    static <E extends Exception> void values(ExponentialHistogramDataPoint dp, CheckedDoubleConsumer<E> values) throws E {
        int scale = dp.getScale();
        ExponentialHistogramDataPoint.Buckets negative = dp.getNegative();

        int offset = negative.getOffset();
        for (int i = negative.getBucketCountsCount() - 1; i >= 0; i--) {
            long count = negative.getBucketCounts(i);
            if (count != 0) {
                values.accept(-ExponentialScaleUtils.getLowerBucketBoundary(offset + i, scale));
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
                values.accept(ExponentialScaleUtils.getUpperBucketBoundary(offset + i, scale));
            }
        }
    }

    static <E extends Exception> void counts(HistogramDataPoint dp, CheckedLongConsumer<E> counts) throws E {
        int boundsCount = dp.getExplicitBoundsCount();
        if (boundsCount == 0) {
            long count = dp.getCount();
            if (count > 0) {
                counts.accept(count);
            }
            return;
        }

        boolean hasPendingCount = false;
        long pendingCount = 0;
        for (int i = 0; i < dp.getBucketCountsCount(); i++) {
            long count = dp.getBucketCounts(i);
            if (count == 0) {
                continue;
            }
            if (i == boundsCount) {
                if (hasPendingCount) {
                    pendingCount += count;
                }
                break;
            }
            if (hasPendingCount) {
                counts.accept(pendingCount);
            }
            pendingCount = count;
            hasPendingCount = true;
        }
        if (hasPendingCount) {
            counts.accept(pendingCount);
        }
    }

    static <E extends Exception> void values(HistogramDataPoint dp, CheckedDoubleConsumer<E> values) throws E {
        int boundsCount = dp.getExplicitBoundsCount();
        if (boundsCount == 0) {
            long count = dp.getCount();
            if (count > 0) {
                values.accept(dp.getSum() / count);
            }
            return;
        }
        for (int i = 0; i < dp.getBucketCountsCount(); i++) {
            long count = dp.getBucketCounts(i);
            if (count == 0) {
                continue;
            }
            if (i == boundsCount) {
                break;
            }
            values.accept(dp.getExplicitBounds(i));
        }
    }

    interface CheckedLongConsumer<E extends Exception> {
        void accept(long value) throws E;
    }

    interface CheckedDoubleConsumer<E extends Exception> {
        void accept(double value) throws E;
    }
}
