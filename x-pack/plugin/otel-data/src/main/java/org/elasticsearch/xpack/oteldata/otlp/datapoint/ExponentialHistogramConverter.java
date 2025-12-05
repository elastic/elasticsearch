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
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import static org.elasticsearch.exponentialhistogram.ExponentialHistogram.MAX_SCALE;

/**
 * Utility class to convert OpenTelemetry histogram data points into exponential histograms
 * so that we can use it with the {@code exponential_histogram} field type.
 *
 * OTLP exponential histograms will be left unchanged and just formatted into the XContent that the field type expects.
 * OTLP explicit bucket histograms will be converted to exponential histograms at maximum scale,
 * with the bucket centers corresponding to the centroids which are computed by {@link TDigestConverter}.
 */
public class ExponentialHistogramConverter {

    /**
     * Writes the provided OTLP exponential histogram as elasticsearch exponential_histogram field value.
     * @param dataPoint the point to write
     * @param builder the builder to write to
     */
    public static void buildExponentialHistogram(ExponentialHistogramDataPoint dataPoint, XContentBuilder builder) throws IOException {
        builder.startObject();
        builder.field("scale", dataPoint.getScale());
        if (dataPoint.getZeroCount() > 0) {
            builder.startObject("zero")
                .field("count", dataPoint.getZeroCount());
            if (dataPoint.getZeroThreshold() != 0) {
                builder.field("threshold", dataPoint.getZeroThreshold());
            }
            builder.endObject();
        }
        if (dataPoint.hasNegative()) {
            writeExponentialBuckets(builder, "negative", dataPoint.getNegative());
        }
        if (dataPoint.hasPositive()) {
            writeExponentialBuckets(builder, "positive", dataPoint.getPositive());
        }
        if (dataPoint.hasSum()) {
            builder.field("sum", dataPoint.getSum());
        }
        if (dataPoint.hasMin()) {
            builder.field("min", dataPoint.getMin());
        }
        if (dataPoint.hasMax()) {
            builder.field("max", dataPoint.getMax());
        }
        builder.endObject();
    }

    private static void writeExponentialBuckets(XContentBuilder builder, String fieldName, ExponentialHistogramDataPoint.Buckets buckets)
        throws IOException {
        builder.startObject(fieldName);
        builder.startArray("indices");
        for (int i = 0; i < buckets.getBucketCountsCount(); i++) {
            long count = buckets.getBucketCounts(i);
            if (count != 0) {
                builder.value(buckets.getOffset() + i);
            }
        }
        builder.endArray();
        builder.startArray("counts");
        for (int i = 0; i < buckets.getBucketCountsCount(); i++) {
            long count = buckets.getBucketCounts(i);
            if (count != 0) {
                builder.value(count);
            }
        }
        builder.endArray();
        builder.endObject();
    }

    /**
     * Writes the provided OTLP explicit bucket histogram as elasticsearch exponential_histogram field value.
     * First, the buckets will be converted to the same centroids as computed by {@link TDigestConverter},
     * then we will use an exponential histogram with maximum scale with one bucket per centroid.
     * Due to the maximum scale, the buckets will be very narrow and will always yield almost exactly the centroids for percentile estimation.
     *
     * @param dataPoint the point to write
     * @param builder the builder to write to
     */
    public static void buildExponentialHistogram(HistogramDataPoint dataPoint, XContentBuilder builder) throws IOException {
        builder.startObject().field("scale", MAX_SCALE);

        int size = dataPoint.getBucketCountsCount();
        if (size > 0) {
            int negativeBucketCount = 0;
            while (negativeBucketCount < size && TDigestConverter.getCentroid(dataPoint, negativeBucketCount) < 0) {
                negativeBucketCount++;
            }
            int zeroBucketCount = 0;
            while (negativeBucketCount + zeroBucketCount < size
                && TDigestConverter.getCentroid(dataPoint, negativeBucketCount + zeroBucketCount) == 0) {
                zeroBucketCount++;
            }
            int positiveBucketCount = size - negativeBucketCount - zeroBucketCount;

            if (zeroBucketCount > 0) {
                long zeroCount = 0;
                for (int i = negativeBucketCount; i < negativeBucketCount + zeroBucketCount; i++) {
                    zeroCount += dataPoint.getBucketCounts(i);
                }
                builder.startObject("zero").field("count", zeroCount).endObject();
            }
            if (negativeBucketCount > 0) {
                writeFixedBucketsAtMaxScale(builder, dataPoint, 0, negativeBucketCount, false);
            }
            if (positiveBucketCount > 0) {
                writeFixedBucketsAtMaxScale(builder, dataPoint, negativeBucketCount + zeroBucketCount, positiveBucketCount, true);
            }
            if (dataPoint.hasSum()) {
                builder.field("sum", dataPoint.getSum());
            }
            if (dataPoint.hasMin()) {
                builder.field("min", dataPoint.getMin());
            }
            if (dataPoint.hasMax()) {
                builder.field("max", dataPoint.getMax());
            }
        }
        builder.endObject();
    }

    private static void writeFixedBucketsAtMaxScale(
        XContentBuilder builder,
        HistogramDataPoint dataPoint,
        int startIndex,
        int bucketCount,
        boolean isPositive
    ) throws IOException {
        // mergedIndices is only for the edge case that two buckets collapse into the same bucket,
        // can only happen usually due to floating point precision or malformed data
        Set<Integer> mergedIndices = null;
        builder.startObject(isPositive ? "positive" : "negative");

        int start, limit, step;
        if (isPositive) {
            start = startIndex;
            limit = startIndex + bucketCount;
            step = 1;
        } else {
            start = startIndex + bucketCount - 1;
            limit = startIndex - 1;
            step = -1;
        }

        long prevExpIndex = Long.MIN_VALUE;
        int prevBucketIndex = -1;

        builder.startArray("indices");
        for (int i = start; i != limit; i += step) {
            if (dataPoint.getBucketCounts(i) > 0) {
                long index = ExponentialScaleUtils.computeIndex(TDigestConverter.getCentroid(dataPoint, i), MAX_SCALE);
                if (prevExpIndex != index) {
                    assert index > prevExpIndex;
                    builder.value(index);
                } else {
                    // edge case, shouldn't happen for well-formed inputs
                    if (mergedIndices == null) {
                        mergedIndices = new HashSet<>();
                    }
                    mergedIndices.add(prevBucketIndex);
                }
                prevExpIndex = index;
                prevBucketIndex = i;
            }
        }
        builder.endArray();

        builder.startArray("counts");
        long mergedCounts = 0;
        for (int i = start; i != limit; i += step) {
            mergedCounts += dataPoint.getBucketCounts(i);
            if (mergedIndices == null || mergedIndices.contains(i) == false) {
                builder.value(mergedCounts);
                mergedCounts = 0;
            }
        }
        builder.endArray();
    }

}
