/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.oteldata.otlp.datapoint;

import io.opentelemetry.proto.metrics.v1.AggregationTemporality;
import io.opentelemetry.proto.metrics.v1.ExponentialHistogramDataPoint;
import io.opentelemetry.proto.metrics.v1.HistogramDataPoint;

import org.elasticsearch.exponentialhistogram.ExponentialScaleUtils;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;

import static org.elasticsearch.exponentialhistogram.ExponentialHistogram.MAX_SCALE;
import static org.elasticsearch.exponentialhistogram.ExponentialHistogramXContent.BUCKET_COUNTS_FIELD;
import static org.elasticsearch.exponentialhistogram.ExponentialHistogramXContent.BUCKET_INDICES_FIELD;
import static org.elasticsearch.exponentialhistogram.ExponentialHistogramXContent.MAX_FIELD;
import static org.elasticsearch.exponentialhistogram.ExponentialHistogramXContent.MIN_FIELD;
import static org.elasticsearch.exponentialhistogram.ExponentialHistogramXContent.NEGATIVE_FIELD;
import static org.elasticsearch.exponentialhistogram.ExponentialHistogramXContent.POSITIVE_FIELD;
import static org.elasticsearch.exponentialhistogram.ExponentialHistogramXContent.SCALE_FIELD;
import static org.elasticsearch.exponentialhistogram.ExponentialHistogramXContent.SUM_FIELD;
import static org.elasticsearch.exponentialhistogram.ExponentialHistogramXContent.ZERO_COUNT_FIELD;
import static org.elasticsearch.exponentialhistogram.ExponentialHistogramXContent.ZERO_FIELD;
import static org.elasticsearch.exponentialhistogram.ExponentialHistogramXContent.ZERO_THRESHOLD_FIELD;

/**
 * Utility class to convert OpenTelemetry histogram data points into exponential histograms
 * so that we can use it with the {@code exponential_histogram} field type.
 *
 * OTLP exponential histograms will be left unchanged and just formatted into the XContent that the field type expects.
 * OTLP explicit bucket histograms will be converted to exponential histograms at maximum scale,
 * with the bucket centers corresponding to the centroids which are computed by {@link TDigestConverter}.
 * For the details see {@link #buildExponentialHistogram(HistogramDataPoint, AggregationTemporality, XContentBuilder, BucketBuffer)}.
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
            builder.startObject(ZERO_FIELD).field(ZERO_COUNT_FIELD, dataPoint.getZeroCount());
            if (dataPoint.getZeroThreshold() != 0) {
                builder.field(ZERO_THRESHOLD_FIELD, dataPoint.getZeroThreshold());
            }
            builder.endObject();
        }
        if (dataPoint.hasNegative()) {
            writeExponentialBuckets(builder, NEGATIVE_FIELD, dataPoint.getNegative());
        }
        if (dataPoint.hasPositive()) {
            writeExponentialBuckets(builder, POSITIVE_FIELD, dataPoint.getPositive());
        }
        if (dataPoint.hasSum()) {
            builder.field(SUM_FIELD, dataPoint.getSum());
        }
        if (dataPoint.hasMin()) {
            builder.field(MIN_FIELD, dataPoint.getMin());
        }
        if (dataPoint.hasMax()) {
            builder.field(MAX_FIELD, dataPoint.getMax());
        }
        builder.endObject();
    }

    private static void writeExponentialBuckets(XContentBuilder builder, String fieldName, ExponentialHistogramDataPoint.Buckets buckets)
        throws IOException {
        builder.startObject(fieldName);
        builder.startArray(BUCKET_INDICES_FIELD);
        for (int i = 0; i < buckets.getBucketCountsCount(); i++) {
            long count = buckets.getBucketCounts(i);
            if (count != 0) {
                builder.value(buckets.getOffset() + i);
            }
        }
        builder.endArray();
        builder.startArray(BUCKET_COUNTS_FIELD);
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
     * Due to the maximum scale, the buckets will be very narrow and will always
     * yield almost exactly the centroids for percentile estimation.
     * <br>
     * In addition, we preserve the min/max if provided. To make sure that the min/max are actually part of a bucket, we do the following:
     * <ul>
     *   <li>If the min is smaller than the centroid of the first bucket with data, we add a separate single-value bucket for it
     *   and reduce the count of the original first bucket by one (removing it, if it becomes empty)</li>
     *   <li>If the min is larger than the centroid of the first bucket with data, we clamp the centroid to the min</li>
     * </ul>
     * And we do the same thing for max with the last populated bucket.
     *
     * @param dataPoint the point to write
     * @param builder the builder to write to
     * @param temporality the temporality of the histogram
     * @param bucketsScratch reusable, temporary memory used to build the output histogram
     */
    public static void buildExponentialHistogram(
        HistogramDataPoint dataPoint,
        AggregationTemporality temporality,
        XContentBuilder builder,
        BucketBuffer bucketsScratch
    ) throws IOException {
        builder.startObject().field(SCALE_FIELD, MAX_SCALE);

        int size = dataPoint.getBucketCountsCount();

        if (size > 0 && dataPoint.getExplicitBoundsCount() > 0) {
            bucketsScratch.clear();

            boolean minHandled = false;

            int lastPopulatedBucket = size - 1;
            while (lastPopulatedBucket >= 0 && dataPoint.getBucketCounts(lastPopulatedBucket) == 0) {
                lastPopulatedBucket--;
            }

            for (int i = 0; i < size; i++) {
                long count = dataPoint.getBucketCounts(i);
                if (count > 0) {
                    double centroid = TDigestConverter.getCentroid(dataPoint, i);
                    boolean injectMaxBucketAfterIteration = false;
                    // for delta histograms, we extract the min and the max into separate buckets with count 1 to improve the accuracy
                    // we can't do this for cumulative temporality, as cumulative temporality needs stable buckets
                    if (temporality != AggregationTemporality.AGGREGATION_TEMPORALITY_CUMULATIVE) {
                        if (dataPoint.hasMin() && minHandled == false) {
                            if (centroid > dataPoint.getMin() && count > 1) {
                                // min is smaller than the centroid and not the only value in the bucket, we inject a separate, single value
                                // bucket for it
                                bucketsScratch.append(dataPoint.getMin(), 1);
                                count -= 1;
                            } else {
                                // clamp the bucket centroid to min
                                centroid = dataPoint.getMin();
                            }
                            minHandled = true;
                        }
                        if (dataPoint.hasMax() && lastPopulatedBucket == i) {
                            if (centroid < dataPoint.getMax() && count > 1) {
                                // max is bigger than the centroid and not the only value in the bucket, we inject a separate, single value
                                // bucket for it
                                injectMaxBucketAfterIteration = true;
                                count -= 1;
                            } else {
                                // clamp the bucket centroid to min
                                centroid = dataPoint.getMax();
                            }
                        }
                    }
                    bucketsScratch.append(centroid, count);
                    if (injectMaxBucketAfterIteration) {
                        bucketsScratch.append(dataPoint.getMax(), 1);
                    }
                }
            }
            bucketsScratch.writeBuckets(builder);
            writeSummaryStatistics(dataPoint, builder);
        } else if (dataPoint.getCount() > 0) {
            bucketsScratch.clear();
            double value = 0.0;
            if (dataPoint.hasSum() && temporality != AggregationTemporality.AGGREGATION_TEMPORALITY_CUMULATIVE) {
                // we can't use the average as singular bucket for cumulative histograms because we need stable buckets across histograms
                value = dataPoint.getSum() / dataPoint.getCount();
            }
            bucketsScratch.append(value, dataPoint.getCount());
            bucketsScratch.writeBuckets(builder);
            writeSummaryStatistics(dataPoint, builder);
        }
        builder.endObject();
    }

    private static void writeSummaryStatistics(HistogramDataPoint dataPoint, XContentBuilder builder) throws IOException {
        if (dataPoint.hasSum()) {
            builder.field(SUM_FIELD, dataPoint.getSum());
        }
        if (dataPoint.hasMin()) {
            builder.field(MIN_FIELD, dataPoint.getMin());
        }
        if (dataPoint.hasMax()) {
            builder.field(MAX_FIELD, dataPoint.getMax());
        }
    }

    private static class IndexWithCountList {

        private static final int INITIAL_CAPACITY = 32;

        private long[] indices = new long[INITIAL_CAPACITY];
        private long[] counts = new long[INITIAL_CAPACITY];
        private int size = 0;

        public void add(long index, long count) {
            if (size == indices.length) {
                doubleCapacity();
            }
            indices[size] = index;
            counts[size] = count;
            size++;
        }

        public void setCount(int position, long count) {
            assert position < size;
            counts[position] = count;
        }

        public int size() {
            return size;
        }

        public long getIndex(int position) {
            assert position < size;
            return indices[position];
        }

        public long getCount(int position) {
            assert position < size;
            return counts[position];
        }

        public void clear() {
            size = 0;
        }

        private void doubleCapacity() {
            long[] newIndices = new long[indices.length * 2];
            long[] newCounts = new long[counts.length * 2];
            System.arraycopy(indices, 0, newIndices, 0, size);
            System.arraycopy(counts, 0, newCounts, 0, size);
            indices = newIndices;
            counts = newCounts;
        }
    }

    public static class BucketBuffer {
        // negative indices are sorted from highest to lowest (smallest bucket center to largest bucket center)
        private final IndexWithCountList negativeBuckets = new IndexWithCountList();

        // positive indices are sorted from lowest to highest (smallest bucket center to largest bucket center)
        private final IndexWithCountList positiveBuckets = new IndexWithCountList();

        long zeroCount = 0;

        private void clear() {
            negativeBuckets.clear();
            positiveBuckets.clear();
            zeroCount = 0;
        }

        void append(double center, long count) {
            if (count == 0) {
                return;
            }
            if (center < 0) {
                addOrMergeBucket(center, count, negativeBuckets);
            } else if (center > 0) {
                addOrMergeBucket(center, count, positiveBuckets);
            } else {
                zeroCount += count;
            }
        }

        private static void addOrMergeBucket(double center, long count, IndexWithCountList buckets) {
            long index = ExponentialScaleUtils.computeIndex(center, MAX_SCALE);
            if (buckets.size() > 0 && buckets.getIndex(buckets.size() - 1) == index) {
                // merge with previous
                int lastPos = buckets.size() - 1;
                buckets.setCount(lastPos, buckets.getCount(lastPos) + count);
            } else {
                buckets.add(index, count);
            }
        }

        private void writeBuckets(XContentBuilder builder) throws IOException {
            if (zeroCount > 0) {
                builder.startObject(ZERO_FIELD).field(ZERO_COUNT_FIELD, zeroCount).endObject();
            }
            if (negativeBuckets.size() > 0) {
                // write in inverse order to get lowest to highest index
                builder.startObject(NEGATIVE_FIELD);
                builder.startArray(BUCKET_INDICES_FIELD);
                for (int i = negativeBuckets.size() - 1; i >= 0; i--) {
                    builder.value(negativeBuckets.getIndex(i));
                }
                builder.endArray();
                builder.startArray(BUCKET_COUNTS_FIELD);
                for (int i = negativeBuckets.size() - 1; i >= 0; i--) {
                    builder.value(negativeBuckets.getCount(i));
                }
                builder.endArray();
                builder.endObject();
            }
            if (positiveBuckets.size() > 0) {
                builder.startObject(POSITIVE_FIELD);
                builder.startArray(BUCKET_INDICES_FIELD);
                for (int i = 0; i < positiveBuckets.size(); i++) {
                    builder.value(positiveBuckets.getIndex(i));
                }
                builder.endArray();
                builder.startArray(BUCKET_COUNTS_FIELD);
                for (int i = 0; i < positiveBuckets.size(); i++) {
                    builder.value(positiveBuckets.getCount(i));
                }
                builder.endArray();
                builder.endObject();
            }
        }
    }
}
