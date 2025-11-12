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
            builder.startObject("zero").field("count", dataPoint.getZeroCount());
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
     * Due to the maximum scale, the buckets will be very narrow and will always
     * yield almost exactly the centroids for percentile estimation.
     * <br>
     * In addition, we preserve the min/max if provided. To make sure that the min/max are actually part of a bucket, we do the following:
     * <ul>
     *   <li>If the min is smaller than the centroid of the first bucket with data, we add a separate single-value bucket for it</li>
     *   <li>If the min is larger than the centroid of the first bucket with data, we clamp the centroid to the min</li>
     * </ul>
     * And we do the same thing for max with the last populated bucket.
     *
     * @param dataPoint the point to write
     * @param builder the builder to write to
     * @param bucketsScratch reusable, temporary memory used to build the output histogram
     */
    public static void buildExponentialHistogram(HistogramDataPoint dataPoint, XContentBuilder builder, BucketBuffer bucketsScratch) throws IOException {
        builder.startObject().field("scale", MAX_SCALE);

        // TODO: When start supporting cumulative buckets, we can't do the synthetic min/max bucket trick anymore.
        // Then we probably need to just drop min/max, which aren't really useful for cumulative histograms anyway

        int size = dataPoint.getBucketCountsCount();

        if (size > 0) {
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
                    boolean injectMaxBucketAfterIteration = false;
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
                    bucketsScratch.append(centroid, count);
                    if (injectMaxBucketAfterIteration) {
                        bucketsScratch.append(dataPoint.getMax(), 1);
                    }
                }
            }
            bucketsScratch.writeBuckets(builder);
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

    private static class IndexWithCountList {
        private long[] indices = new long[32];
        private long[] counts = new long[32];
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
                builder.startObject("zero").field("count", zeroCount).endObject();
            }
            if (negativeBuckets.size() > 0) {
                // write in inverse order to get lowest to highest index
                builder.startObject("negative");
                builder.startArray("indices");
                for (int i = negativeBuckets.size() - 1; i >= 0; i--) {
                    builder.value(negativeBuckets.getIndex(i));
                }
                builder.endArray();
                builder.startArray("counts");
                for (int i = negativeBuckets.size() - 1; i >= 0; i--) {
                    builder.value(negativeBuckets.getCount(i));
                }
                builder.endArray();
                builder.endObject();
            }
            if (positiveBuckets.size() > 0) {
                builder.startObject("positive");
                builder.startArray("indices");
                for (int i = 0; i < positiveBuckets.size(); i++) {
                    builder.value(positiveBuckets.getIndex(i));
                }
                builder.endArray();
                builder.startArray("counts");
                for (int i = 0; i < positiveBuckets.size(); i++) {
                    builder.value(positiveBuckets.getCount(i));
                }
                builder.endArray();
                builder.endObject();
            }
        }
    }
}
