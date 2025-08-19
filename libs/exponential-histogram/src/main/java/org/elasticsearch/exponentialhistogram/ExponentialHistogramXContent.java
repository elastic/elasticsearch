/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.exponentialhistogram;

import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;

/**
 * Handles the serialization of an {@link ExponentialHistogram} to XContent.
 */
public class ExponentialHistogramXContent {

    public static final String SCALE_FIELD = "scale";
    public static final String ZERO_FIELD = "zero";
    public static final String ZERO_COUNT_FIELD = "count";
    public static final String ZERO_THRESHOLD_FIELD = "threshold";
    public static final String POSITIVE_FIELD = "positive";
    public static final String NEGATIVE_FIELD = "negative";
    public static final String BUCKET_INDICES_FIELD = "indices";
    public static final String BUCKET_COUNTS_FIELD = "counts";

    /**
     * Serializes an {@link ExponentialHistogram} to the provided {@link XContentBuilder}.
     * @param builder the XContentBuilder to write to
     * @param histogram the ExponentialHistogram to serialize
     * @throws IOException if the XContentBuilder throws an IOException
     */
    public static void serialize(XContentBuilder builder, ExponentialHistogram histogram) throws IOException {
        builder.startObject();

        builder.field(SCALE_FIELD, histogram.scale());
        double zeroThreshold = histogram.zeroBucket().zeroThreshold();
        long zeroCount = histogram.zeroBucket().count();

        if (zeroCount != 0 || zeroThreshold != 0) {
            builder.startObject(ZERO_FIELD);
            if (zeroCount != 0) {
                builder.field(ZERO_COUNT_FIELD, zeroCount);
            }
            if (zeroThreshold != 0) {
                builder.field(ZERO_THRESHOLD_FIELD, zeroThreshold);
            }
            builder.endObject();
        }

        writeBuckets(builder, POSITIVE_FIELD, histogram.positiveBuckets());
        writeBuckets(builder, NEGATIVE_FIELD, histogram.negativeBuckets());

        builder.endObject();
    }

    private static void writeBuckets(XContentBuilder b, String fieldName, ExponentialHistogram.Buckets buckets) throws IOException {
        if (buckets.iterator().hasNext() == false) {
            return;
        }
        b.startObject(fieldName);
        BucketIterator it = buckets.iterator();
        b.startArray(BUCKET_INDICES_FIELD);
        while (it.hasNext()) {
            b.value(it.peekIndex());
            it.advance();
        }
        b.endArray();
        it = buckets.iterator();
        b.startArray(BUCKET_COUNTS_FIELD);
        while (it.hasNext()) {
            b.value(it.peekCount());
            it.advance();
        }
        b.endArray();
        b.endObject();
    }

}
