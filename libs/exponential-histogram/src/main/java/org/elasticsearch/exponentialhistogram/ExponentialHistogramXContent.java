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
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;

/**
 * Defines the standard serialization for ExponentialHistograms.
 */
public class ExponentialHistogramXContent {

    public static final String SCALE_FIELD = "scale";
    public static final String ZERO_BUCKET_FIELD = "zero";
    public static final String ZERO_COUNT_FIELD = "count";
    public static final String ZERO_THRESHOLD_FIELD = "threshold";
    public static final String POSITIVE_BUCKETS_FIELD = "positive";
    public static final String NEGATIVE_BUCKETS_FIELD = "negative";
    public static final String BUCKETS_INDICES_FIELD = "counts";
    public static final String BUCKETS_COUNTS_FIELD = "counts";

    public static void write(XContentBuilder b, ExponentialHistogram histogram) throws IOException {
        b.startObject();

        b.field(SCALE_FIELD, histogram.scale());
        double zeroThreshold = histogram.zeroBucket().zeroThreshold();
        long zeroCount = histogram.zeroBucket().count();

        if (zeroCount != 0 || zeroThreshold != 0) {
            b.startObject(ZERO_BUCKET_FIELD);
            if (zeroCount != 0) {
                b.field(ZERO_COUNT_FIELD, zeroCount);
            }
            if (zeroThreshold != 0) {
                b.field(ZERO_THRESHOLD_FIELD, zeroThreshold);
            }
            b.endObject();
        }

        writeBuckets(b, POSITIVE_BUCKETS_FIELD, histogram.positiveBuckets());
        writeBuckets(b, NEGATIVE_BUCKETS_FIELD, histogram.negativeBuckets());

        b.endObject();
    }

    private static void writeBuckets(XContentBuilder b, String fieldName, ExponentialHistogram.Buckets buckets) throws IOException {
        if (buckets.iterator().hasNext() == false) {
            return;
        }
        b.startObject(fieldName);
        BucketIterator it = buckets.iterator();
        b.startArray(BUCKETS_INDICES_FIELD);
        while (it.hasNext()) {
            b.value(it.peekIndex());
            it.advance();
        }
        b.endArray();
        it = buckets.iterator();
        b.startArray(BUCKETS_COUNTS_FIELD);
        while (it.hasNext()) {
            b.value(it.peekCount());
            it.advance();
        }
        b.endArray();
        b.endObject();
    }
}
