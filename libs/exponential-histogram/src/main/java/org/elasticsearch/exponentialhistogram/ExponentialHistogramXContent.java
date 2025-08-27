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
