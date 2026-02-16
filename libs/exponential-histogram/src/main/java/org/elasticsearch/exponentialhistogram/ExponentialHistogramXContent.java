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

import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Types;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;

/**
 * Handles the serialization of an {@link ExponentialHistogram} to XContent.
 */
public class ExponentialHistogramXContent {

    public static final String SCALE_FIELD = "scale";
    public static final String SUM_FIELD = "sum";
    public static final String MIN_FIELD = "min";
    public static final String MAX_FIELD = "max";
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
    public static void serialize(XContentBuilder builder, @Nullable ExponentialHistogram histogram) throws IOException {
        if (histogram == null) {
            builder.nullValue();
            return;
        }
        builder.startObject();

        builder.field(SCALE_FIELD, histogram.scale());

        if (histogram.sum() != 0.0 || histogram.valueCount() > 0) {
            builder.field(SUM_FIELD, histogram.sum());
        }
        if (Double.isNaN(histogram.min()) == false) {
            builder.field(MIN_FIELD, histogram.min());
        }
        if (Double.isNaN(histogram.max()) == false) {
            builder.field(MAX_FIELD, histogram.max());
        }
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

    /**
     * Parses an {@link ExponentialHistogram} from the provided {@link XContentParser}.
     * This method is neither optimized, nor does it do any validation of the parsed content.
     * No estimation for missing sum/min/max is done.
     * Therefore only intended for testing!
     *
     * @param xContent the serialized histogram to read
     * @return the deserialized histogram
     * @throws IOException if the XContentParser throws an IOException
     */
    public static ExponentialHistogram parseForTesting(XContentParser xContent) throws IOException {
        if (xContent.currentToken() == null) {
            xContent.nextToken();
        }
        if (xContent.currentToken() == XContentParser.Token.VALUE_NULL) {
            return null;
        }
        return parseForTesting(xContent.map());
    }

    /**
     * Parses an {@link ExponentialHistogram} from a {@link Map}.
     * This method is neither optimized, nor does it do any validation of the parsed content.
     * No estimation for missing sum/min/max is done.
     * Therefore only intended for testing!
     *
     * @param xContent the serialized histogram as a map
     * @return the deserialized histogram
     */
    public static ExponentialHistogram parseForTesting(@Nullable Map<String, Object> xContent) {
        if (xContent == null) {
            return null;
        }
        int scale = ((Number) xContent.get(SCALE_FIELD)).intValue();
        ExponentialHistogramBuilder builder = ExponentialHistogram.builder(scale, ExponentialHistogramCircuitBreaker.noop());

        Map<String, Number> zero = Types.forciblyCast(xContent.getOrDefault(ZERO_FIELD, Collections.emptyMap()));
        double zeroThreshold = zero.getOrDefault(ZERO_THRESHOLD_FIELD, 0).doubleValue();
        long zeroCount = zero.getOrDefault(ZERO_COUNT_FIELD, 0).longValue();
        builder.zeroBucket(ZeroBucket.create(zeroThreshold, zeroCount));

        builder.sum(((Number) xContent.getOrDefault(SUM_FIELD, 0)).doubleValue());
        builder.min(((Number) xContent.getOrDefault(MIN_FIELD, Double.NaN)).doubleValue());
        builder.max(((Number) xContent.getOrDefault(MAX_FIELD, Double.NaN)).doubleValue());

        parseBuckets(Types.forciblyCast(xContent.getOrDefault(NEGATIVE_FIELD, Collections.emptyMap())), builder::setNegativeBucket);
        parseBuckets(Types.forciblyCast(xContent.getOrDefault(POSITIVE_FIELD, Collections.emptyMap())), builder::setPositiveBucket);

        return builder.build();
    }

    private static void parseBuckets(Map<String, List<Number>> serializedBuckets, BiConsumer<Long, Long> bucketSetter) {
        List<Number> indices = serializedBuckets.getOrDefault(BUCKET_INDICES_FIELD, Collections.emptyList());
        List<Number> counts = serializedBuckets.getOrDefault(BUCKET_COUNTS_FIELD, Collections.emptyList());
        assert indices.size() == counts.size();
        for (int i = 0; i < indices.size(); i++) {
            bucketSetter.accept(indices.get(i).longValue(), counts.get(i).longValue());
        }
    }
}
