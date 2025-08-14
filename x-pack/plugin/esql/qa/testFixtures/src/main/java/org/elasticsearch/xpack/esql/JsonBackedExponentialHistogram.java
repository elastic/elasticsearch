/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.elasticsearch.exponentialhistogram.CopyableBucketIterator;
import org.elasticsearch.exponentialhistogram.ExponentialHistogram;
import org.elasticsearch.exponentialhistogram.ZeroBucket;

import java.util.List;
import java.util.Map;
import java.util.OptionalLong;

import static org.elasticsearch.exponentialhistogram.ExponentialHistogramXContent.BUCKETS_COUNTS_FIELD;
import static org.elasticsearch.exponentialhistogram.ExponentialHistogramXContent.BUCKETS_INDICES_FIELD;
import static org.elasticsearch.exponentialhistogram.ExponentialHistogramXContent.NEGATIVE_BUCKETS_FIELD;
import static org.elasticsearch.exponentialhistogram.ExponentialHistogramXContent.POSITIVE_BUCKETS_FIELD;
import static org.elasticsearch.exponentialhistogram.ExponentialHistogramXContent.SCALE_FIELD;
import static org.elasticsearch.exponentialhistogram.ExponentialHistogramXContent.ZERO_BUCKET_FIELD;
import static org.elasticsearch.exponentialhistogram.ExponentialHistogramXContent.ZERO_COUNT_FIELD;
import static org.elasticsearch.exponentialhistogram.ExponentialHistogramXContent.ZERO_THRESHOLD_FIELD;

public class JsonBackedExponentialHistogram implements ExponentialHistogram {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    static  {
        MAPPER.configure(JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES, true);
    }

    private final int scale;
    private final ZeroBucket zeroBucket;

    private final SerializedBuckets positive;
    private final SerializedBuckets negative;

    private JsonBackedExponentialHistogram(int scale, ZeroBucket zeroBucket,
                                          List<? extends Number> negativeIndices,
                                          List<? extends Number> negativeCounts,
                                          List<? extends Number> positiveIndices,
                                            List<? extends Number> positiveCounts)
    {
        this.scale = scale;
        this.zeroBucket = zeroBucket;
        this.positive = new SerializedBuckets(negativeIndices, negativeCounts);
        this.negative = new SerializedBuckets(positiveIndices, positiveCounts);
    }

    @SuppressWarnings("unchecked")
    static ExponentialHistogram createFromJson(String json) {
        try {
            ZeroBucket zeroBucket = ZeroBucket.minimalEmpty();
            List<? extends Number> negativeIndices = List.of();
            List<? extends Number> negativeCounts = List.of();
            List<? extends Number> positiveIndices = List.of();
            List<? extends Number> positiveCounts = List.of();

            Map<?,?> data = MAPPER.readValue(json, Map.class);
            if (data == null) {
                return null;
            }
            int scale = ((Number) data.get(SCALE_FIELD)).intValue();
            Map<?, ?> zero = (Map<?, ?>) data.get(ZERO_BUCKET_FIELD);
            if (zero != null) {
                Number threshold = (Number) zero.get(ZERO_THRESHOLD_FIELD);
                Number count = (Number) zero.get(ZERO_COUNT_FIELD);
                zeroBucket = new ZeroBucket(
                    threshold == null ? 0.0 : threshold.doubleValue(),
                    count == null ? 0 :count.longValue());
            }
            Map<?,Object> negative = (Map<?,Object>) data.get(NEGATIVE_BUCKETS_FIELD);
            if (negative != null) {
                negativeIndices = (List<? extends Number>) negative.getOrDefault(BUCKETS_INDICES_FIELD, List.of());
                negativeCounts = (List<? extends Number>) negative.getOrDefault(BUCKETS_COUNTS_FIELD, List.of());
            }
            Map<?,Object> positive = (Map<?,Object>) data.get(POSITIVE_BUCKETS_FIELD);
            if (positive != null) {
                positiveIndices = (List<? extends Number>) positive.getOrDefault(BUCKETS_INDICES_FIELD, List.of());
                positiveCounts = (List<? extends Number>) positive.getOrDefault(BUCKETS_COUNTS_FIELD, List.of());
            }
            return new JsonBackedExponentialHistogram(
                scale, zeroBucket, negativeIndices, negativeCounts, positiveIndices, positiveCounts
            );
        } catch (Exception e) {
            throw new RuntimeException("Failed to parse ExponentialHistogram from JSON", e);
        }
    }
    private class SerializedBuckets implements ExponentialHistogram.Buckets {
        private final List<? extends Number> indices;
        private final List<? extends Number> counts;

        private SerializedBuckets(List<? extends Number> indices, List<? extends Number> counts) {
            this.indices = indices;
            this.counts = counts;
        }

        @Override
        public CopyableBucketIterator iterator() {
            return new Iterator(0);
        }

        @Override
        public OptionalLong maxBucketIndex() {
            if (indices.isEmpty()) {
                return OptionalLong.empty();
            } else {
                return  OptionalLong.of(indices.get(indices.size() - 1).longValue());
            }
        }

        @Override
        public long valueCount() {
            long sum = 0;
            for (Number count : counts) {
                sum += count.longValue();
            }
            return sum;
        }

        @Override
        public int bucketCount() {
            return indices.size();
        }

        private class Iterator implements CopyableBucketIterator {

            private int index = 0;

            Iterator(int index) {
                this.index = index;
            }

            @Override
            public boolean hasNext() {
                return index < indices.size();
            }

            @Override
            public long peekCount() {
                return counts.get(index).longValue();
            }

            @Override
            public long peekIndex() {
                return indices.get(index).longValue();
            }

            @Override
            public void advance() {
                index++;
            }

            @Override
            public int scale() {
                return scale;
            }

            @Override
            public CopyableBucketIterator copy() {
                return new Iterator(index);
            }
        }
    }

    @Override
    public int scale() {
        return scale;
    }

    @Override
    public ZeroBucket zeroBucket() {
        return zeroBucket;
    }

    @Override
    public Buckets positiveBuckets() {
        return positive;
    }

    @Override
    public Buckets negativeBuckets() {
        return negative;
    }

}
