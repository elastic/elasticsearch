/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.dataframe.transforms.pivot;

import org.elasticsearch.search.aggregations.bucket.composite.CompositeAggregation.Bucket;

import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public final class Aggregations {

    // the field mapping should not explicitly be set and allow ES to dynamically determine mapping via the data.
    private static final String DYNAMIC = "_dynamic";
    // the field mapping should be determined explicitly from the source field mapping if possible.
    private static final String SOURCE = "_source";
    private Aggregations() {}

    /**
     * Supported aggregation by dataframe and corresponding meta information.
     *
     * aggregationType - the name of the aggregation as returned by
     * {@link org.elasticsearch.search.aggregations.BaseAggregationBuilder#getType()}}
     *
     * targetMapping - the field type for the output, if null, the source type should be used
     *
     */
    public enum AggregationType {
        AVG("avg", "double"),
        COUNT("count", "long", Aggregations::extractCount),
        CARDINALITY("cardinality", "long"),
        VALUE_COUNT("value_count", "long"),
        MAX("max", SOURCE),
        MIN("min", SOURCE),
        SUM("sum", "double"),
        GEO_CENTROID("geo_centroid", "geo_point"),
        SCRIPTED_METRIC("scripted_metric", DYNAMIC),
        WEIGHTED_AVG("weighted_avg", DYNAMIC),
        BUCKET_SCRIPT("bucket_script", DYNAMIC);

        private final String aggregationType;
        private final String targetMapping;
        private final Function<Bucket, Object> bucketFunction;

        AggregationType(String name, String targetMapping) {
            this(name, targetMapping, null);
        }

        AggregationType(String name, String targetMapping, Function<Bucket, Object> bucketFunction) {
            this.aggregationType = name;
            this.targetMapping = targetMapping;
            this.bucketFunction = bucketFunction;
        }

        public String getName() {
            return aggregationType;
        }

        public String getTargetMapping() {
            return targetMapping;
        }

        public Function<Bucket, Object> getBucketFunction() {
            return bucketFunction;
        }

        public boolean isSpecialAggregation() {
            return bucketFunction != null;
        }
    }

    private static Set<String> aggregationSupported = Stream.of(AggregationType.values()).map(AggregationType::name)
            .collect(Collectors.toSet());

    public static boolean isSupportedByDataframe(String aggregationType) {
        return aggregationSupported.contains(aggregationType.toUpperCase(Locale.ROOT));
    }

    public static boolean isDynamicMapping(String targetMapping) {
        return DYNAMIC.equals(targetMapping);
    }

    public static boolean isSpecialAggregation(String aggregationType) {
        return AggregationType.valueOf(aggregationType.toUpperCase(Locale.ROOT)).isSpecialAggregation();
    }

    public static Function<Bucket, Object> getBucketFunction(String aggregationType) {
        return AggregationType.valueOf(aggregationType.toUpperCase(Locale.ROOT)).getBucketFunction();
    }

    public static String resolveTargetMapping(String aggregationType, String sourceType) {
        AggregationType agg = AggregationType.valueOf(aggregationType.toUpperCase(Locale.ROOT));
        return agg.getTargetMapping().equals(SOURCE) ? sourceType : agg.getTargetMapping();
    }

    public static Map<String, String> getSpecialAggregations(Map<String, Object> source) {
        Map<String, String> specialAggregations = new LinkedHashMap<>();

        for (Entry<String, Object> entry : source.entrySet()) {
            String field = entry.getKey();
            if (entry.getValue() instanceof Map) {
                @SuppressWarnings("unchecked")
                Map<String, Object> aggregations = (Map<String, Object>) entry.getValue();
                if (aggregations.size() == 1) {
                    Entry<String, Object> agg = aggregations.entrySet().iterator().next();

                    // skip over unknowns, by design do not validate
                    if (isSupportedByDataframe(agg.getKey()) == false) {
                        continue;
                    }

                    AggregationType aggType = AggregationType.valueOf(agg.getKey().toUpperCase(Locale.ROOT));
                    if (aggType.isSpecialAggregation()) {
                        specialAggregations.put(field, agg.getKey());
                    }
                }
            }
        }

        return specialAggregations.isEmpty() ? null : specialAggregations;
    }

    public static Map<String, Object> filterSpecialAggregations(Map<String, Object> source) {
        Set<String> fieldsToRemove = new HashSet<>();

        for (Entry<String, Object> entry : source.entrySet()) {
            String field = entry.getKey();
            if (entry.getValue() instanceof Map) {
                @SuppressWarnings("unchecked")
                Map<String, Object> aggregations = (Map<String, Object>) entry.getValue();
                if (aggregations.size() == 1) {
                    Entry<String, Object> agg = aggregations.entrySet().iterator().next();
                    // skip over unknowns, by design do not validate
                    if (isSupportedByDataframe(agg.getKey()) == false) {
                        continue;
                    }

                    AggregationType aggType = AggregationType.valueOf(agg.getKey().toUpperCase(Locale.ROOT));
                    if (aggType.isSpecialAggregation()) {
                        // mark field to be removed
                        fieldsToRemove.add(field);
                    }
                }
            }
        }

        if (fieldsToRemove.size() > 0) {
            Map<String, Object> copyWithoutSpecialFields = new LinkedHashMap<>();
            for (Entry<String, Object> entry:source.entrySet()) {
                if (fieldsToRemove.contains(entry.getKey()) == false) {
                    copyWithoutSpecialFields.put(entry.getKey(), entry.getValue());
                }
            }

            return copyWithoutSpecialFields;
        }

        return source;
    }

    public static Long extractCount(Bucket bucket) {
        return bucket.getDocCount();
    }
}
