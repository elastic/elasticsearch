/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.transform.transforms.pivot;

import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.PercentilesAggregationBuilder;
import org.elasticsearch.xpack.transform.utils.OutputFieldNameConverter;

import java.util.Arrays;
import java.util.Collections;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public final class Aggregations {

    // the field mapping should not explicitly be set and allow ES to dynamically determine mapping via the data.
    private static final String DYNAMIC = "_dynamic";
    // the field mapping should be determined explicitly from the source field mapping if possible.
    private static final String SOURCE = "_source";

    public static final String FLOAT = "float";
    public static final String SCALED_FLOAT = "scaled_float";
    public static final String DOUBLE = "double";
    public static final String LONG = "long";
    public static final String GEO_SHAPE = "geo_shape";
    public static final String GEO_POINT = "geo_point";

    private Aggregations() {}

    /**
     * Supported aggregation by transform and corresponding meta information.
     *
     * aggregationType - the name of the aggregation as returned by
     * {@link org.elasticsearch.search.aggregations.BaseAggregationBuilder#getType()}}
     *
     * targetMapping - the field type for the output, if null, the source type should be used
     *
     */
    enum AggregationType {
        AVG("avg", DOUBLE),
        CARDINALITY("cardinality", LONG),
        VALUE_COUNT("value_count", LONG),
        MAX("max", SOURCE),
        MIN("min", SOURCE),
        SUM("sum", DOUBLE),
        GEO_CENTROID("geo_centroid", GEO_POINT),
        GEO_BOUNDS("geo_bounds", GEO_SHAPE),
        SCRIPTED_METRIC("scripted_metric", DYNAMIC),
        WEIGHTED_AVG("weighted_avg", DYNAMIC),
        BUCKET_SELECTOR("bucket_selector", DYNAMIC),
        BUCKET_SCRIPT("bucket_script", DYNAMIC),
        PERCENTILES("percentiles", DOUBLE);

        private final String aggregationType;
        private final String targetMapping;

        AggregationType(String name, String targetMapping) {
            this.aggregationType = name;
            this.targetMapping = targetMapping;
        }

        public String getName() {
            return aggregationType;
        }

        public String getTargetMapping() {
            return targetMapping;
        }
    }

    private static Set<String> aggregationSupported = Stream.of(AggregationType.values())
        .map(AggregationType::name)
        .collect(Collectors.toSet());

    public static boolean isSupportedByTransform(String aggregationType) {
        return aggregationSupported.contains(aggregationType.toUpperCase(Locale.ROOT));
    }

    public static boolean isDynamicMapping(String targetMapping) {
        return DYNAMIC.equals(targetMapping);
    }

    public static String resolveTargetMapping(String aggregationType, String sourceType) {
        AggregationType agg = AggregationType.valueOf(aggregationType.toUpperCase(Locale.ROOT));

        if (agg.getTargetMapping().equals(SOURCE)) {
            // scaled float requires an additional parameter "scaling_factor", which we do not know, therefore we fallback to float
            if (sourceType.equals(SCALED_FLOAT)) {
                return FLOAT;
            }
            return sourceType;
        }

        return agg.getTargetMapping();
    }

    public static Map<String, String> getAggregationOutputTypes(AggregationBuilder agg) {
        if (agg instanceof PercentilesAggregationBuilder) {
            PercentilesAggregationBuilder percentilesAgg = (PercentilesAggregationBuilder) agg;

            // note: eclipse does not like p -> agg.getType()
            // the merge function (p1, p2) -> p1 ignores duplicates
            return Arrays.stream(percentilesAgg.percentiles())
                .mapToObj(OutputFieldNameConverter::fromDouble)
                .collect(Collectors.toMap(p -> agg.getName() + "." + p, p -> { return agg.getType(); }, (p1, p2) -> p1));
        }
        // catch all
        return Collections.singletonMap(agg.getName(), agg.getType());
    }

}
