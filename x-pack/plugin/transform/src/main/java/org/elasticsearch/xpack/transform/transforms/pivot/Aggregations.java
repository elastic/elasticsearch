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
        AVG("avg", "double"),
        CARDINALITY("cardinality", "long"),
        VALUE_COUNT("value_count", "long"),
        MAX("max", SOURCE),
        MIN("min", SOURCE),
        SUM("sum", "double"),
        GEO_CENTROID("geo_centroid", "geo_point"),
        GEO_BOUNDS("geo_bounds", "geo_shape"),
        SCRIPTED_METRIC("scripted_metric", DYNAMIC),
        WEIGHTED_AVG("weighted_avg", DYNAMIC),
        BUCKET_SELECTOR("bucket_selector", DYNAMIC),
        BUCKET_SCRIPT("bucket_script", DYNAMIC),
        PERCENTILES("percentiles", "double");

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
        return agg.getTargetMapping().equals(SOURCE) ? sourceType : agg.getTargetMapping();
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
