/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.transform.transforms.pivot;

import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.PercentilesAggregationBuilder;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregationBuilder;
import org.elasticsearch.xpack.transform.utils.OutputFieldNameConverter;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public final class Aggregations {

    // the field mapping should not explicitly be set and allow ES to dynamically determine mapping via the data.
    private static final String DYNAMIC = "_dynamic";
    // the field mapping should be determined explicitly from the source field mapping if possible.
    private static final String SOURCE = "_source";

    public static final String FLOAT = "float";
    public static final String FLATTENED = "flattened";
    public static final String SCALED_FLOAT = "scaled_float";
    public static final String DOUBLE = "double";
    public static final String LONG = "long";
    public static final String GEO_SHAPE = "geo_shape";
    public static final String GEO_POINT = "geo_point";

    /*
     * List of currently unsupported aggregations (not group_by) in transform.
     *
     * The only purpose of this list is to track which aggregations should be added to transform and assert if new
     * aggregations are added.
     *
     * Created a new aggs?
     *
     * Please add it to the list (sorted) together with a comment containing a link to the created github issue.
     */
    private static final List<String> UNSUPPORTED_AGGS = Arrays.asList(
        "adjacency_matrix",
        "auto_date_histogram",
        "boxplot", // https://github.com/elastic/elasticsearch/issues/52189
        "composite", // DONT because it makes no sense
        "date_histogram",
        "date_range",
        "diversified_sampler",
        "extended_stats", // https://github.com/elastic/elasticsearch/issues/51925
        "filters",
        "geo_distance",
        "geohash_grid",
        "geotile_grid",
        "global",
        "histogram",
        "ip_range",
        "matrix_stats",
        "median_absolute_deviation",
        "missing",
        "nested",
        "percentile_ranks",
        "range",
        "reverse_nested",
        "sampler",
        "significant_terms", // https://github.com/elastic/elasticsearch/issues/51073
        "significant_text",
        "stats", // https://github.com/elastic/elasticsearch/issues/51925
        "string_stats", // https://github.com/elastic/elasticsearch/issues/51925
        "top_hits",
        "top_metrics", // https://github.com/elastic/elasticsearch/issues/52236
        "t_test", // https://github.com/elastic/elasticsearch/issues/54503,
        "variable_width_histogram" // https://github.com/elastic/elasticsearch/issues/58140
    );

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
        PERCENTILES("percentiles", DOUBLE),
        FILTER("filter", LONG),
        TERMS("terms", FLATTENED),
        RARE_TERMS("rare_terms", FLATTENED);

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

    private static Set<String> aggregationsNotSupported = UNSUPPORTED_AGGS.stream()
        .map(agg -> agg.toUpperCase(Locale.ROOT))
        .collect(Collectors.toSet());

    public static boolean isSupportedByTransform(String aggregationType) {
        return aggregationSupported.contains(aggregationType.toUpperCase(Locale.ROOT));
    }

    // only for testing
    static boolean isUnSupportedByTransform(String aggregationType) {
        return aggregationsNotSupported.contains(aggregationType.toUpperCase(Locale.ROOT));
    }

    public static boolean isDynamicMapping(String targetMapping) {
        return DYNAMIC.equals(targetMapping);
    }

    public static String resolveTargetMapping(String aggregationType, String sourceType) {
        AggregationType agg = AggregationType.valueOf(aggregationType.toUpperCase(Locale.ROOT));

        if (agg.getTargetMapping().equals(SOURCE)) {

            if (sourceType == null) {
                // this should never happen and would mean a bug in the calling code, the error is logged in {@link
                // org.elasticsearch.xpack.transform.transforms.pivot.SchemaUtil#resolveMappings()}
                return null;
            }

            // scaled float requires an additional parameter "scaling_factor", which we do not know, therefore we fallback to float
            if (sourceType.equals(SCALED_FLOAT)) {
                return FLOAT;
            }

            return sourceType;
        }

        return agg.getTargetMapping();
    }

    public static Tuple<Map<String, String>, Map<String, String>> getAggregationInputAndOutputTypes(AggregationBuilder agg) {
        if (agg instanceof PercentilesAggregationBuilder) {
            PercentilesAggregationBuilder percentilesAgg = (PercentilesAggregationBuilder) agg;

            // note: eclipse does not like p -> agg.getType()
            // the merge function (p1, p2) -> p1 ignores duplicates
            return new Tuple<>(
                Collections.emptyMap(),
                Arrays.stream(percentilesAgg.percentiles())
                    .mapToObj(OutputFieldNameConverter::fromDouble)
                    .collect(Collectors.toMap(p -> agg.getName() + "." + p, p -> { return agg.getType(); }, (p1, p2) -> p1))
            );
        }

        if (agg instanceof ValuesSourceAggregationBuilder) {
            ValuesSourceAggregationBuilder<?> valueSourceAggregation = (ValuesSourceAggregationBuilder<?>) agg;
            return new Tuple<>(
                Collections.singletonMap(valueSourceAggregation.getName(), valueSourceAggregation.field()),
                Collections.singletonMap(agg.getName(), agg.getType())
            );
        }

        // does the agg have sub aggregations?
        if (agg.getSubAggregations().size() > 0) {
            HashMap<String, String> outputTypes = new HashMap<>();
            HashMap<String, String> inputTypes = new HashMap<>();

            for (AggregationBuilder subAgg : agg.getSubAggregations()) {
                Tuple<Map<String, String>, Map<String, String>> subAggregationTypes = getAggregationInputAndOutputTypes(subAgg);

                for (Entry<String, String> subAggOutputType : subAggregationTypes.v2().entrySet()) {
                    outputTypes.put(String.join(".", agg.getName(), subAggOutputType.getKey()), subAggOutputType.getValue());
                }

                for (Entry<String, String> subAggInputType : subAggregationTypes.v1().entrySet()) {
                    inputTypes.put(String.join(".", agg.getName(), subAggInputType.getKey()), subAggInputType.getValue());
                }
            }

            return new Tuple<>(inputTypes, outputTypes);
        }

        // catch all in case no special handling required
        return new Tuple<>(Collections.emptyMap(), Collections.singletonMap(agg.getName(), agg.getType()));
    }

}
