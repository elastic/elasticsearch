/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.transform.transforms.pivot;

import org.elasticsearch.core.Tuple;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.range.RangeAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.range.RangeAggregator.Range;
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
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public final class TransformAggregations {

    // the field mapping should not explicitly be set and allow ES to dynamically determine mapping via the data.
    private static final String DYNAMIC = "_dynamic";
    // the field mapping should be determined explicitly from the source field mapping if possible.
    private static final String SOURCE = "_source";

    public static final String FLOAT = "float";
    public static final String FLATTENED = "flattened";
    public static final String SCALED_FLOAT = "scaled_float";
    public static final String DOUBLE = "double";
    public static final String AGGREGATE_METRIC_DOUBLE = "aggregate_metric_double";
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
     * Please add it to the list (sorted) together with a comment containing a link to the created GitHub issue.
     */
    private static final List<String> UNSUPPORTED_AGGS = Arrays.asList(
        "adjacency_matrix",
        "auto_date_histogram",
        "composite", // DONT because it makes no sense
        "date_histogram",
        "date_range",
        "diversified_sampler",
        "filters",
        "geo_distance",
        "geohash_grid",
        "geotile_grid",
        "global",
        "histogram",
        "ip_prefix",
        "ip_range",
        "matrix_stats",
        "nested",
        "percentile_ranks",
        "random_sampler",
        "reverse_nested",
        "sampler",
        "significant_terms", // https://github.com/elastic/elasticsearch/issues/51073
        "significant_text",
        "string_stats", // https://github.com/elastic/elasticsearch/issues/51925
        "top_hits",
        "t_test", // https://github.com/elastic/elasticsearch/issues/54503,
        "variable_width_histogram", // https://github.com/elastic/elasticsearch/issues/58140
        "rate", // https://github.com/elastic/elasticsearch/issues/61351
        "multi_terms", // https://github.com/elastic/elasticsearch/issues/67609
        "time_series" // https://github.com/elastic/elasticsearch/issues/74660
    );

    private TransformAggregations() {}

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
        MEDIAN_ABSOLUTE_DEVIATION("median_absolute_deviation", DOUBLE),
        CARDINALITY("cardinality", LONG),
        VALUE_COUNT("value_count", LONG),
        MAX("max", SOURCE),
        MIN("min", SOURCE),
        SUM("sum", DOUBLE),
        GEO_BOUNDS("geo_bounds", GEO_SHAPE),
        GEO_CENTROID("geo_centroid", GEO_POINT),
        GEO_LINE("geo_line", GEO_SHAPE),
        SCRIPTED_METRIC("scripted_metric", DYNAMIC),
        WEIGHTED_AVG("weighted_avg", DOUBLE),
        BUCKET_SELECTOR("bucket_selector", DYNAMIC),
        BUCKET_SCRIPT("bucket_script", DYNAMIC),
        PERCENTILES("percentiles", DOUBLE),
        RANGE("range", LONG),
        FILTER("filter", LONG),
        TERMS("terms", FLATTENED),
        RARE_TERMS("rare_terms", FLATTENED),
        MISSING("missing", LONG),
        TOP_METRICS("top_metrics", SOURCE),
        STATS("stats", DOUBLE),
        BOXPLOT("boxplot", DOUBLE),
        EXTENDED_STATS("extended_stats", DOUBLE);

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

            // min/max/sum aggregations over aggregate_metric_double return double
            if (sourceType.equals(AGGREGATE_METRIC_DOUBLE)) {
                return DOUBLE;
            }

            return sourceType;
        }

        return agg.getTargetMapping();
    }

    /**
     * Checks the aggregation object and returns a tuple with 2 maps:
     *
     * 1. mapping the name of the agg to the used field
     * 2. mapping the name of the agg to the aggregation type
     *
     * Example:
     * {
     *   "my_agg": {
     *     "max": {
     *       "field": "my_field"
     * }}}
     *
     * creates ({ "my_agg": "my_field" }, { "my_agg": "max" })
     *
     * Both mappings can contain _multiple_ entries, e.g. due to sub aggregations or because of aggregations creating multiple
     * values(e.g. percentiles)
     *
     * Note about order: aggregation can hit in multiple places (e.g. a multi value agg implement {@link ValuesSourceAggregationBuilder})
     * Be careful changing the order in this method
     *
     * @param agg the aggregation builder
     * @return a tuple with 2 mappings that maps the used field(s) and aggregation type(s)
     */
    public static Tuple<Map<String, String>, Map<String, String>> getAggregationInputAndOutputTypes(AggregationBuilder agg) {
        // todo: can this be removed?
        if (agg instanceof PercentilesAggregationBuilder percentilesAgg) {

            // the merge function (p1, p2) -> p1 ignores duplicates
            return new Tuple<>(
                Collections.emptyMap(),
                Arrays.stream(percentilesAgg.percentiles())
                    .mapToObj(OutputFieldNameConverter::fromDouble)
                    .collect(Collectors.toMap(p -> percentilesAgg.getName() + "." + p, p -> percentilesAgg.getType(), (p1, p2) -> p1))
            );
        }

        if (agg instanceof RangeAggregationBuilder rangeAgg) {
            HashMap<String, String> outputTypes = new HashMap<>();
            HashMap<String, String> inputTypes = new HashMap<>();
            for (Range range : rangeAgg.ranges()) {
                String fieldName = rangeAgg.getName() + "." + generateKeyForRange(range.getFrom(), range.getTo());
                if (rangeAgg.getSubAggregations().isEmpty()) {
                    outputTypes.put(fieldName, AggregationType.RANGE.getName());
                    continue;
                }
                for (AggregationBuilder subAgg : rangeAgg.getSubAggregations()) {
                    Tuple<Map<String, String>, Map<String, String>> subAggregationTypes = getAggregationInputAndOutputTypes(subAgg);
                    for (Entry<String, String> subAggOutputType : subAggregationTypes.v2().entrySet()) {
                        outputTypes.put(String.join(".", fieldName, subAggOutputType.getKey()), subAggOutputType.getValue());
                    }
                    for (Entry<String, String> subAggInputType : subAggregationTypes.v1().entrySet()) {
                        inputTypes.put(String.join(".", fieldName, subAggInputType.getKey()), subAggInputType.getValue());
                    }
                }
            }
            return new Tuple<>(inputTypes, outputTypes);
        }

        // does the agg specify output field names
        Optional<Set<String>> outputFieldNames = agg.getOutputFieldNames();
        if (outputFieldNames.isPresent()) {
            return new Tuple<>(
                outputFieldNames.get()
                    .stream()
                    .collect(
                        Collectors.toMap(outputField -> agg.getName() + "." + outputField, outputField -> outputField, (v1, v2) -> v1)
                    ),
                outputFieldNames.get()
                    .stream()
                    .collect(
                        Collectors.toMap(outputField -> agg.getName() + "." + outputField, outputField -> agg.getType(), (v1, v2) -> v1)
                    )
            );
        }

        if (agg instanceof ValuesSourceAggregationBuilder<?> valueSourceAggregation) {
            return new Tuple<>(
                Collections.singletonMap(valueSourceAggregation.getName(), valueSourceAggregation.field()),
                Collections.singletonMap(valueSourceAggregation.getName(), valueSourceAggregation.getType())
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

    // Visible for testing
    static String generateKeyForRange(double from, double to) {
        return new StringBuilder().append(Double.isInfinite(from) ? "*" : OutputFieldNameConverter.fromDouble(from))
            .append("-")
            .append(Double.isInfinite(to) ? "*" : OutputFieldNameConverter.fromDouble(to))
            .toString();
    }
}
