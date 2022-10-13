/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.rollup;

import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.index.mapper.NumberFieldMapper;
import org.elasticsearch.search.aggregations.metrics.AvgAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.MaxAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.MinAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.SumAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.ValueCountAggregationBuilder;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregationBuilder;
import org.elasticsearch.xcontent.ParseField;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class RollupField {
    // Fields that are used both in core Rollup actions and Rollup plugin
    public static final ParseField ID = new ParseField("id");
    public static final String TASK_NAME = "xpack/rollup/job";
    public static final String ROLLUP_META = "_rollup";
    public static final String INTERVAL = "interval";
    public static final String COUNT_FIELD = "_count";
    public static final String VERSION_FIELD = "version";
    public static final String VALUE = "value";
    public static final String TIMESTAMP = "timestamp";
    public static final String FILTER = "filter";
    public static final String NAME = "rollup";
    public static final String TYPE_NAME = "_doc";
    public static final String AGG = "agg";
    public static final String ROLLUP_MISSING = "ROLLUP_MISSING_40710B25931745D4B0B8B310F6912A69";
    public static final List<String> SUPPORTED_NUMERIC_METRICS = Arrays.asList(
        MaxAggregationBuilder.NAME,
        MinAggregationBuilder.NAME,
        SumAggregationBuilder.NAME,
        AvgAggregationBuilder.NAME,
        ValueCountAggregationBuilder.NAME
    );
    public static final List<String> SUPPORTED_DATE_METRICS = Arrays.asList(
        MaxAggregationBuilder.NAME,
        MinAggregationBuilder.NAME,
        ValueCountAggregationBuilder.NAME
    );

    // a set of ALL our supported metrics, to be a union of all other supported metric types (numeric, date, etc.)
    public static final Set<String> SUPPORTED_METRICS;
    static {
        SUPPORTED_METRICS = new HashSet<>();
        SUPPORTED_METRICS.addAll(SUPPORTED_NUMERIC_METRICS);
        SUPPORTED_METRICS.addAll(SUPPORTED_DATE_METRICS);
    }

    // these mapper types are used by the configs (metric, histo, etc) to validate field mappings
    public static final List<String> NUMERIC_FIELD_MAPPER_TYPES;
    static {
        List<String> types = Stream.of(NumberFieldMapper.NumberType.values())
            .map(NumberFieldMapper.NumberType::typeName)
            .collect(Collectors.toList());
        types.add("scaled_float"); // have to add manually since scaled_float is in a module
        NUMERIC_FIELD_MAPPER_TYPES = types;
    }

    public static final List<String> DATE_FIELD_MAPPER_TYPES = List.of(
        DateFieldMapper.CONTENT_TYPE,
        DateFieldMapper.DATE_NANOS_CONTENT_TYPE
    );

    /**
     * Format to the appropriate Rollup field name convention
     *
     * @param source Source aggregation to get type and name from
     * @param extra The type of value this field is (VALUE, INTERVAL, etc)
     * @return formatted field name
     */
    public static String formatFieldName(ValuesSourceAggregationBuilder<?> source, String extra) {
        return source.field() + "." + source.getType() + "." + extra;
    }

    /**
     * Format to the appropriate Rollup field name convention
     *
     * @param field The field we are formatting
     * @param type  The aggregation type that was used for rollup
     * @param extra The type of value this field is (VALUE, INTERVAL, etc)
     * @return formatted field name
     */
    public static String formatFieldName(String field, String type, String extra) {
        return field + "." + type + "." + extra;
    }

    /**
     * Format to the appropriate Rollup convention for internal Metadata fields (_rollup)
     */
    public static String formatMetaField(String extra) {
        return RollupField.ROLLUP_META + "." + extra;
    }

    /**
     * Format to the appropriate Rollup convention for extra Count aggs.
     * These are added to averages and bucketing aggs that need a count
     */
    public static String formatCountAggName(String field) {
        return field + "." + RollupField.COUNT_FIELD;
    }

    /**
     * Format to the appropriate Rollup convention for agg names that
     * might conflict with empty buckets.  `value` is appended to agg name.
     * E.g. used for averages
     */
    public static String formatValueAggName(String field) {
        return field + "." + RollupField.VALUE;
    }

    /**
     * Format into the convention for computed field lookups
     */
    public static String formatComputed(String field, String agg) {
        return field + "." + agg;
    }

    /**
     * Format into the convention used by the Indexer's composite agg, so that
     * the normal field name is translated into a Rollup fieldname via the agg name
     */
    public static String formatIndexerAggName(String field, String agg) {
        return field + "." + agg;
    }
}
