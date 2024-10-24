/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.Build;
import org.elasticsearch.common.util.FeatureFlag;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.rest.action.admin.cluster.RestNodesCapabilitiesAction;
import org.elasticsearch.xpack.esql.core.plugin.EsqlCorePlugin;
import org.elasticsearch.xpack.esql.plugin.EsqlFeatures;
import org.elasticsearch.xpack.esql.plugin.EsqlPlugin;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Set;

/**
 * A {@link Set} of "capabilities" supported by the {@link RestEsqlQueryAction}
 * and {@link RestEsqlAsyncQueryAction} APIs. These are exposed over the
 * {@link RestNodesCapabilitiesAction} and we use them to enable tests.
 */
public class EsqlCapabilities {
    public enum Cap {
        /**
         * Support for function {@code REVERSE}.
         */
        FN_REVERSE,

        /**
         * Support for reversing whole grapheme clusters. This is not supported
         * on JDK versions less than 20 which are not supported in ES 9.0.0+ but this
         * exists to keep the {@code 8.x} branch similar to the {@code main} branch.
         */
        FN_REVERSE_GRAPHEME_CLUSTERS,

        /**
         * Support for function {@code CBRT}. Done in #108574.
         */
        FN_CBRT,

        /**
         * Support for function {@code HYPOT}.
         */
        FN_HYPOT,

        /**
         * Support for {@code MV_APPEND} function. #107001
         */
        FN_MV_APPEND,

        /**
         * Support for {@code MV_MEDIAN_ABSOLUTE_DEVIATION} function.
         */
        FN_MV_MEDIAN_ABSOLUTE_DEVIATION,

        /**
         * Support for {@code MV_PERCENTILE} function.
         */
        FN_MV_PERCENTILE,

        /**
         * Support for function {@code IP_PREFIX}.
         */
        FN_IP_PREFIX,

        /**
         * Fix on function {@code SUBSTRING} that makes it not return null on empty strings.
         */
        FN_SUBSTRING_EMPTY_NULL,

        /**
         * Support for the {@code INLINESTATS} syntax.
         */
        INLINESTATS(EsqlPlugin.INLINESTATS_FEATURE_FLAG),

        /**
         * Support for the expressions in grouping in {@code INLINESTATS} syntax.
         */
        INLINESTATS_V2(EsqlPlugin.INLINESTATS_FEATURE_FLAG),

        /**
         * Support for aggregation function {@code TOP}.
         */
        AGG_TOP,

        /**
         * Support for booleans in aggregations {@code MAX} and {@code MIN}.
         */
        AGG_MAX_MIN_BOOLEAN_SUPPORT,

        /**
         * Support for ips in aggregations {@code MAX} and {@code MIN}.
         */
        AGG_MAX_MIN_IP_SUPPORT,

        /**
         * Support for strings in aggregations {@code MAX} and {@code MIN}.
         */
        AGG_MAX_MIN_STRING_SUPPORT,

        /**
         * Support for booleans in {@code TOP} aggregation.
         */
        AGG_TOP_BOOLEAN_SUPPORT,

        /**
         * Support for ips in {@code TOP} aggregation.
         */
        AGG_TOP_IP_SUPPORT,

        /**
         * Support for {@code keyword} and {@code text} fields in {@code TOP} aggregation.
         */
        AGG_TOP_STRING_SUPPORT,

        /**
         * {@code CASE} properly handling multivalue conditions.
         */
        CASE_MV,

        /**
         * Optimization for ST_CENTROID changed some results in cartesian data. #108713
         */
        ST_CENTROID_AGG_OPTIMIZED,

        /**
         * Support for requesting the "_ignored" metadata field.
         */
        METADATA_IGNORED_FIELD,

        /**
         * LOOKUP command with
         * - tables using syntax {@code "tables": {"type": [<values>]}}
         * - fixed variable shadowing
         * - fixed Join.references(), requiring breaking change to Join serialization
         */
        LOOKUP_V4(Build.current().isSnapshot()),

        /**
         * Support for requesting the "REPEAT" command.
         */
        REPEAT,

        /**
         * Cast string literals to datetime in addition and subtraction when the other side is a date or time interval.
         */
        STRING_LITERAL_AUTO_CASTING_TO_DATETIME_ADD_SUB,

        /**
         * Support for named or positional parameters in EsqlQueryRequest.
         */
        NAMED_POSITIONAL_PARAMETER,

        /**
         * Support multiple field mappings if appropriate conversion function is used (union types)
         */
        UNION_TYPES,

        /**
         * Support for function {@code ST_DISTANCE}. Done in #108764.
         */
        ST_DISTANCE,

        /**
         * Fix determination of CRS types in spatial functions when folding.
         */
        SPATIAL_FUNCTIONS_FIX_CRSTYPE_FOLDING,

        /**
         * Enable spatial predicate functions to support multi-values. Done in #112063.
         */
        SPATIAL_PREDICATES_SUPPORT_MULTIVALUES,

        /**
         * Enable spatial distance function to support multi-values. Done in #114836.
         */
        SPATIAL_DISTANCE_SUPPORTS_MULTIVALUES,

        /**
         * Support a number of fixes and enhancements to spatial distance pushdown. Done in #112938.
         */
        SPATIAL_DISTANCE_PUSHDOWN_ENHANCEMENTS,

        /**
         * Fix for spatial centroid when no records are found.
         */
        SPATIAL_CENTROID_NO_RECORDS,

        /**
         * Fix to GROK and DISSECT that allows extracting attributes with the same name as the input
         * https://github.com/elastic/elasticsearch/issues/110184
         */
        GROK_DISSECT_MASKING,

        /**
         * Support for quoting index sources in double quotes.
         */
        DOUBLE_QUOTES_SOURCE_ENCLOSING,

        /**
         * Support for WEIGHTED_AVG function.
         */
        AGG_WEIGHTED_AVG,

        /**
         * Fix for union-types when aggregating over an inline conversion with casting operator. Done in #110476.
         */
        UNION_TYPES_AGG_CAST,

        /**
         * Fix to GROK validation in case of multiple fields with same name and different types
         * https://github.com/elastic/elasticsearch/issues/110533
         */
        GROK_VALIDATION,

        /**
         * Fix for union-types when aggregating over an inline conversion with conversion function. Done in #110652.
         */
        UNION_TYPES_INLINE_FIX,

        /**
         * Fix for union-types when sorting a type-casted field. We changed how we remove synthetic union-types fields.
         */
        UNION_TYPES_REMOVE_FIELDS,

        /**
         * Fix for union-types when renaming unrelated columns.
         * https://github.com/elastic/elasticsearch/issues/111452
         */
        UNION_TYPES_FIX_RENAME_RESOLUTION,

        /**
         * Fix for union-types when some indexes are missing the required field. Done in #111932.
         */
        UNION_TYPES_MISSING_FIELD,

        /**
         * Fix for widening of short numeric types in union-types. Done in #112610
         */
        UNION_TYPES_NUMERIC_WIDENING,

        /**
         * Fix a parsing issue where numbers below Long.MIN_VALUE threw an exception instead of parsing as doubles.
         * see <a href="https://github.com/elastic/elasticsearch/issues/104323"> Parsing large numbers is inconsistent #104323 </a>
         */
        FIX_PARSING_LARGE_NEGATIVE_NUMBERS,

        /**
         * Fix the status code returned when trying to run count_distinct on the _source type (which is not supported).
         * see <a href="https://github.com/elastic/elasticsearch/issues/105240">count_distinct(_source) returns a 500 response</a>
         */
        FIX_COUNT_DISTINCT_SOURCE_ERROR,

        /**
         * Use RangeQuery for BinaryComparison on DateTime fields.
         */
        RANGEQUERY_FOR_DATETIME,

        /**
         * Fix for non-unique attribute names in ROW and logical plans.
         * https://github.com/elastic/elasticsearch/issues/110541
         */
        UNIQUE_NAMES,

        /**
         * Make attributes of GROK/DISSECT adjustable and fix a shadowing bug when pushing them down past PROJECT.
         * https://github.com/elastic/elasticsearch/issues/108008
         */
        FIXED_PUSHDOWN_PAST_PROJECT,

        /**
         * Adds the {@code MV_PSERIES_WEIGHTED_SUM} function for converting sorted lists of numbers into
         * a bounded score. This is a generalization of the
         * <a href="https://en.wikipedia.org/wiki/Riemann_zeta_function">riemann zeta function</a> but we
         * don't name it that because we don't support complex numbers and don't want to make folks think
         * of mystical number theory things. This is just a weighted sum that is adjacent to magic.
         */
        MV_PSERIES_WEIGHTED_SUM,

        /**
         * Support for match operator
         */
        MATCH_OPERATOR(Build.current().isSnapshot()),

        /**
         * Removing support for the {@code META} keyword.
         */
        NO_META,

        /**
         * Add CombineBinaryComparisons rule.
         */
        COMBINE_BINARY_COMPARISONS,

        /**
         * Support for nanosecond dates as a data type
         */
        DATE_NANOS_TYPE(EsqlCorePlugin.DATE_NANOS_FEATURE_FLAG),

        /**
         * Support for to_date_nanos function
         */
        TO_DATE_NANOS(EsqlCorePlugin.DATE_NANOS_FEATURE_FLAG),

        /**
         * Support Least and Greatest functions on Date Nanos type
         */
        LEAST_GREATEST_FOR_DATENANOS(EsqlCorePlugin.DATE_NANOS_FEATURE_FLAG),

        /**
         * Support for datetime in least and greatest functions
         */
        LEAST_GREATEST_FOR_DATES,

        /**
         * Support CIDRMatch in CombineDisjunctions rule.
         */
        COMBINE_DISJUNCTIVE_CIDRMATCHES,

        /**
         * Support sending HTTP headers about the status of an async query.
         */
        ASYNC_QUERY_STATUS_HEADERS,

        /**
         * Consider the upper bound when computing the interval in BUCKET auto mode.
         */
        BUCKET_INCLUSIVE_UPPER_BOUND,

        /**
         * Changed error messages for fields with conflicting types in different indices.
         */
        SHORT_ERROR_MESSAGES_FOR_UNSUPPORTED_FIELDS,

        /**
         * Support for the whole number spans in BUCKET function.
         */
        BUCKET_WHOLE_NUMBER_AS_SPAN,

        /**
         * Allow mixed numeric types in coalesce
         */
        MIXED_NUMERIC_TYPES_IN_COALESCE,

        /**
         * Support for requesting the "SPACE" function.
         */
        SPACE,

        /**
         * Support explicit casting from string literal to DATE_PERIOD or TIME_DURATION.
         */
        CAST_STRING_LITERAL_TO_TEMPORAL_AMOUNT,

        /**
         * Supported the text categorization function "CATEGORIZE".
         */
        CATEGORIZE(Build.current().isSnapshot()),

        /**
         * QSTR function
         */
        QSTR_FUNCTION,

        /**
         * MATCH function
         */
        MATCH_FUNCTION,

        /**
         * Don't optimize CASE IS NOT NULL function by not requiring the fields to be not null as well.
         * https://github.com/elastic/elasticsearch/issues/112704
         */
        FIXED_WRONG_IS_NOT_NULL_CHECK_ON_CASE,

        /**
         * Compute year differences in full calendar years.
         */
        DATE_DIFF_YEAR_CALENDARIAL,

        /**
         * Fix sorting not allowed on _source and counters.
         */
        SORTING_ON_SOURCE_AND_COUNTERS_FORBIDDEN,

        /**
         * Allow filter per individual aggregation.
         */
        PER_AGG_FILTERING,

        /**
         * Fix {@link #PER_AGG_FILTERING} grouped by ordinals.
         */
        PER_AGG_FILTERING_ORDS,

        /**
         * Fix for https://github.com/elastic/elasticsearch/issues/114714
         */
        FIX_STATS_BY_FOLDABLE_EXPRESSION,

        /**
         * Adding stats for functions (stack telemetry)
         */
        FUNCTION_STATS,
        /**
         * Support for semantic_text field mapping
         */
        SEMANTIC_TEXT_TYPE(EsqlCorePlugin.SEMANTIC_TEXT_FEATURE_FLAG),
        /**
         * Fix for an optimization that caused wrong results
         * https://github.com/elastic/elasticsearch/issues/115281
         */
        FIX_FILTER_PUSHDOWN_PAST_STATS,

        /**
         * This enables 60_usage.yml "Basic ESQL usage....snapshot" version test. See also the next capability.
         */
        SNAPSHOT_TEST_FOR_TELEMETRY(Build.current().isSnapshot()),

        /**
         * This enables 60_usage.yml "Basic ESQL usage....non-snapshot" version test. See also the previous capability.
         */
        NON_SNAPSHOT_TEST_FOR_TELEMETRY(Build.current().isSnapshot() == false),

        /**
         * Support simplified syntax for named parameters for field and function names.
         */
        NAMED_PARAMETER_FOR_FIELD_AND_FUNCTION_NAMES_SIMPLIFIED_SYNTAX(Build.current().isSnapshot());

        private final boolean enabled;

        Cap() {
            this.enabled = true;
        };

        Cap(boolean enabled) {
            this.enabled = enabled;
        };

        Cap(FeatureFlag featureFlag) {
            this.enabled = featureFlag.isEnabled();
        }

        public boolean isEnabled() {
            return enabled;
        }

        public String capabilityName() {
            return name().toLowerCase(Locale.ROOT);
        }
    }

    public static final Set<String> CAPABILITIES = capabilities(false);

    /**
     * Get a {@link Set} of all capabilities. If the {@code all} parameter is {@code false}
     * then only <strong>enabled</strong> capabilities are returned - otherwise <strong>all</strong>
     * known capabilities are returned.
     */
    public static Set<String> capabilities(boolean all) {
        List<String> caps = new ArrayList<>();
        for (Cap cap : Cap.values()) {
            if (all || cap.isEnabled()) {
                caps.add(cap.capabilityName());
            }
        }

        /*
         * Add all of our cluster features without the leading "esql."
         */
        for (NodeFeature feature : new EsqlFeatures().getFeatures()) {
            caps.add(cap(feature));
        }
        for (NodeFeature feature : new EsqlFeatures().getHistoricalFeatures().keySet()) {
            caps.add(cap(feature));
        }
        return Set.copyOf(caps);
    }

    /**
     * Convert a {@link NodeFeature} from {@link EsqlFeatures} into a
     * capability.
     */
    public static String cap(NodeFeature feature) {
        assert feature.id().startsWith("esql.");
        return feature.id().substring("esql.".length());
    }
}
