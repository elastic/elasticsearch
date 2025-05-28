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

import static org.elasticsearch.xpack.esql.core.plugin.EsqlCorePlugin.AGGREGATE_METRIC_DOUBLE_FEATURE_FLAG;

/**
 * A {@link Set} of "capabilities" supported by the {@link RestEsqlQueryAction}
 * and {@link RestEsqlAsyncQueryAction} APIs. These are exposed over the
 * {@link RestNodesCapabilitiesAction} and we use them to enable tests.
 */
public class EsqlCapabilities {
    public enum Cap {
        /**
         * Introduction of {@code MV_SORT}, {@code MV_SLICE}, and {@code MV_ZIP}.
         * Added in #106095.
         */
        MV_SORT,

        /**
         * When we disabled some broken optimizations around {@code nullable}.
         * Fixed in #105691.
         */
        DISABLE_NULLABLE_OPTS,

        /**
         * Introduction of {@code ST_X} and {@code ST_Y}. Added in #105768.
         */
        ST_X_Y,

        /**
         * Changed precision of {@code geo_point} and {@code cartesian_point} fields, by loading from source into WKB. Done in #103691.
         */
        SPATIAL_POINTS_FROM_SOURCE,

        /**
         * Support for loading {@code geo_shape} and {@code cartesian_shape} fields. Done in #104269.
         */
        SPATIAL_SHAPES,

        /**
         * Do validation check on geo_point and geo_shape fields. Done in #128259.
         */
        GEO_VALIDATION,

        /**
         * Support for spatial aggregation {@code ST_CENTROID}. Done in #104269.
         */
        ST_CENTROID_AGG,

        /**
         * Support for spatial aggregation {@code ST_INTERSECTS}. Done in #104907.
         */
        ST_INTERSECTS,

        /**
         * Support for spatial aggregation {@code ST_CONTAINS} and {@code ST_WITHIN}. Done in #106503.
         */
        ST_CONTAINS_WITHIN,

        /**
         * Support for spatial aggregation {@code ST_DISJOINT}. Done in #107007.
         */
        ST_DISJOINT,

        /**
         * The introduction of the {@code VALUES} agg.
         */
        AGG_VALUES,

        /**
         * Expand the {@code VALUES} agg to cover spatial types.
         */
        AGG_VALUES_SPATIAL,

        /**
         * Does ESQL support async queries.
         */
        ASYNC_QUERY,

        /**
         * Does ESQL support FROM OPTIONS?
         */
        @Deprecated
        FROM_OPTIONS,

        /**
         * Cast string literals to a desired data type.
         */
        STRING_LITERAL_AUTO_CASTING,

        /**
         * Base64 encoding and decoding functions.
         */
        BASE64_DECODE_ENCODE,

        /**
         * Support for the :: casting operator
         */
        CASTING_OPERATOR,

        /**
         * Support for the ::date casting operator
         */
        CASTING_OPERATOR_FOR_DATE,

        /**
         * Blocks can be labelled with {@link org.elasticsearch.compute.data.Block.MvOrdering#SORTED_ASCENDING} for optimizations.
         */
        MV_ORDERING_SORTED_ASCENDING,

        /**
         * Support for metrics counter fields
         */
        METRICS_COUNTER_FIELDS,

        /**
         * Cast string literals to a desired data type for IN predicate and more types for BinaryComparison.
         */
        STRING_LITERAL_AUTO_CASTING_EXTENDED,
        /**
         * Support for metadata fields.
         */
        METADATA_FIELDS,

        /**
         * Support specifically for *just* the _index METADATA field. Used by CsvTests, since that is the only metadata field currently
         * supported.
         */
        INDEX_METADATA_FIELD,

        /**
         * Support for timespan units abbreviations
         */
        TIMESPAN_ABBREVIATIONS,

        /**
         * Support metrics counter types
         */
        COUNTER_TYPES,

        /**
         * Support for function {@code BIT_LENGTH}. Done in #115792
         */
        FN_BIT_LENGTH,

        /**
         * Support for function {@code BYTE_LENGTH}.
         */
        FN_BYTE_LENGTH,

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
         * Fixes on function {@code ROUND} that avoid it throwing exceptions on runtime for unsigned long cases.
         */
        FN_ROUND_UL_FIXES,

        /**
         * Support for function {@code SCALB}.
         */
        FN_SCALB,

        /**
         * Fixes for multiple functions not serializing their source, and emitting warnings with wrong line number and text.
         */
        FUNCTIONS_SOURCE_SERIALIZATION_WARNINGS,

        /**
         * All functions that take TEXT should never emit TEXT, only KEYWORD. #114334
         */
        FUNCTIONS_NEVER_EMIT_TEXT,

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
         * Support for loading values over enrich. This is supported by all versions of ESQL but not
         * the unit test CsvTests.
         */
        ENRICH_LOAD,

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
         * Support unmapped using the INSIST keyword.
         */
        UNMAPPED_FIELDS(Build.current().isSnapshot()),

        /**
         * Support for function {@code ST_DISTANCE}. Done in #108764.
         */
        ST_DISTANCE,

        /** Support for function {@code ST_EXTENT_AGG}. */
        ST_EXTENT_AGG,

        /** Optimization of ST_EXTENT_AGG with doc-values as IntBlock. */
        ST_EXTENT_AGG_DOCVALUES,

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
         * Support ST_ENVELOPE function (and related ST_XMIN, etc.).
         */
        ST_ENVELOPE,

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
         * When pushing down {@code STATS count(field::type)} for a union type field, we wrongly used a synthetic attribute name in the
         * query instead of the actual field name. This led to 0 counts instead of the correct result.
         */
        FIX_COUNT_PUSHDOWN_FOR_UNION_TYPES,

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
         * Execute `RENAME` operations sequentially from left to right,
         * see <a href="https://github.com/elastic/elasticsearch/issues/122250"> ESQL: Align RENAME behavior with EVAL for sequential processing #122250 </a>
         */
        RENAME_SEQUENTIAL_PROCESSING,

        /**
         * Support for removing empty attribute in merging output.
         * See <a href="https://github.com/elastic/elasticsearch/issues/126392"> ESQL: EVAL after STATS produces an empty column #126392 </a>
         */
        REMOVE_EMPTY_ATTRIBUTE_IN_MERGING_OUTPUT,

        /**
         * Support for retain aggregate when grouping.
         * See <a href="https://github.com/elastic/elasticsearch/issues/126026"> ES|QL: columns not projected away despite KEEP #126026 </a>
         */
        RETAIN_AGGREGATE_WHEN_GROUPING,

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
         * Fix precision of scaled_float field values retrieved from stored source
         * see <a href="https://github.com/elastic/elasticsearch/issues/122547"> Slight inconsistency in ESQL using scaled_float field #122547 </a>
         */
        FIX_PRECISION_OF_SCALED_FLOAT_FIELDS,

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
         * Enforce strict type checking on ENRICH range types, and warnings for KEYWORD parsing at runtime. Done in #115091.
         */
        ENRICH_STRICT_RANGE_TYPES,

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
         * Support for match operator as a colon. Previous support for match operator as MATCH has been removed
         */
        MATCH_OPERATOR_COLON,

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
        DATE_NANOS_TYPE(),

        /**
         * Support for to_date_nanos function
         */
        TO_DATE_NANOS(),

        /**
         * Support for date nanos type in binary comparisons
         */
        DATE_NANOS_BINARY_COMPARISON(),

        /**
         * Support for mixed comparisons between nanosecond and millisecond dates
         */
        DATE_NANOS_COMPARE_TO_MILLIS(),
        /**
         * Support implicit casting of strings to date nanos
         */
        DATE_NANOS_IMPLICIT_CASTING(),
        /**
         * Support Least and Greatest functions on Date Nanos type
         */
        LEAST_GREATEST_FOR_DATENANOS(),
        /**
         * support date extract function for date nanos
         */
        DATE_NANOS_DATE_EXTRACT(),
        /**
         * Support add and subtract on date nanos
         */
        DATE_NANOS_ADD_SUBTRACT(),
        /**
         * Support for date_trunc function on date nanos type
         */
        DATE_TRUNC_DATE_NANOS(),

        /**
         * Support date nanos values as the field argument to bucket
         */
        DATE_NANOS_BUCKET(),

        /**
         * support aggregations on date nanos
         */
        DATE_NANOS_AGGREGATIONS(),

        /**
         * Support the {@link org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.In} operator for date nanos
         */
        DATE_NANOS_IN_OPERATOR(),
        /**
         * Support running date format function on nanosecond dates
         */
        DATE_NANOS_DATE_FORMAT(),
        /**
         * support date diff function on date nanos type, and mixed nanos/millis
         */
        DATE_NANOS_DATE_DIFF(),
        /**
         * Indicates that https://github.com/elastic/elasticsearch/issues/125439 (incorrect lucene push down for date nanos) is fixed
         */
        FIX_DATE_NANOS_LUCENE_PUSHDOWN_BUG(),
        /**
         * Fixes a bug where dates are incorrectly formatted if a where clause compares nanoseconds to both milliseconds and nanoseconds,
         * e.g. {@code WHERE millis > to_datenanos("2023-10-23T12:15:03.360103847") AND millis < to_datetime("2023-10-23T13:53:55.832")}
         */
        FIX_DATE_NANOS_MIXED_RANGE_PUSHDOWN_BUG(),
        /**
         * DATE_PARSE supports reading timezones
         */
        DATE_PARSE_TZ(),

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
         * Enhanced DATE_TRUNC with arbitrary month and year intervals. (#120302)
         */
        DATE_TRUNC_WITH_ARBITRARY_INTERVALS,

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
        CATEGORIZE_V5,

        /**
         * Support for multiple groupings in "CATEGORIZE".
         */
        CATEGORIZE_MULTIPLE_GROUPINGS,
        /**
         * QSTR function
         */
        QSTR_FUNCTION,

        /**
         * MATCH function
         */
        MATCH_FUNCTION,

        /**
         * KQL function
         */
        KQL_FUNCTION,

        /**
         * Hash function
         */
        HASH_FUNCTION,
        /**
         * Hash function aliases such as MD5
         */
        HASH_FUNCTION_ALIASES_V1,

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
         * Fix {@code SORT} when the {@code _source} field is not a sort key but
         * <strong>is</strong> being returned.
         */
        SORT_RETURNING_SOURCE_OK,

        /**
         * _source field mapping directives: https://www.elastic.co/guide/en/elasticsearch/reference/current/mapping-source-field.html
         */
        SOURCE_FIELD_MAPPING,

        /**
         * Allow filter per individual aggregation.
         */
        PER_AGG_FILTERING,

        /**
         * Fix {@link #PER_AGG_FILTERING} grouped by ordinals.
         */
        PER_AGG_FILTERING_ORDS,

        /**
         * Support for {@code STD_DEV} aggregation.
         */
        STD_DEV,

        /**
         * Fix for https://github.com/elastic/elasticsearch/issues/114714
         */
        FIX_STATS_BY_FOLDABLE_EXPRESSION,

        /**
         * Adding stats for functions (stack telemetry)
         */
        FUNCTION_STATS,
        /**
         * Fix for an optimization that caused wrong results
         * https://github.com/elastic/elasticsearch/issues/115281
         */
        FIX_FILTER_PUSHDOWN_PAST_STATS,

        /**
         * Send warnings on STATS alias collision
         * https://github.com/elastic/elasticsearch/issues/114970
         */
        STATS_ALIAS_COLLISION_WARNINGS,

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
        NAMED_PARAMETER_FOR_FIELD_AND_FUNCTION_NAMES_SIMPLIFIED_SYNTAX(),

        /**
         * Fix pushdown of LIMIT past MV_EXPAND
         */
        ADD_LIMIT_INSIDE_MV_EXPAND,

        DELAY_DEBUG_FN(Build.current().isSnapshot()),

        /** Capability for remote metadata test */
        METADATA_FIELDS_REMOTE_TEST(false),
        /**
         * WIP on Join planning
         * - Introduce BinaryPlan and co
         * - Refactor INLINESTATS and LOOKUP as a JOIN block
         */
        JOIN_PLANNING_V1(Build.current().isSnapshot()),

        /**
         * Support implicit casting from string literal to DATE_PERIOD or TIME_DURATION.
         */
        IMPLICIT_CASTING_STRING_LITERAL_TO_TEMPORAL_AMOUNT,

        /**
         * LOOKUP JOIN
         */
        JOIN_LOOKUP_V12,

        /**
         * LOOKUP JOIN with TEXT fields on the right (right side of the join) (#119473)
         */
        LOOKUP_JOIN_TEXT(JOIN_LOOKUP_V12.isEnabled()),

        /**
         * LOOKUP JOIN skipping MVs and sending warnings (https://github.com/elastic/elasticsearch/issues/118780)
         */
        JOIN_LOOKUP_SKIP_MV_WARNINGS(JOIN_LOOKUP_V12.isEnabled()),

        /**
         * Fix pushing down LIMIT past LOOKUP JOIN in case of multiple matching join keys.
         */
        JOIN_LOOKUP_FIX_LIMIT_PUSHDOWN(JOIN_LOOKUP_V12.isEnabled()),

        /**
         * Fix for https://github.com/elastic/elasticsearch/issues/117054
         */
        FIX_NESTED_FIELDS_NAME_CLASH_IN_INDEXRESOLVER,

        /**
         * Fix for https://github.com/elastic/elasticsearch/issues/114714, again
         */
        FIX_STATS_BY_FOLDABLE_EXPRESSION_2,

        /**
         * Support the "METADATA _score" directive to enable _score column.
         */
        METADATA_SCORE,

        /**
         * Term function
         */
        TERM_FUNCTION(Build.current().isSnapshot()),

        /**
         * Additional types for match function and operator
         */
        MATCH_ADDITIONAL_TYPES,

        /**
         * Fix for regex folding with case-insensitive pattern https://github.com/elastic/elasticsearch/issues/118371
         */
        FIXED_REGEX_FOLD,

        /**
         * Full text functions can be used in disjunctions
         */
        FULL_TEXT_FUNCTIONS_DISJUNCTIONS,

        /**
         * Change field caps response for semantic_text fields to be reported as text
         */
        SEMANTIC_TEXT_FIELD_CAPS,

        /**
         * Support named argument for function in map format.
         */
        OPTIONAL_NAMED_ARGUMENT_MAP_FOR_FUNCTION(Build.current().isSnapshot()),

        /**
         * Disabled support for index aliases in lookup joins
         */
        LOOKUP_JOIN_NO_ALIASES(JOIN_LOOKUP_V12.isEnabled()),

        /**
         * Full text functions can be used in disjunctions as they are implemented in compute engine
         */
        FULL_TEXT_FUNCTIONS_DISJUNCTIONS_COMPUTE_ENGINE,

        /**
         * Support match options in match function
         */
        MATCH_FUNCTION_OPTIONS,

        /**
         * Support options in the query string function.
         */
        QUERY_STRING_FUNCTION_OPTIONS,

        /**
         * Support for aggregate_metric_double type
         */
        AGGREGATE_METRIC_DOUBLE(AGGREGATE_METRIC_DOUBLE_FEATURE_FLAG),

        /**
         * Support for partial subset of metrics in aggregate_metric_double type
         */
        AGGREGATE_METRIC_DOUBLE_PARTIAL_SUBMETRICS(AGGREGATE_METRIC_DOUBLE_FEATURE_FLAG),

        /**
         * Support change point detection "CHANGE_POINT".
         */
        CHANGE_POINT,

        /**
         * Fix for https://github.com/elastic/elasticsearch/issues/120817
         * and https://github.com/elastic/elasticsearch/issues/120803
         * Support for queries that have multiple SORTs that cannot become TopN
         */
        REMOVE_REDUNDANT_SORT,

        /**
         * Fixes a series of issues with inlinestats which had an incomplete implementation after lookup and inlinestats
         * were refactored.
         */
        INLINESTATS_V7(EsqlPlugin.INLINESTATS_FEATURE_FLAG),

        /**
         * Support partial_results
         */
        SUPPORT_PARTIAL_RESULTS,

        /**
         * Support for rendering aggregate_metric_double type
         */
        AGGREGATE_METRIC_DOUBLE_RENDERING(AGGREGATE_METRIC_DOUBLE_FEATURE_FLAG),

        /**
         * Support for FORK command
         */
        FORK(Build.current().isSnapshot()),

        /**
         * Support for RERANK command
         */
        RERANK(Build.current().isSnapshot()),

        /**
         * Support for COMPLETION command
         */
        COMPLETION(Build.current().isSnapshot()),

        /**
         * Allow mixed numeric types in conditional functions - case, greatest and least
         */
        MIXED_NUMERIC_TYPES_IN_CASE_GREATEST_LEAST,

        /**
         * Support for RRF command
         */
        RRF(Build.current().isSnapshot()),

        /**
         * Lucene query pushdown to StartsWith and EndsWith functions.
         * This capability was created to avoid receiving wrong warnings from old nodes in mixed clusters
         */
        STARTS_WITH_ENDS_WITH_LUCENE_PUSHDOWN,

        /**
         * Full text functions can be scored when being part of a disjunction
         */
        FULL_TEXT_FUNCTIONS_DISJUNCTIONS_SCORE,

        /**
         * Support for multi-match function.
         */
        MULTI_MATCH_FUNCTION(Build.current().isSnapshot()),

        /**
         * Do {@code TO_LOWER} and {@code TO_UPPER} process all field values?
         */
        TO_LOWER_MV,

        /**
         * Use double parameter markers to represent field or function names.
         */
        DOUBLE_PARAMETER_MARKERS_FOR_IDENTIFIERS,

        /**
         * Non full text functions do not contribute to score
         */
        NON_FULL_TEXT_FUNCTIONS_SCORING,

        /**
         * Support for to_aggregate_metric_double function
         */
        AGGREGATE_METRIC_DOUBLE_CONVERT_TO(AGGREGATE_METRIC_DOUBLE_FEATURE_FLAG),

        /**
         * The {@code _query} API now reports the original types.
         */
        REPORT_ORIGINAL_TYPES,

        /**
         * The metrics command
         */
        METRICS_COMMAND(Build.current().isSnapshot()),

        /**
         * Are the {@code documents_found} and {@code values_loaded} fields available
         * in the response and profile?
         */
        DOCUMENTS_FOUND_AND_VALUES_LOADED,

        /**
         * Index component selector syntax (my-data-stream-name::failures)
         */
        INDEX_COMPONENT_SELECTORS,

        /**
         * Make numberOfChannels consistent with layout in DefaultLayout by removing duplicated ChannelSet.
         */
        MAKE_NUMBER_OF_CHANNELS_CONSISTENT_WITH_LAYOUT,

        /**
         * Support for sorting when aggregate_metric_doubles are present
         */
        AGGREGATE_METRIC_DOUBLE_SORTING(AGGREGATE_METRIC_DOUBLE_FEATURE_FLAG),

        /**
         * Supercedes {@link Cap#MAKE_NUMBER_OF_CHANNELS_CONSISTENT_WITH_LAYOUT}.
         */
        FIX_REPLACE_MISSING_FIELD_WITH_NULL_DUPLICATE_NAME_ID_IN_LAYOUT,

        /**
         * Support for filter in converted null.
         * See <a href="https://github.com/elastic/elasticsearch/issues/125832"> ESQL: Fix `NULL` handling in `IN` clause #125832 </a>
         */
        FILTER_IN_CONVERTED_NULL,

        /**
         * When creating constant null blocks in {@link org.elasticsearch.compute.lucene.ValuesSourceReaderOperator}, we also handed off
         * the ownership of that block - but didn't account for the fact that the caller might close it, leading to double releases
         * in some union type queries. C.f. https://github.com/elastic/elasticsearch/issues/125850
         */
        FIX_DOUBLY_RELEASED_NULL_BLOCKS_IN_VALUESOURCEREADER,

        /**
         * Listing queries and getting information on a specific query.
         */
        QUERY_MONITORING,

        /**
         * Support max_over_time aggregation that gets evaluated per time-series
         */
        MAX_OVER_TIME(Build.current().isSnapshot()),

        /**
         * Support streaming of sub plan results
         */
        FORK_V5(Build.current().isSnapshot()),

        /**
         * Support for the {@code leading_zeros} named parameter.
         */
        TO_IP_LEADING_ZEROS,

        /**
         * Does the usage information for ESQL contain a histogram of {@code took} values?
         */
        USAGE_CONTAINS_TOOK,

        /**
         * Support avg_over_time aggregation that gets evaluated per time-series
         */
        AVG_OVER_TIME(Build.current().isSnapshot()),

        /**
         * Support loading of ip fields if they are not indexed.
         */
        LOADING_NON_INDEXED_IP_FIELDS,

        /**
         * During resolution (pre-analysis) we have to consider that joins or enriches can override EVALuated values
         * https://github.com/elastic/elasticsearch/issues/126419
         */
        FIX_JOIN_MASKING_EVAL,

        /**
         * Support for keeping `DROP` attributes when resolving field names.
         * see <a href="https://github.com/elastic/elasticsearch/issues/126418"> ES|QL: no matches for pattern #126418 </a>
         */
        DROP_AGAIN_WITH_WILDCARD_AFTER_EVAL,

        /**
         * Support last_over_time aggregation that gets evaluated per time-series
         */
        LAST_OVER_TIME(Build.current().isSnapshot()),

        /**
         * Support for the SAMPLE command
         */
        SAMPLE(Build.current().isSnapshot()),

        /**
         * The {@code _query} API now gives a cast recommendation if multiple types are found in certain instances.
         */
        SUGGESTED_CAST,

        /**
         * Guards a bug fix matching {@code TO_LOWER(f) == ""}.
         */
        TO_LOWER_EMPTY_STRING,

        /**
         * Support min_over_time aggregation that gets evaluated per time-series
         */
        MIN_OVER_TIME(Build.current().isSnapshot()),

        /**
         * Support first_over_time aggregation that gets evaluated per time-series
         */
        FIRST_OVER_TIME(Build.current().isSnapshot()),

        /**
         * Support sum_over_time aggregation that gets evaluated per time-series
         */
        SUM_OVER_TIME(Build.current().isSnapshot()),

        /**
         * Resolve groupings before resolving references to groupings in the aggregations.
         */
        RESOLVE_GROUPINGS_BEFORE_RESOLVING_REFERENCES_TO_GROUPINGS_IN_AGGREGATIONS,

        /**
         * Support for the SAMPLE aggregation function
         */
        AGG_SAMPLE,

        /**
         * Full text functions in STATS
         */
        FULL_TEXT_FUNCTIONS_IN_STATS_WHERE,

        /**
         * During resolution (pre-analysis) we have to consider that joins can override regex extracted values
         * see <a href="https://github.com/elastic/elasticsearch/issues/127467"> ES|QL: pruning of JOINs leads to missing fields #127467 </a>
         */
        FIX_JOIN_MASKING_REGEX_EXTRACT,

        /**
         * Avid GROK and DISSECT attributes being removed when resolving fields.
         * see <a href="https://github.com/elastic/elasticsearch/issues/127468"> ES|QL: Grok only supports KEYWORD or TEXT values, found expression [type] type [INTEGER] #127468 </a>
         */
        KEEP_REGEX_EXTRACT_ATTRIBUTES,

        /**
         * The {@code ROUND_TO} function.
         */
        ROUND_TO,

        /**
         * Allow lookup join on mixed numeric fields, among byte, short, int, long, half_float, scaled_float, float and double.
         */
        LOOKUP_JOIN_ON_MIXED_NUMERIC_FIELDS,

        /**
         * {@link org.elasticsearch.compute.lucene.LuceneQueryEvaluator} rewrites the query before executing it in Lucene. This
         * provides support for KQL in a STATS ... BY command that uses a KQL query for filter, for example.
         */
        LUCENE_QUERY_EVALUATOR_QUERY_REWRITE,

        /**
         * Support parameters for LiMIT command.
         */
        PARAMETER_FOR_LIMIT,

        /**
         * Dense vector field type support
         */
        DENSE_VECTOR_FIELD_TYPE(EsqlCorePlugin.DENSE_VECTOR_FEATURE_FLAG),

        /**
         * Enable support for index aliases in lookup joins
         */
        ENABLE_LOOKUP_JOIN_ON_ALIASES(JOIN_LOOKUP_V12.isEnabled());

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
