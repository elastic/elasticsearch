/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.Build;
import org.elasticsearch.common.util.FeatureFlag;
import org.elasticsearch.compute.lucene.read.ValuesSourceReaderOperator;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.rest.action.admin.cluster.RestNodesCapabilitiesAction;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.ReplaceStatsFilteredOrNullAggWithEval;
import org.elasticsearch.xpack.esql.plugin.EsqlFeatures;

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
         * Quantize results of {@code ST_X} and {@code ST_Y} and related functions
         */
        ST_X_Y_QUANTIZED,

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
         * Fold in spatial functions should return null for null input.
         */
        GEO_NULL_LITERALS_FOLDING,

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
         * Support for spatial simplification {@code ST_SIMPLIFY}
         */
        ST_SIMPLIFY,

        /**
         * The introduction of the {@code VALUES} agg.
         */
        AGG_VALUES,

        /**
         * Expand the {@code VALUES} agg to cover spatial types.
         */
        AGG_VALUES_SPATIAL,

        /**
         * Accept unsigned longs on MAX and MIN aggregations.
         */
        AGG_MAX_MIN_UNSIGNED_LONG,

        /**
         * Accept unsigned longs on VALUES and SAMPLE aggregations.
         */
        AGG_VALUES_SAMPLE_UNSIGNED_LONG,

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
         * Support for optional fields (might or might not be present in the mappings) using FAIL/NULLIFY/LOAD
         */
        OPTIONAL_FIELDS(Build.current().isSnapshot()),

        /**
         * Support for Optional fields (might or might not be present in the mappings) using FAIL/NULLIFY only. This is a temporary
         * capability until we enable the LOAD option mentioned above.
         */
        OPTIONAL_FIELDS_NULLIFY_TECH_PREVIEW,

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
         * Support for function {@code CONTAINS}. Done in <a href="https://github.com/elastic/elasticsearch/pull/133016">#133016.</a>
         */
        FN_CONTAINS,

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
         * Fix a bug leading to the scratch leaking data to other rows.
         */
        FN_IP_PREFIX_FIX_DIRTY_SCRATCH_LEAK,

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
         * Support for function DAY_NAME
         */
        FN_DAY_NAME,

        /**
         * Support for function MONTH_NAME
         */
        FN_MONTH_NAME,

        /**
         * support for MV_CONTAINS function
         * <a href="https://github.com/elastic/elasticsearch/pull/133099/">Add MV_CONTAINS function #133099</a>
         */
        FN_MV_CONTAINS_V1,

        /**
         * support for MV_INTERSECTS function
         */
        FN_MV_INTERSECTS,

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
        INLINESTATS(),

        /**
         * Support for the expressions in grouping in {@code INLINESTATS} syntax.
         */
        INLINESTATS_V2(),

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
         * Make optional the order field in the TOP agg command, and default it to "ASC".
         */
        AGG_TOP_WITH_OPTIONAL_ORDER_FIELD,

        /**
         * Support for the extra "map" field in {@code TOP} aggregation.
         */
        AGG_TOP_WITH_OUTPUT_FIELD,

        /**
         * Fix for a bug when surrogating a {@code TOP}  with limit 1 and output field.
         */
        FIX_AGG_TOP_WITH_OUTPUT_FIELD_SURROGATE,

        /**
         * {@code CASE} properly handling multivalue conditions.
         */
        CASE_MV,

        /**
         * {@code CASE} folding with DATE_PERIOD and TIME_DURATION return types.
         */
        CASE_FOLD_TEMPORAL_AMOUNT,

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
         * Support implicit casting for union typed fields that are mixed with date and date_nanos type.
         */
        IMPLICIT_CASTING_DATE_AND_DATE_NANOS,

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
         * Fix ST_ENVELOPE to support multi-values and doc-values.
         */
        ST_ENVELOPE_MV_FIX,

        /**
         * Support ST_NPOINTS function.
         */
        ST_NPOINTS,

        /**
         * Support ST_GEOHASH, ST_GEOTILE and ST_GEOHEX functions
         */
        SPATIAL_GRID,

        /**
         * Support geohash, geotile and geohex data types. Done in #129581
         */
        SPATIAL_GRID_TYPES,

        /**
         * Support geohash, geotile and geohex in ST_INTERSECTS and ST_DISJOINT. Done in #133546
         */
        SPATIAL_GRID_INTERSECTS,

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
         * Support for assignment in RENAME, besides the use of `AS` keyword.
         */
        RENAME_ALLOW_ASSIGNMENT,

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
         * Fix for resolving union type casts past projections (KEEP) and MV_EXPAND operations.
         * Ensures that casting a union type field works correctly when the field has been projected
         * and expanded through MV_EXPAND. See #137923
         */
        UNION_TYPES_RESOLVE_PAST_PROJECTIONS,

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
         * When resolving renames, consider all {@code Attribute}s in the plan, not just the {@code ReferenceAttribute}s.
         */
        FIXED_PUSHDOWN_PAST_PROJECT_WITH_ATTRIBUTES_RESOLUTION,

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
         * Support for date nanos in lookup join. Done in #127962
         */
        DATE_NANOS_LOOKUP_JOIN,

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
         * Fix async headers not being sent on "get" requests
         */
        ASYNC_QUERY_STATUS_HEADERS_FIX,

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
        CATEGORIZE_V6,

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
         * Support for optional parameters in KQL function (case_insensitive, time_zone, default_field, boost).
         */
        KQL_FUNCTION_OPTIONS,

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
        SNAPSHOT_TEST_FOR_TELEMETRY_V2(Build.current().isSnapshot()),

        /**
         * This enables 60_usage.yml "Basic ESQL usage....non-snapshot" version test. See also the previous capability.
         */
        NON_SNAPSHOT_TEST_FOR_TELEMETRY_V2(Build.current().isSnapshot() == false),

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
         * Fix for async operator sometimes completing the driver without emitting the stored warnings
         */
        ASYNC_OPERATOR_WARNINGS_FIX,

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
         * Enable aggregate_metric_double in non-snapshot builds
         */
        AGGREGATE_METRIC_DOUBLE_V0,

        /**
         * Support running all aggregations on aggregate_metric_double using the default metric
         */
        AGGREGATE_METRIC_DOUBLE_DEFAULT_METRIC,

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
        INLINESTATS_V11,

        /**
         * Renamed `INLINESTATS` to `INLINE STATS`.
         */
        INLINE_STATS,

        /**
         * Added support for having INLINE STATS preceded by a SORT clause, now executable in certain cases.
         */
        INLINE_STATS_PRECEEDED_BY_SORT,

        /**
         * Support partial_results
         */
        SUPPORT_PARTIAL_RESULTS,

        /**
         * Support for RERANK command
         */
        RERANK,

        /**
         * Support for COMPLETION command
         */
        COMPLETION,

        /**
         * Allow mixed numeric types in conditional functions - case, greatest and least
         */
        MIXED_NUMERIC_TYPES_IN_CASE_GREATEST_LEAST,

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
         * Does {@code CHUNK} process all field values?
         */
        CHUNK_MV,

        /**
         * Use double parameter markers to represent field or function names.
         */
        DOUBLE_PARAMETER_MARKERS_FOR_IDENTIFIERS,

        /**
         * Non full text functions do not contribute to score
         */
        NON_FULL_TEXT_FUNCTIONS_SCORING,

        /**
         * The {@code _query} API now reports the original types.
         */
        REPORT_ORIGINAL_TYPES,

        /**
         * The metrics command
         */
        @Deprecated
        METRICS_COMMAND(Build.current().isSnapshot()),
        /**
         * Enables automatically grouping by all dimension fields in TS mode queries
         */
        METRICS_GROUP_BY_ALL(),

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
         * Supercedes {@link Cap#MAKE_NUMBER_OF_CHANNELS_CONSISTENT_WITH_LAYOUT}.
         */
        FIX_REPLACE_MISSING_FIELD_WITH_NULL_DUPLICATE_NAME_ID_IN_LAYOUT,

        /**
         * Support for filter in converted null.
         * See <a href="https://github.com/elastic/elasticsearch/issues/125832"> ESQL: Fix `NULL` handling in `IN` clause #125832 </a>
         */
        FILTER_IN_CONVERTED_NULL,

        /**
         * When creating constant null blocks in {@link ValuesSourceReaderOperator}, we also handed off
         * the ownership of that block - but didn't account for the fact that the caller might close it, leading to double releases
         * in some union type queries. C.f. https://github.com/elastic/elasticsearch/issues/125850
         */
        FIX_DOUBLY_RELEASED_NULL_BLOCKS_IN_VALUESOURCEREADER,

        /**
         * Listing queries and getting information on a specific query.
         */
        QUERY_MONITORING,

        /**
         * Support for FORK out of snapshot
         */
        FORK_V9,

        /**
         * Support for union types in FORK
         */
        FORK_UNION_TYPES,

        /**
         * Support non-correlated subqueries in the FROM clause.
         */
        SUBQUERY_IN_FROM_COMMAND(Build.current().isSnapshot()),

        /**
         * Support non-correlated subqueries in the FROM clause without implicit limit.
         */
        SUBQUERY_IN_FROM_COMMAND_WITHOUT_IMPLICIT_LIMIT(Build.current().isSnapshot()),

        /**
         * Append an implicit limit to unbounded sorts in subqueries in the FROM clause.
         */
        SUBQUERY_IN_FROM_COMMAND_APPEND_IMPLICIT_LIMIT_TO_UNBOUNDED_SORT_IN_SUBQUERY(Build.current().isSnapshot()),

        /**
         * Support for views in cluster state (and REST API).
         */
        VIEWS_IN_CLUSTER_STATE(EsqlFeatures.ESQL_VIEWS_FEATURE_FLAG.isEnabled()),

        /**
         * Basic Views with no branching (do not need subqueries or FORK).
         */
        VIEWS_WITH_NO_BRANCHING(VIEWS_IN_CLUSTER_STATE.isEnabled()),

        /**
         * Views with branching (requires subqueries/FORK).
         */
        VIEWS_WITH_BRANCHING(VIEWS_WITH_NO_BRANCHING.isEnabled() && SUBQUERY_IN_FROM_COMMAND.isEnabled()),

        /**
         * Support for the {@code leading_zeros} named parameter.
         */
        TO_IP_LEADING_ZEROS,

        /**
         * Does the usage information for ESQL contain a histogram of {@code took} values?
         */
        USAGE_CONTAINS_TOOK,

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
         * Correctly ask for all fields from lookup indices even when there is e.g. a {@code DROP *field} after.
         * See <a href="https://github.com/elastic/elasticsearch/issues/129561">
         *     ES|QL: missing columns for wildcard drop after lookup join  #129561</a>
         */
        DROP_WITH_WILDCARD_AFTER_LOOKUP_JOIN,

        /**
         * score function
         */
        SCORE_FUNCTION,

        /**
         * Support for the SAMPLE command
         */
        SAMPLE_V3,

        /**
         * The {@code _query} API now gives a cast recommendation if multiple types are found in certain instances.
         */
        SUGGESTED_CAST,

        /**
         * Guards a bug fix matching {@code TO_LOWER(f) == ""}.
         */
        TO_LOWER_EMPTY_STRING,

        /**
         * Support for INCREASE, DELTA timeseries aggregations.
         */
        INCREASE,
        DELTA_TS_AGG,
        CLAMP_FUNCTIONS,

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
         * Allow the merging of the children to use {@code Aliase}s, instead of just {@code ReferenceAttribute}s.
         */
        FIX_JOIN_OUTPUT_MERGING,

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
         * Support for the {@code COPY_SIGN} function.
         */
        COPY_SIGN,

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
         * Support parameters for LIMIT command.
         */
        PARAMETER_FOR_LIMIT,

        /**
         * Changed and normalized the LIMIT error message.
         */
        NORMALIZED_LIMIT_ERROR_MESSAGE,

        /**
         * Dense vector field type support
         */
        DENSE_VECTOR_FIELD_TYPE_RELEASED,

        /**
         * Enable support for index aliases in lookup joins
         */
        ENABLE_LOOKUP_JOIN_ON_ALIASES,

        /**
         * Lookup error messages were updated to make them a bit easier to understand.
         */
        UPDATE_LOOKUP_JOIN_ERROR_MESSAGES,

        /**
         * Allows RLIKE to correctly handle the "empty language" flag, `#`.
         */
        RLIKE_WITH_EMPTY_LANGUAGE_PATTERN,

        /**
         * Enable support for cross-cluster lookup joins.
         */
        ENABLE_LOOKUP_JOIN_ON_REMOTE,

        /**
         * Fix the planning of {@code | ENRICH _remote:policy} when there's a preceding {@code | LOOKUP JOIN},
         * see <a href="https://github.com/elastic/elasticsearch/issues/129372">java.lang.ClassCastException when combining LOOKUP JOIN and remote ENRICH</a>
         */
        REMOTE_ENRICH_AFTER_LOOKUP_JOIN,

        /**
         * MATCH PHRASE function
         */
        MATCH_PHRASE_FUNCTION,

        /**
         * Support knn function
         */
        KNN_FUNCTION_V5,

        /**
         * Support for the {@code TEXT_EMBEDDING} function for generating dense vector embeddings.
         */
        TEXT_EMBEDDING_FUNCTION,

        /**
         * Support for the LIKE operator with a list of wildcards.
         */
        LIKE_WITH_LIST_OF_PATTERNS,

        LIKE_LIST_ON_INDEX_FIELDS,

        /**
         * Support parameters for SAMPLE command.
         */
        PARAMETER_FOR_SAMPLE,

        /**
         * From now, Literal only accepts strings as BytesRefs.
         * No java.lang.String anymore.
         *
         * https://github.com/elastic/elasticsearch/issues/129322
         */
        NO_PLAIN_STRINGS_IN_LITERALS,

        /**
         * Support for the mv_expand target attribute should be retained in its original position.
         * see <a href="https://github.com/elastic/elasticsearch/issues/129000"> ES|QL: inconsistent column order #129000 </a>
         */
        FIX_MV_EXPAND_INCONSISTENT_COLUMN_ORDER,

        /**
         * Support for the SET command.
         */
        SET_COMMAND,

        /**
         * Support timezones in DATE_TRUNC and dependent functions.
         */
        DATE_TRUNC_TIMEZONE_SUPPORT,

        /**
         * Support timezones in DATE_DIFF.
         */
        DATE_DIFF_TIMEZONE_SUPPORT,

        /**
         * Support timezones in KQL and QSTR.
         */
        KQL_QSTR_TIMEZONE_SUPPORT,

        /**
         * Support timezones in the conversion utils and functions, like TO_STRING.
         */
        TYPE_CONVERSION_TIMEZONE_SUPPORT,

        /**
         * Support timezones in DATE_FORMAT and DATE_PARSE.
         */
        DATE_FORMAT_DATE_PARSE_TIMEZONE_SUPPORT,

        /**
         * Support timezones in + and - operators.
         */
        ADD_SUB_OPERATOR_TIMEZONE_SUPPORT,

        /**
         * (Re)Added EXPLAIN command
         */
        EXPLAIN(Build.current().isSnapshot()),
        /**
         * Support for the RLIKE operator with a list of regexes.
         */
        RLIKE_WITH_LIST_OF_PATTERNS,

        /**
         * FUSE command
         */
        FUSE_V6,

        /**
         * Support improved behavior for LIKE operator when used with index fields.
         */
        LIKE_ON_INDEX_FIELDS,

        /**
         * Forbid usage of brackets in unquoted index and enrich policy names
         * https://github.com/elastic/elasticsearch/issues/130378
         */
        NO_BRACKETS_IN_UNQUOTED_INDEX_NAMES,

        /**
         * Cosine vector similarity function
         */
        COSINE_VECTOR_SIMILARITY_FUNCTION,

        /**
         * Fixed some profile serialization issues
         */
        FIXED_PROFILE_SERIALIZATION,

        /**
         * Support for lookup join on multiple fields.
         */
        LOOKUP_JOIN_ON_MULTIPLE_FIELDS,
        /**
         * Dot product vector similarity function
         */
        DOT_PRODUCT_VECTOR_SIMILARITY_FUNCTION,

        /**
         * l1 norm vector similarity function
         */
        L1_NORM_VECTOR_SIMILARITY_FUNCTION,

        /**
         * l2 norm vector similarity function
         */
        L2_NORM_VECTOR_SIMILARITY_FUNCTION,

        /**
         * Support for the options field of CATEGORIZE.
         */
        CATEGORIZE_OPTIONS,

        /**
         * Decay function for custom scoring
         */
        DECAY_FUNCTION,

        /**
         * Support correct counting of skipped shards.
         */
        CORRECT_SKIPPED_SHARDS_COUNT,

        /*
         * Support for calculating the scalar vector magnitude.
         */
        MAGNITUDE_SCALAR_VECTOR_FUNCTION(Build.current().isSnapshot()),

        /**
         * Byte elements dense vector field type support.
         */
        DENSE_VECTOR_FIELD_TYPE_BYTE_ELEMENTS,

        /**
         * Bit elements dense vector field type support.
         */
        DENSE_VECTOR_FIELD_TYPE_BIT_ELEMENTS,

        /**
         * Support directIO rescoring and `bfloat16` for `bbq_hnsw` and `bbq_disk`, and `bfloat16` for `hnsw` ans `bbq_flat` index types.
         */
        GENERIC_VECTOR_FORMAT,

        /**
         * Support null elements on vector similarity functions
         */
        VECTOR_SIMILARITY_FUNCTIONS_SUPPORT_NULL,

        /**
         * Support for vector Hamming distance.
         */
        HAMMING_VECTOR_SIMILARITY_FUNCTION,

        /**
         * Support for tbucket function
         */
        TBUCKET,

        /**
         * Allow qualifiers in attribute names.
         */
        NAME_QUALIFIERS(Build.current().isSnapshot()),

        /**
         * URL encoding function.
         */
        URL_ENCODE(),

        /**
         * URL component encoding function.
         */
        URL_ENCODE_COMPONENT(),

        /**
         * URL decoding function.
         */
        URL_DECODE(),

        /**
         * Allow lookup join on boolean expressions
         */
        LOOKUP_JOIN_ON_BOOLEAN_EXPRESSION,
        /**
         * Lookup join with Full Text Function or other Lucene Pushable condition
         * to be applied to the lookup index used
         */
        LOOKUP_JOIN_WITH_FULL_TEXT_FUNCTION,
        /**
         * Bugfix for lookup join with Full Text Function
         */
        LOOKUP_JOIN_WITH_FULL_TEXT_FUNCTION_BUGFIX,
        /**
         * FORK with remote indices
         */
        ENABLE_FORK_FOR_REMOTE_INDICES_V2,

        /**
         * Support for the Present function
         */
        FN_PRESENT,

        /**
         * Bugfix for STATS {{expression}} WHERE {{condition}} when the
         * expression is replaced by something else on planning
         * e.g. STATS SUM(1) WHERE x==3 is replaced by
         *      STATS MV_SUM(const)*COUNT(*) WHERE x == 3.
         */
        STATS_WITH_FILTERED_SURROGATE_FIXED,

        /**
         * TO_DENSE_VECTOR function.
         */
        TO_DENSE_VECTOR_FUNCTION,

        /**
         * Multivalued query parameters
         */
        QUERY_PARAMS_MULTI_VALUES(),

        FIX_PERCENTILE_PRECISION(),

        /**
         * Support for the Absent function
         */
        FN_ABSENT,

        /** INLINE STATS supports remote indices */
        INLINE_STATS_SUPPORTS_REMOTE(INLINESTATS_V11.enabled),

        INLINE_STATS_WITH_UNION_TYPES_IN_STUB_RELATION(INLINE_STATS.enabled),

        /**
         * Support TS command in non-snapshot builds
         */
        TS_COMMAND_V0(),

        /**
         * Custom error for renamed timestamp
         */
        TS_RENAME_TIMESTAMP_ERROR_MESSAGE,
        /**
         * Add support for counter doubles, ints, and longs in first_ and last_over_time
         */
        FIRST_LAST_OVER_TIME_COUNTER_SUPPORT,

        FIX_ALIAS_ID_WHEN_DROP_ALL_AGGREGATES,

        /**
         * Percentile over time and other ts-aggregations
         */
        PERCENTILE_OVER_TIME,
        VARIANCE_STDDEV_OVER_TIME,
        TS_LINREG_DERIVATIVE,
        TS_RATE_DATENANOS,
        TS_RATE_DATENANOS_2,
        TS_DERIV_DATENANOS,

        /**
         * Rate and increase calculations use interpolation at the boundaries between time buckets
         */
        RATE_WITH_INTERPOLATION,
        RATE_WITH_INTERPOLATION_V2,

        /**
         * INLINE STATS fix incorrect prunning of null filtering
         * https://github.com/elastic/elasticsearch/pull/135011
         */
        INLINE_STATS_FIX_PRUNING_NULL_FILTER(INLINESTATS_V11.enabled),

        INLINE_STATS_FIX_OPTIMIZED_AS_LOCAL_RELATION(INLINESTATS_V11.enabled),

        DENSE_VECTOR_AGG_METRIC_DOUBLE_IF_FNS,

        DENSE_VECTOR_AGG_METRIC_DOUBLE_IF_VERSION,

        /**
         * FUSE L2_NORM score normalization support
         */
        FUSE_L2_NORM(Build.current().isSnapshot()),

        /**
         * Support for requesting the "_tsid" metadata field.
         */
        METADATA_TSID_FIELD,

        /**
         * Permit the data type of a field changing from TEXT to KEYWORD
         * when being grouped on in aggregations on the TS command.
         */
        TS_PERMIT_TEXT_BECOMING_KEYWORD_WHEN_GROUPED_ON,

        /**
         * Fix for a bug where if you queried multiple TS indices with a field
         * mapped to different types, the original types/suggested cast sections
         * of the return result would be empty.
         */
        TS_ORIGINAL_TYPES_BUG_FIXED,

        /**
         * Fix management of plans with no columns
         * https://github.com/elastic/elasticsearch/issues/120272
         */
        FIX_NO_COLUMNS,

        /**
         * Support for dots in FUSE attributes
         */
        DOTS_IN_FUSE,

        /**
         * Support for the DATE_RANGE field type.
         */
        DATE_RANGE_FIELD_TYPE(Build.current().isSnapshot()),

        /**
         * Network direction function.
         */
        NETWORK_DIRECTION(Build.current().isSnapshot()),

        /**
         * Support for the literal {@code m} suffix as an alias for {@code minute} in temporal amounts.
        */
        TEMPORAL_AMOUNT_M,

        /**
         * Pack dimension values in TS command
         */
        PACK_DIMENSIONS_IN_TS,

        /**
         * Support for exponential_histogram fields in the state of when it first was released into tech preview.
         */
        EXPONENTIAL_HISTOGRAM_TECH_PREVIEW,

        /**
         * Support for the T-Digest elasticsearch field mapper and ES|QL type when they were released into tech preview.
         */
        TDIGEST_TECH_PREVIEW,

        /**
         * Adds the ability for the {@link org.elasticsearch.xpack.esql.expression.function.scalar.conditional.Case}
         * to return values of type TDIGEST, type HISTOGRAM, and type AGGREGATE_METRIC_DOUBLE.
         */
        CASE_SUPPORT_FOR_SUMMARY_FIELDS,

        /**
         * Histogram field integration
         */
        HISTOGRAM_RELEASE_VERSION,

        /**
         * Support for running the Count aggregation on t-digest and exponential histogram types
         */
        COUNT_OF_HISTOGRAM_TYPES,
        /**
         * Fix for <a href="https://github.com/elastic/elasticsearch/issues/140670">140670</a>,
         * this allows for type conversion functions with no further computation to be
         * evaluated inside default wrapping _over_time functions.
         */
        ALLOW_CASTING_IN_DEFAULT_TS_AGGS,
        /**
         * Create new block when filtering OrdinalBytesRefBlock
         */
        FIX_FILTER_ORDINALS,

        /**
         * "time_zone" parameter in request body and in {@code SET time_zone="x"}.
         * <p>
         *     Originally `GLOBAL_TIMEZONE_PARAMETER`, but changed to "_WITH_OUTPUT" so tests don't fail after formatting the _query output.
         * </p>
         */
        GLOBAL_TIMEZONE_PARAMETER_WITH_OUTPUT(Build.current().isSnapshot()),

        /**
         * Optional options argument for DATE_PARSE
         */
        DATE_PARSE_OPTIONS,

        /**
         * Allow multiple patterns for GROK command
         */
        GROK_MULTI_PATTERN,

        /**
         * Fix pruning of columns when shadowed in INLINE STATS
         */
        INLINE_STATS_PRUNE_COLUMN_FIX(INLINESTATS.enabled),

        /**
         * Fix double release in inline stats when LocalRelation is reused
         */
        INLINE_STATS_DOUBLE_RELEASE_FIX(INLINESTATS_V11.enabled),

        /**
         * Support for pushing down EVAL with SCORE
         * https://github.com/elastic/elasticsearch/issues/133462
         */
        PUSHING_DOWN_EVAL_WITH_SCORE,

        /**
         * Fix for ClassCastException in STATS
         * https://github.com/elastic/elasticsearch/issues/133992
         * https://github.com/elastic/elasticsearch/issues/136598
         */
        FIX_STATS_CLASSCAST_EXCEPTION,

        /**
         * Fix attribute equality to respect the name id of the attribute.
         */
        ATTRIBUTE_EQUALS_RESPECTS_NAME_ID,

        /**
         * Fix for lookup join filter pushdown not using semantic equality.
         * This prevents duplicate filters from being pushed down when they are semantically equivalent, causing an infinite loop where
         * BooleanSimplification will simplify the original and duplicate filters, so they'll be pushed down again...
         */
        LOOKUP_JOIN_SEMANTIC_FILTER_DEDUP,

        /**
         * Warning when SORT is followed by LOOKUP JOIN which does not preserve order.
         */
        LOOKUP_JOIN_SORT_WARNING,

        /**
         * Temporarily forbid the use of an explicit or implicit LIMIT before INLINE STATS.
         */
        FORBID_LIMIT_BEFORE_INLINE_STATS(INLINE_STATS.enabled),

        /**
         * Catch-and-rethrow determinization complexity errors as 400s rather than 500s
         */
        HANDLE_DETERMINIZATION_COMPLEXITY,

        /**
         * Support for the TRANGE function
         */
        FN_TRANGE,

        /**
         * https://github.com/elastic/elasticsearch/issues/136851
         */
        INLINE_STATS_WITH_NO_COLUMNS(INLINE_STATS.enabled),

        FIX_MV_CONSTANT_EQUALS_FIELD,

        /**
         * Support for base conversion in TO_LONG and TO_INTEGER
         */
        BASE_CONVERSION,

        /**
         * {@link org.elasticsearch.xpack.esql.optimizer.rules.logical.ReplaceAliasingEvalWithProject} did not fully account for shadowing.
         * https://github.com/elastic/elasticsearch/issues/137019.
         */
        FIX_REPLACE_ALIASING_EVAL_WITH_PROJECT_SHADOWING,

        /**
         * Chunk function.
         */
        CHUNK_FUNCTION_V2(),

        /**
         * Support for vector similarity functions pushdown
         */
        VECTOR_SIMILARITY_FUNCTIONS_PUSHDOWN,

        FIX_MV_CONSTANT_COMPARISON_FIELD,

        FULL_TEXT_FUNCTIONS_ACCEPT_NULL_FIELD,

        /**
         * Make FIRST agg work with null and multi-value fields.
         */
        FIRST_AGG_WITH_NULL_AND_MV_SUPPORT,

        /**
         * Make LAST agg work with null and multi-value fields.
         */
        LAST_AGG_WITH_NULL_AND_MV_SUPPORT,

        /**
         * Allow FIRST/LAST aggs to accept DATE/DATE_NANOS in the search field
         * https://github.com/elastic/elasticsearch/issues/142137
         */
        FIRST_LAST_AGG_WITH_DATES,

        /**
         * Allow ST_EXTENT_AGG to gracefully handle missing spatial shapes
         */
        ST_EXTENT_AGG_NULL_SUPPORT,

        /**
         * Support grouping window in time-series for example: rate(counter, "1m") or avg_over_time(field, "5m")
         */
        TIME_SERIES_WINDOW_V1,

        /**
         * Support like/rlike parameters https://github.com/elastic/elasticsearch/issues/131356
         */
        LIKE_PARAMETER_SUPPORT,

        /**
         * PromQL support in ESQL, in the state it was when first available in non-snapshot builds.
         */
        PROMQL_COMMAND_V0,

        /**
         * Bundle flag for PromQL math functions.
         */
        PROMQL_MATH_V0,

        /**
         * Support for the ACOSH function.
         */
        ACOSH_FUNCTION,

        /**
         * Initial support for simple binary comparisons in PromQL.
         * Only top-level comparisons are supported where the right-hand side is a scalar.
         */
        PROMQL_BINARY_COMPARISON_V0,

        /**
         * Support for PromQL time() function.
         */
        PROMQL_TIME,

        /**
         * Queries for unmapped fields return no data instead of an error.
         * Also filters out nulls from results.
         */
        PROMQL_UNMAPPED_FIELDS_FILTER_NULLS,

        /**
         * Support for nested across-series aggregates in PromQL.
         * E.g., avg(sum by (cluster) (rate(foo[5m])))
         */
        PROMQL_NESTED_AGGREGATES(PROMQL_COMMAND_V0.isEnabled()),

        /**
         * KNN function adds support for k and visit_percentage options
         */
        KNN_FUNCTION_OPTIONS_K_VISIT_PERCENTAGE,

        /**
         * Enables automatically grouping by all dimension fields in TS mode queries and outputs the _timeseries column
         * with all the dimensions.
         */
        METRICS_GROUP_BY_ALL_WITH_TS_DIMENSIONS,

        /**
         * Fix for circular reference in alias chains during PushDownEnrich and aggregate deduplication.
         * Prevents "Potential cycle detected" errors when aliases reference each other.
         * https://github.com/elastic/elasticsearch/issues/138346
         */
        FIX_ENRICH_ALIAS_CYCLE_IN_DEDUPLICATE_AGGS,

        /**
         * Returns the top snippets for given text content and associated query.
         */
        TOP_SNIPPETS_FUNCTION,

        /**
         * A fix allowing the {@code TOP_SNIPPETS} function to process string config
         * parameters like the other functions.
         */
        TOP_SNIPPETS_FUNCTION_STRING_CONFIG,

        /**
         * Fix for multi-value constant propagation after GROUP BY.
         * When a multi-value constant (e.g., [1, 2]) is used as GROUP BY key, the aggregation explodes
         * it into single values. Propagating the original multi-value literal after the Aggregate would
         * incorrectly treat the field as still being multi-valued.
         * https://github.com/elastic/elasticsearch/issues/135926
         */
        FIX_STATS_MV_CONSTANT_FOLD,

        /**
         * https://github.com/elastic/elasticsearch/issues/138283
         */
        FIX_INLINE_STATS_INCORRECT_PRUNNING(INLINE_STATS.enabled),

        /**
         * Support for ST_CENTROID_AGG aggregation on geo_shape and cartesian_shape fields.
         */
        ST_CENTROID_AGG_SHAPES,

        /**
         * {@link ReplaceStatsFilteredOrNullAggWithEval} replaced a stats
         * with false filter with null with {@link org.elasticsearch.xpack.esql.expression.function.aggregate.Present} or
         * {@link org.elasticsearch.xpack.esql.expression.function.aggregate.Absent}
         */
        FIX_PRESENT_AND_ABSENT_ON_STATS_WITH_FALSE_FILTER,

        /**
         * Support for the MV_INTERSECTION function which returns the set intersection of two multivalued fields
         */
        FN_MV_INTERSECTION,

        /**
         * Support for the MV_UNION function which returns the set union of two multivalued fields
         */
        FN_MV_UNION,

        /**
         * Enables late materialization on node reduce. See also QueryPragmas.NODE_LEVEL_REDUCTION
         */
        ENABLE_REDUCE_NODE_LATE_MATERIALIZATION(Build.current().isSnapshot()),

        /**
         * {@link ReplaceStatsFilteredOrNullAggWithEval} now replaces an
         * {@link org.elasticsearch.xpack.esql.expression.function.aggregate.AggregateFunction} with null value with an
         * {@link org.elasticsearch.xpack.esql.plan.logical.Eval}.
         * https://github.com/elastic/elasticsearch/issues/137544
         */
        FIX_AGG_ON_NULL_BY_REPLACING_WITH_EVAL,

        /**
         * Support for requesting the "_tier" metadata field.
         */
        METADATA_TIER_FIELD(Build.current().isSnapshot()),
        /**
         * Fix folding of coalesce function
         * https://github.com/elastic/elasticsearch/issues/139344
         */
        FIX_FOLD_COALESCE,

        /**
         * Exceptions parsing date-times are thrown as IllegalArgumentException
         */
        DATE_TIME_EXCEPTIONS_HANDLED,

        /**
         * Enrich works with dense_vector fields
         */
        ENRICH_DENSE_VECTOR_BUGFIX,

        /**
         * Support for dense_vector arithmetic operations (+, -, *, /)
         */
        DENSE_VECTOR_ARITHMETIC,

        /**
         * Dense_vector aggregation functions
         */
        DENSE_VECTOR_AGG_FUNCTIONS,
        /**
         * Marks the move to the hash(doc) % shard_count routing function. Added in #137062.
         */
        ROUTING_FUNCTION_UPDATE,

        /**
         * Adds support for binary operations (such as addition, subtraction, etc.) to the TS|STATS command.
         */
        TS_STATS_BINARY_OPS,

        /**
         * Fix for INLINE STATS GROUP BY null being incorrectly pruned by PruneLeftJoinOnNullMatchingField.
         * For INLINE STATS, the right side of the join can be Aggregate or LocalRelation (when optimized).
         * The join key is always the grouping, and since STATS supports GROUP BY null, pruning the join when
         * the join key (grouping) is null would incorrectly change the query results. This fix ensures
         * PruneLeftJoinOnNullMatchingField only applies to LOOKUP JOIN (where right side is EsRelation).
         * https://github.com/elastic/elasticsearch/issues/139887
         */
        FIX_INLINE_STATS_GROUP_BY_NULL(INLINE_STATS.enabled),

        /**
         * Adds a conditional block loader for text fields that prefers using the sub-keyword field whenever possible.
         */
        CONDITIONAL_BLOCK_LOADER_FOR_TEXT_FIELDS,

        /**
         * MMR result diversification command
         */
        MMR(Build.current().isSnapshot()),

        /**
         * Allow wildcards in FROM METADATA, eg FROM idx METADATA _ind*
         */
        METADATA_WILDCARDS,

        /**
         * Fixes reset calculation in rates where partitioning data into multiple slices can lead to incorrect results.
         */
        RATE_FIX_RESETS_MULTIPLE_SEGMENTS,

        /**
         * Support query approximation.
         */
        APPROXIMATION(Build.current().isSnapshot()),

        /**
         * Create a ScoreOperator only when shard contexts are available
         */
        FIX_SCORE_OPERATOR_PLANNING,

        /**
         * Periodically emit partial aggregation results when the number of groups exceeds the threshold.
         */
        PERIODIC_EMIT_PARTIAL_AGGREGATION_RESULTS,

        /**
         * Support for requesting the "_size" metadata field when the mapper-size plugin is enabled.
         */
        METADATA_SIZE_FIELD,

        /**
         * Fix for <a href="https://github.com/elastic/elasticsearch/issues/141627">141627</a>,
         * TO_IP with leading_zeros=octal generates proper warning and returns null when given invalid input.
         */
        FIX_TO_IP_LEADING_ZEROS_OCTAL,

        /**
         * Support for configuring T-Digest elasticsearch field as a time series metric.
         */
        TDIGEST_TIME_SERIES_METRIC,

        /**
         * Fix bug with TS command where you can't group on aliases (i.e. `by c = cluster`)
         */
        TS_COMMAND_GROUP_ON_ALIASES,

        /**
         * Implicit SORT @timestamp DESC for TS queries without STATS or explicit SORT.
         */
        TS_IMPLICIT_TIMESTAMP_SORT,

        // Last capability should still have a comma for fewer merge conflicts when adding new ones :)
        // This comment prevents the semicolon from being on the previous capability when Spotless formats the file.
        ;

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
        assert feature.id().startsWith("esql.") : "node feature must start with 'esql.' but was " + feature.id();
        return feature.id().substring("esql.".length());
    }
}
