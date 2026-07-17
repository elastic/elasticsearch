/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.Build;
import org.elasticsearch.common.util.FeatureFlag;
import org.elasticsearch.compute.lucene.query.LuceneQueryEvaluator;
import org.elasticsearch.compute.lucene.read.ValuesSourceReaderOperator;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.index.SliceIndexing;
import org.elasticsearch.rest.action.admin.cluster.RestNodesCapabilitiesAction;
import org.elasticsearch.xpack.esql.expression.function.EsqlFunctionRegistry;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.ReplaceStatsFilteredOrNullAggWithEval;
import org.elasticsearch.xpack.esql.plugin.EsqlFeatures;

import java.util.HashSet;
import java.util.Locale;
import java.util.Set;

/**
 * A {@link Set} of "capabilities" supported by the {@link RestEsqlQueryAction}
 * and {@link RestEsqlAsyncQueryAction} APIs. These are exposed over the
 * {@link RestNodesCapabilitiesAction} and we use them to enable tests.
 */
public class EsqlCapabilities {
    /**
     * ESQL capabilities.
     *
     * @param all if {@code false} then only <strong>enabled</strong> capabilities are returned,
     *            otherwise <strong>all</strong> known capabilities are returned.
     */
    public static EsqlCapabilities capabilities(EsqlFunctionRegistry functions, boolean all) {
        Builder builder = new Builder(all);
        for (Cap cap : Cap.values()) {
            builder.add(cap.capabilityName(), cap.isEnabled());
        }

        /*
         * Add all of our cluster features without the leading "esql."
         */
        for (NodeFeature feature : new EsqlFeatures().getFeatures()) {
            builder.add(cap(feature), true);
        }

        if (all) {
            functions = functions.snapshotRegistry();
        }
        functions.addCapabilities(builder);
        return builder.build();
    }

    public static class Builder {
        private final Set<String> capabilities = new HashSet<>();
        private final boolean all;

        public Builder(boolean all) {
            this.all = all;
        }

        public boolean all() {
            return all;
        }

        public void add(String cap, boolean enabled) {
            if (all || enabled) {
                boolean firstTime = capabilities.add(cap);
                if (firstTime == false) {
                    throw new IllegalStateException("duplicate capability [" + cap + "]");
                }
            }
        }

        public EsqlCapabilities build() {
            return new EsqlCapabilities(Set.copyOf(capabilities));
        }
    }

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
         * Support for named options ({@code quad_segs}, {@code endcap}, {@code join}, {@code mitre_limit})
         * on {@code ST_BUFFER}. Requires a wire-protocol bump, so gates new csv-spec tests away from
         * mixed-version clusters that pre-date the change.
         */
        ST_BUFFER_OPTIONS,

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
         * Support for the {@code ::tdigest} and {@code ::exponential_histogram} casting operators.
         */
        CASTING_OPERATOR_FOR_HISTOGRAM_TYPES,

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
         * Support for optional fields (might or might not be present in the mappings) using DEFAULT/NULLIFY only.
         * Compared to {@link #OPTIONAL_FIELDS_V5}, this does not enable support for LOAD.
         */
        OPTIONAL_FIELDS_NULLIFY_TECH_PREVIEW,

        /**
         * Fix incorrect detection of unmapped fields in nullify/load mode when unresolved attributes
         * match fields already present in the children's output.
         */
        OPTIONAL_FIELDS_FIX_UNMAPPED_FIELD_DETECTION,

        /**
         * Don't nullify aliases for Aggregate groupings.
         */
        OPTIONAL_FIELDS_NULLIFY_SKIP_GROUP_ALIASES,

        /**
         * Nullify unmapped fields in agg filters like {@code STATS agg_fun(field) WHERE field...}, even when
         * {@link org.elasticsearch.xpack.esql.analysis.Analyzer.ResolveRefs} marks the field as unresolvable with a custom error message.
         */
        OPTIONAL_FIELDS_DETECT_UNMAPPED_FIELDS_IN_AGG_FILTERS,

        /**
         * Fix for 500 error when querying multiple indices with {@code unmapped_fields="load"}.
         * See https://github.com/elastic/elasticsearch/issues/145555
         */
        OPTIONAL_FIELDS_FIX_UNMAPPED_LOAD_MULTI_INDEX_PATTERN,

        /**
         * Fix for flattened subfields not being nullified when {@code unmapped_fields="nullify"} is set.
         * See https://github.com/elastic/elasticsearch/issues/142616
         */
        OPTIONAL_FIELDS_FIX_NULLIFY_FLATTENED_SUBFIELD,

        /**
         * Fix for 500 return code when loading from {@code _source} (hence {@code KEYWORD}) and passing to a convert function that doesn't
         * take {@code KEYWORD}s.
         * See https://github.com/elastic/elasticsearch/issues/145998.
         */
        OPTIONAL_FIELDS_FIX_UNMAPPED_LOAD_CONVERT_FUNCTION,

        /**
         * Fix for {@code <no-fields>} leaking into plans when {@code unmapped_fields="load"} loads fields from an empty mapping.
         * See https://github.com/elastic/elasticsearch/issues/141990.
         */
        OPTIONAL_FIELDS_FIX_UNMAPPED_LOAD_EMPTY_MAPPING_NO_FIELDS,

        /**
         * Fix for LOOKUP JOIN and ENRICH failing when the match field has NULL type from unmapped field nullification.
         * See https://github.com/elastic/elasticsearch/issues/141827
         */
        OPTIONAL_FIELDS_FIX_NULL_MATCH_FIELD_IN_JOIN_AND_ENRICH,

        /**
         * Support for optional fields (might or might not be present in the mappings) using DEFAULT/NULLIFY/LOAD.
         * V2: Prevent pushing down filters and sorts to Lucene of potentially unmapped fields.
         * V3: Fix synthetic _source numeric load bug (#143916)
         * V4: Support for union type like resolution for load.
         * V5: Support for rejecting partially unmapped non-keywords unless cast or projected
         *     Support for rejecting loading subfields of flattened fields
         */
        OPTIONAL_FIELDS_V5,

        /**
         * Unconditionally load partially mapped keyword fields, whether they are mentioned in expressions or not.
         * <p>
         * Also, always load values of partially mapped fields from the indices where they are mapped.
         * <p>
         * Fixes https://github.com/elastic/elasticsearch/issues/141994
         * and https://github.com/elastic/elasticsearch/issues/145206
         */
        OPTIONAL_FIELDS_FIX_LOAD_PARTIALLY_MAPPED,

        /**
         * Implicit casting of PUNKs that have two types (or legs): KEYWORD by virtue of loading from _source, and exactly one other type
         * where mapped.
         *
         * See https://github.com/elastic/elasticsearch/issues/141995
         */
        OPTIONAL_FIELDS_UNMAPPED_LOAD_AUTO_CAST_TWO_LEGGED_PUNKS,

        /**
         * Fixes a bug when using an EVAL on the grouped by columns after a STATS.
         *
         * See https://github.com/elastic/elasticsearch/issues/152496.
         */
        OPTIONAL_FIELDS_UNMAPPED_EVAL_AFTER_STATS_FIX,

        /**
         * Fix for a {@code ClassCastException} when an explicitly cast or implicitly widened partially unmapped small numeric field.
         * See https://github.com/elastic/elasticsearch/issues/151525.
         */
        OPTIONAL_FIELDS_FIX_PARTIALLY_UNMAPPED_SMALL_NUMERIC,

        /**
         * Null-fallback under {@code unmapped_fields="load"}: full-text search functions are supported, and single-type partially
         * unmapped non-keyword fields (PUNKs) fall back to their mapped type, nullifying the unmapped rows -- matching the default
         * (no-load) behavior -- instead of being rejected. This also lets such fields be renamed and used in expressions.
         */
        OPTIONAL_FIELDS_UNMAPPED_LOAD_NULL_FALLBACK,

        /**
         * Bugfix: {@code IndexResolver.mergedMappings} crashed with {@code UnsupportedOperationException} when a keyword
         * field with multi-fields (e.g. {@code my_field.analyzed}) was partially unmapped across the queried indices and
         * {@code SET unmapped_fields="load"} was active. {@code PotentiallyUnmappedKeywordEsField} was constructed with
         * an immutable empty properties map, preventing child fields from being inserted.
         */
        OPTIONAL_FIELDS_FIX_LOAD_KEYWORD_WITH_MULTIFIELDS,

        /**
         * Don't implicitly cast a partially unmapped {@code dense_vector} field under {@code unmapped_fields="load"}.
         * See https://github.com/elastic/elasticsearch/issues/152184.
         */
        OPTIONAL_FIELDS_FIX_PARTIALLY_UNMAPPED_DENSE_VECTOR,

        /**
         * Warn when a partially unmapped field with a single non-KEYWORD mapped type is referenced explicitly, but its type has no
         * KEYWORD converter and so cannot be loaded from _source: it falls back to null in the indices where it is unmapped.
         */
        OPTIONAL_FIELDS_WARN_NON_LOADABLE_PUNK,

        /**
         * With {@code unmapped_fields="nullify"} or {@code "load"}, a {@code DROP} wildcard that matches no field is a no-op
         * instead of failing with "No matches found for pattern".
         * See https://github.com/elastic/elasticsearch/issues/143226
         */
        OPTIONAL_FIELDS_DROP_NON_MATCHING_PATTERN_NOOP,

        /**
         * Fixes count on an unmapped field. Previously, it tried to push down a query filter on the unmapped field, leading to a 0-count
         * since the field isn't mapped.
         * See https://github.com/elastic/elasticsearch/issues/152884.
         */
        OPTIONAL_FIELDS_FIX_COUNT_ON_UNMAPPED,

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
         * support for MV_CONTAINS function
         * <a href="https://github.com/elastic/elasticsearch/pull/133099/">Add MV_CONTAINS function #133099</a>
         */
        FN_MV_CONTAINS_V1,

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
         * Test-only capability since loading a value from a flattened field is possible using the unmapped field infrastructure, but
         * is only supported by full integration tests. So this capability is used to disable some tests in CsvTests.
         */
        LOAD_FLATTENED_FIELD,

        /**
         * Support for the {@code flattened} data type in ES|QL, which loads flattened fields as JSON objects.
         */
        FLATTENED_DATATYPE,

        /**
         * Flattened field keys are returned in alphabetical order.
         */
        FLATTENED_DATATYPE_SORTED_KEYS,

        /**
         * Flattened fields apply {@code null_value} replacement when loading from {@code _source},
         * matching the doc-values behaviour applied at index time.
         */
        FLATTENED_DATATYPE_NULL_VALUE,

        /**
         * Support for the {@code field_extract} function, which reads a sub-key from a {@code flattened} field root.
         */
        FIELD_EXTRACT_FUNCTION,

        /**
         * Pushdown optimizations for {@code field_extract(<flattened root>, "<literal key>")}: block-loader
         * fusion that reads the keyed sub-field's doc values directly, and Lucene query pushdown for
         * {@code ==}, {@code !=}, {@code IN}, the four range comparators ({@code >}, {@code >=},
         * {@code <}, {@code <=}), and closed ranges (combined {@code >=}/{@code <=}, equivalent to
         * {@code BETWEEN}) against the same shape. The one-sided range forms rely on the keyed
         * flattened mapper substituting a key-prefix sentinel for the open bound so the resulting
         * Lucene query stays inside the open key's portion of the term namespace. Tests that
         * depend on the fused multi-value output or on the {@code SingleValueQuery} warning text
         * must require this capability so they skip on mixed clusters where any data node still
         * runs the per-row evaluator.
         */
        FIELD_EXTRACT_FLATTENED_PUSHDOWN,

        /**
         * The per-row evaluator for {@code field_extract(<flattened root>, "<key>")} returns a multi-value
         * keyword block for an array sub-field, a JSON-string keyword for a nested-object sub-field, and a
         * null position for {@code VALUE_NULL}, instead of always going through {@code parser.text()} (which
         * threw on every non-scalar value). Tests that exercise the parse path on a non-scalar sub-field
         * must require this capability so they skip on mixed clusters where any data node still runs the
         * pre-fix evaluator and would surface the legacy {@code Expected text at &lt;line&gt;:&lt;col&gt;
         * but found START_ARRAY} warning instead of the new value.
         */
        FIELD_EXTRACT_RETURNS_MULTI_VALUE,

        /**
         * {@code field_extract(<flattened root>, "<key>")} returns the sub-field's value for an explicitly
         * mapped sub-key (one declared under {@code properties}) instead of {@code null}. Mapped sub-keys are
         * no longer fused into the keyed block loader nor pushed to a Lucene query - the keyed channel never
         * stores them, and a typed-field query would apply different comparison semantics than the keyword
         * evaluator - so they always go through the per-row evaluator over the merged flattened root. This
         * makes the result independent of whether the optimizer pushed the call. Tests that assert the value
         * (rather than {@code null}) for a mapped sub-key, or that a mapped-key comparison is not pushed to
         * Lucene, must require this capability so they skip on mixed clusters where any data node still fuses
         * mapped sub-keys and returns {@code null}.
         */
        FIELD_EXTRACT_MAPPED_SUBFIELD_RETURNS_VALUE,

        /**
         * A {@code flattened} root that declares mapped sub-fields (e.g. {@code KEEP attributes}) is always loaded
         * from {@code _source}, producing one canonical stringly-typed blob on every loading path: every leaf is a
         * string (a mapped {@code long} sub-field reads back as {@code "200"}, not the native {@code 200}) and every
         * key is present, including a bare {@code text} sub-field that has no doc values and so could never be rebuilt
         * by the doc-values root loader. Direct access to the typed sub-field column (e.g. {@code attributes.status_code}
         * or {@code attributes.message}) is unaffected and still returns the native value. Tests that pin this blob
         * shape must require this capability so they skip on mixed clusters where an older data node still builds the
         * root from doc values, rendering mapped sub-fields with their native type and dropping a bare text sub-field.
         */
        FLATTENED_ROOT_STRINGIFIES_MAPPED_SUBFIELDS,

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
         * Support for function {@code ST_DISTANCE}. Done in #108764.
         */
        ST_DISTANCE,

        /** Support for function {@code ST_EXTENT_AGG}. */
        ST_EXTENT_AGG,

        /** Optimization of ST_EXTENT_AGG with doc-values as IntBlock. */
        ST_EXTENT_AGG_DOCVALUES,

        /** Fix to bug with spatial aggregations not properly supporting the WHERE clause. Fixes #142329. */
        SPATIAL_AGGS_FILTERING,

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
         * Support for ST_CENTROID_AGG aggregation on geo_shape and cartesian_shape fields.
         */
        ST_CENTROID_AGG_SHAPES,

        /**
         * Support for ST_CENTROID_AGG aggregation on shapes from doc-values.
         */
        ST_CENTROID_AGG_SHAPES_DOC_VALUES,

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
         * Expose resolved bucket interval in {@code _meta} on {@code BUCKET} grouping columns, gated behind the
         * {@code SET column_metadata=true} setting. Without the setting, non-approximation metadata is omitted.
         */
        COLUMN_METADATA_BUCKET_V2,

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
         * Guards a fix for the boost parameter in QueryString queries
         */
        QSTR_FUNCTION_BOOST_FIX,
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
         * Fix sorting not allowed on histogram and _tsid.
         */
        SORTING_ON_HISTOGRAM_AND_TSID_FORBIDDEN,

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
         * Support for field aliases in mappings. Used by tests, since this was feature wasn't always supported by CsvTests.
         */
        FIELD_ALIAS_SUPPORT,

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
         * Support avg as a possible default metric for aggregate_metric_double
         */
        AGGREGATE_METRIC_DOUBLE_AVG_AS_DEFAULT_METRIC,

        /**
         * Return 0 (instead of null) for count on AMD when there are no rows
         */
        AGGREGATE_METRIC_DOUBLE_NO_ROWS_COUNT_0,

        /**
         * Support binary operators for aggregate_metric_double
         */
        AGGREGATE_METRIC_DOUBLE_BINARY_OPERATORS,

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
        SUBQUERY_IN_FROM_COMMAND,

        /**
         * Support non-correlated subqueries in the FROM clause without implicit limit.
         */
        SUBQUERY_IN_FROM_COMMAND_WITHOUT_IMPLICIT_LIMIT,

        /**
         * Append an implicit limit to unbounded sorts in subqueries in the FROM clause.
         */
        SUBQUERY_IN_FROM_COMMAND_APPEND_IMPLICIT_LIMIT_TO_UNBOUNDED_SORT_IN_SUBQUERY,

        /**
         * Prune no-fields in subquery project.
         */
        SUBQUERY_IN_FROM_COMMAND_PRUNE_NO_FIELDS,

        /**
         * Fix for union types when fields have conflicting types between subqueries.
         * https://github.com/elastic/elasticsearch/issues/142499
         */
        SUBQUERY_IN_FROM_COMMAND_UNION_TYPES_CONFLICT_RESOLUTION,

        /**
         * Carry over synthetic convert-function attributes introduced by
         * {@code ResolveUnionTypesInUnionAll} through intermediate {@code Project} nodes (e.g. those
         * produced by {@code RENAME}, {@code KEEP}, or {@code DROP}) sitting above the {@code UnionAll}.
         * Without this, the synthetic {@code $$<field>$converted_to$<type>} attribute referenced by the
         * rewritten convert function would not be visible above the {@code Project}, producing a plan
         * with missing references that fails the optimizer's plan consistency check.
         * https://github.com/elastic/elasticsearch/issues/149509
         */
        SUBQUERY_IN_FROM_COMMAND_CARRY_OVER_SYNTHETIC_CONVERT_ATTRIBUTES,

        /**
         * Fix for union types that have counter field renamed, but the data type is inconsistent with union all output.
         */
        SUBQUERY_IN_FROM_COMMAND_UNION_TYPES_IMPLICIT_CASTING_INCONSISTENT_AFTER_RENAME,

        /**
         * Fix for {@code PruneColumns} leaving an inconsistent plan when an {@code INLINE STATS} sits above a {@code UnionAll}
         * (from a subquery in FROM) or a {@code Fork}.
         */
        SUBQUERY_IN_FROM_COMMAND_INLINE_STATS_PRUNING,

        /**
         * Support IN non-correlated subqueries in WHERE command.
         */
        WHERE_IN_SUBQUERY,

        /**
         * Support IN non-correlated subqueries in WHERE command without View. When a view is referenced by an IN subquery, or there is an
         * IN subquery inside the view definition(especially nested views), it is out of the scope of this capability.
         * Add a new capability, so that integration tests don't run on nodes that only have WHERE_IN_SUBQUERY capability.
         */
        WHERE_IN_SUBQUERY_WITHOUT_VIEW,

        /**
         * Support IN non-correlated subqueries in WHERE command with View. The views can be referenced by IN subqueries, and the view
         * definition can contain IN subqueries.
         */
        WHERE_IN_SUBQUERY_WITH_VIEW,

        /**
         * Support ROW as a source command inside subquery in the from command.
         */
        SUBQUERY_WITH_ROW,

        /**
         * Support TS as a source command inside subquery in the from command.
         */
        SUBQUERY_WITH_TS,

        /**
         * Fixed {@code TranslateTimeSeriesWithout} and {@code TranslateTimeSeriesAggregate} to associate time-series attributes with the
         * correct time-series index when a join presents.
         */
        WHERE_IN_SUBQUERY_WITH_TS,
        /**
         * Support for views in cluster state (and REST API).
         */
        VIEWS_IN_CLUSTER_STATE,

        /**
         * Basic Views with no branching (do not need subqueries or FORK).
         */
        VIEWS_WITH_NO_BRANCHING(VIEWS_IN_CLUSTER_STATE.isEnabled()),
        /**
         * Views crud actions as index actions
         */
        VIEWS_CRUD_AS_INDEX_ACTIONS(VIEWS_WITH_NO_BRANCHING.isEnabled()),
        /**
         * Signals that {@code PUT /_query/view/{name}} is exposed with {@code @ServerlessScope(Scope.PUBLIC)}.
         * Old nodes in a mixed cluster predate this annotation and will not report this capability via
         * {@code /_capabilities}, so any mixed cluster containing such a node correctly returns
         * {@code supported=false}.
         */
        VIEWS_PUT_SERVERLESS_SCOPE(VIEWS_CRUD_AS_INDEX_ACTIONS.isEnabled()),
        /**
         * Views with branching (requires subqueries/FORK).
         */
        VIEWS_WITH_BRANCHING(VIEWS_WITH_NO_BRANCHING.isEnabled() && SUBQUERY_IN_FROM_COMMAND.isEnabled()),
        /**
         * Added telemetry for views
         */
        VIEWS_TELEMETRY,
        /**
         * Fixed a bug where views are incorrectly de-duplicated.
         */

        VIEWS_DEDUPLICATION_BUGFIX,
        /**
         * Fixed false circular view reference errors when multiple sibling views are resolved together.
         * See https://github.com/elastic/elasticsearch/issues/146208
         */
        VIEWS_FALSE_CIRCULAR_REFERENCE_FIX,

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
         * Support for {@code TO_COUNTER} function and the {@code ::counter} cast operator, which converts
         * {@code long}, {@code integer}, and {@code double} values to their counter-typed equivalents.
         */
        TO_COUNTER,

        /**
         * Support for {@code TO_GAUGE} function and the {@code ::gauge} cast operator, which converts
         * {@code counter_long}, {@code counter_integer}, and {@code counter_double} values to their
         * plain numeric (gauge) equivalents.
         */
        TO_GAUGE,

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
         * {@link LuceneQueryEvaluator} rewrites the query before executing it in Lucene. This
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
         * EXPLAIN command with remote plans (5 columns: cluster, node, role, type, plan)
         */
        EXPLAIN_WITH_REMOTE_PLANS(Build.current().isSnapshot()),
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
         * Decay function for custom scoring.
         */
        DECAY_FUNCTION,

        /**
         * Fix conversions for parameters for {@code DECAY}.
         */
        DECAY_FUNCTION_PARAMETER_CONVERSION,

        /**
         * Support DECAY with unsigned_long parameters for {@code DECAY}.
         */
        DECAY_FUNCTION_UNSIGNED_LONG,

        /**
         * Fix latitude/longitude ordering of the in geo-point {@code DECAY}.
         * Previously the origin was serialized as "lon,lat" before being parsed by
         * {@code GeoUtils.parseGeoPoint}, which expects "lat,lon", effectively swapping the
         * origin's coordinates and producing incorrect distances whenever {@code lat != lon}.
         */
        DECAY_GEO_POINT_ORIGIN_LAT_LON_FIX,

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
         * Support for tstep function
         */
        TSTEP(Build.current().isSnapshot()),

        /**
         * Support for tstep explicit bounds variant: TSTEP(step, from, to)
         */
        TSTEP_EXPLICIT_BOUNDS(TSTEP.isEnabled()),

        /**
         * Support for tstep bucket count variant: TSTEP(count, from, to)
         */
        TSTEP_BUCKET_COUNT(TSTEP.isEnabled()),

        /**
         * Support lower-open upper-closed boundaries (*;*] in addition to lower-closed upper-open [*;*)
         */
        FIX_TSTEP_BUCKET_ROUNDING(TSTEP.isEnabled()),

        /**
         * Fix windowed over-time/rate aggregations whose window is smaller than the time bucket when the bucket is
         * end-labeled (right-closed), as produced by {@code TSTEP} and PromQL range queries. Previously such queries
         * returned empty results because the per-sample window filter looked one bucket past the correct boundary.
         */
        FIX_TSTEP_WINDOW_FILTER_ROUNDING(TSTEP.isEnabled()),

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
         * COALESCE function support for dense_vector type.
         */
        COALESCE_DENSE_VECTOR,

        /**
         * Multivalued query parameters
         */
        QUERY_PARAMS_MULTI_VALUES(),

        FIX_PERCENTILE_PRECISION(),

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
         * V3 fixes a bug on how we handle single-value time buckets for INCREASE with the sole value falling onto the bucket boundary.
         */
        RATE_WITH_INTERPOLATION_V3,

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
         * Fix LimitOperator truncation with zero columns
         * https://github.com/elastic/elasticsearch/issues/142473
         */
        FIX_LIMIT_TRUNCATION_WITH_ZERO_COLUMNS,

        /**
         * Support for dots in FUSE attributes
         */
        DOTS_IN_FUSE,

        /**
         * Support for the DATE_RANGE field type, RANGE_WITHIN, TO_DATE_RANGE(string), RANGE_MIN, RANGE_MAX.
         * TO_DATE_RANGE(string) honors the query timezone (Configuration); malformed input produces a
         * warning + null. RANGE_MIN/MAX/WITHIN/INTERSECTS return {@code null} when any argument is
         * multi-valued (consistent with ES|QL null-for-MV semantics). The Lucene pushdown uses
         * {@code RECHECK} so the row-level evaluator rechecks each candidate and returns {@code null}
         * for multi-valued positions, which {@code FilterOperator} treats as {@code false}.
         */
        DATE_RANGE_FIELD_TYPE_V6,

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
         * Top-level {@code settings} object on the {@code _query} request body, mirroring in-query SET keys.
         */
        QUERY_SETTINGS_REQUEST_BODY,

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
         * Supporting grouping window in time-series where the window is smaller than the time bucket
         */
        TIME_SERIES_WINDOW_SMALLER_THAN_BUCKET,

        /**
         * TS window functions use backward window semantics only.
         */
        FIX_TIME_SERIES_WINDOW_BACKWARD,

        /**
         * PromQL uses TSTEP instead of TBUCKET, with corrected open-ended range query bounds.
         */
        FIX_PROMQL_TIME_BUCKET_V2(FIX_TIME_SERIES_WINDOW_BACKWARD.isEnabled()),

        /**
         * PromQL {@code round(v, to_nearest)} uses the Prometheus formula, fixing wrong rounding
         * and floating point junk from dividing by small {@code to_nearest} values.
         */
        FIX_PROMQL_ROUND_TO_NEAREST,

        /**
         * Extended time-bucket fix covering scalar float-division step-timestamp alignment.
         * Disabled until the serverless-side fix for the one-hour timestamp offset is deployed.
         * https://github.com/elastic/elasticsearch-serverless/issues/6817
         */
        FIX_PROMQL_TIME_BUCKET_V3(false),

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
         * Support for the ASINH function.
         */
        ASINH_FUNCTION,

        /**
         * Support for the ATANH function.
         */
        ATANH_FUNCTION,

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
         * Support for PromQL instant queries.
         */
        PROMQL_INSTANT_QUERY,

        /**
         * Support for the {@code DATE_UNIT_COUNT} function.
         */
        ESQL_DATE_UNIT_COUNT_FN,

        /**
         * Support for deriving PromQL time buckets from [start, end, buckets] when [step] is omitted.
         */
        PROMQL_BUCKETS_PARAMETER,

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
         * Support post-processing STATS commands after PROMQL source commands.
         */
        PROMQL_POST_PROCESSING_STATS,

        /**
         * PromQL scalar() function support.
         */
        PROMQL_SCALAR,

        /**
         * Support implicit conversion from an instant selector to a range selector for range-vector functions.
         * For example, `rate(metric)` is interpreted as `rate(metric[step])`.
         */
        PROMQL_IMPLICIT_RANGE_SELECTOR,

        /**
         * PromQL functions accept any numeric range vector. ES|QL translates mismatched counter/gauge
         * types with implicit {@code to_counter()} or {@code to_gauge()} wraps based on each function's
         * {@link org.elasticsearch.xpack.esql.expression.promql.function.PromqlFunctionDefinition.CounterSupport}.
         */
        PROMQL_IMPLICIT_TYPE_COERCION,

        /**
         * Support for PromQL {@code without} grouping.
         */
        PROMQL_WITHOUT_GROUPING,

        /**
         * Corrected output shape for PromQL {@code without}: a {@code without} over a concrete-output child (e.g.
         * {@code sum without(pod) (sum by(cluster,region,pod) (...))}) projects the child's concrete grouping columns
         * minus the excluded labels, rather than the opaque {@code _timeseries} column. Gates the affected csv-spec
         * tests so mixed-version clusters skip them on older nodes that still emit {@code _timeseries}.
         */
        FIX_PROMQL_WITHOUT_OUTPUT,

        /**
         * PromQL label matchers that accept the empty string (e.g. {@code {label=""}} or {@code {label!="foo"}})
         * also match time series where the label is absent ({@code NULL}), per PromQL spec.
         */
        PROMQL_ABSENT_LABEL_MATCHING,

        /**
         * Support for the PromQL {@code offset} modifier, implemented as a constant time shift of the evaluation
         * timestamp. Heterogeneous offsets within a single source-backed binary operator remain unsupported.
         */
        PROMQL_OFFSET_MODIFIER(PROMQL_COMMAND_V0.isEnabled()),

        /**
         * Support for the {@code TS_COLLAPSE} pipe command, which collapses PromQL results
         * into one multi-valued row per series.
         */
        TS_COLLAPSE,

        /**
         * Support for `WITHOUT` grouping function
         * that excludes specific dimensions from time-series grouping.
         */
        ESQL_WITHOUT_GROUPING,

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
         * Does {@code TOP_SNIPPETS} process all field values?
         */
        TOP_SNIPPETS_MV,

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
         * {@link ReplaceStatsFilteredOrNullAggWithEval} replaced a stats
         * with false filter with null with {@link org.elasticsearch.xpack.esql.expression.function.aggregate.Present} or
         * {@link org.elasticsearch.xpack.esql.expression.function.aggregate.Absent}
         */
        FIX_PRESENT_AND_ABSENT_ON_STATS_WITH_FALSE_FILTER,

        /**
         * Enables late materialization on node reduce. See also QueryPragmas.NODE_LEVEL_REDUCTION
         */
        ENABLE_REDUCE_NODE_LATE_MATERIALIZATION,

        /**
         * Fix stale row-stride reader state when a conditional block loader uses column-at-a-time loading after row-stride loading
         * across segments.
         */
        FIX_VALUES_READER_STALE_ROW_STRIDE_READER,

        /**
         * {@link ReplaceStatsFilteredOrNullAggWithEval} now replaces an
         * {@link org.elasticsearch.xpack.esql.expression.function.aggregate.AggregateFunction} with null value with an
         * {@link org.elasticsearch.xpack.esql.plan.logical.Eval}.
         * https://github.com/elastic/elasticsearch/issues/137544
         */
        FIX_AGG_ON_NULL_BY_REPLACING_WITH_EVAL,

        /**
         * Makes SUM(long) agg return null+warning instead of a 500 overflow.
         */
        FIX_SUM_AGG_LONG_OVERFLOW,

        /**
         * AVG(long) casts the field to double up-front in its surrogate, so the intermediate sum
         * can no longer overflow.
         * https://github.com/elastic/elasticsearch/issues/99575
         */
        FIX_AVG_AGG_LONG_OVERFLOW,

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
         * Support for arithmetic operations (+, -, *, /) between dense_vector and scalar values
         */
        DENSE_VECTOR_SCALAR_ARITHMETIC,

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
         * Fix null comparison type check in binary comparisons.
         * Null should be compatible with any type in binary comparisons.
         * https://github.com/elastic/elasticsearch/issues/140460
         */
        FIX_NULL_COMPARISON_TYPE_CHECK,

        /**
         * Adds a conditional block loader for text fields that prefers using the sub-keyword field whenever possible.
         */
        CONDITIONAL_BLOCK_LOADER_FOR_TEXT_FIELDS,

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
        APPROXIMATION_V7,

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
         * Support for the {@code TO_EXPONENTIAL_HISTOGRAM} conversion function.
         */
        TO_EXPONENTIAL_HISTOGRAM,

        /**
         * Support for converting {@code exponential_histogram} fields via {@code TO_TDIGEST}.
         */
        TO_TDIGEST_FROM_EXPONENTIAL_HISTOGRAM,

        /**
         * Support for {@code MEDIAN} aggregation on {@code tdigest} type fields.
         */
        TDIGEST_MEDIAN,

        /**
         * Support for {@code FIRST_OVER_TIME} and {@code LAST_OVER_TIME} on {@code tdigest} type fields.
         */
        TDIGEST_FIRST_LAST_OVER_TIME,

        /**
         * A bugfix we applied to the HISTOGRAM_PERCENTILE algorithm on the tdigest type.
         * We previously were using hybrid-digests by accident and now use a merging digest.
         */
        TDIGEST_PERCENTILES_USE_MERGING_DIGEST,

        /**
         * Fix bug with TS command where you can't group on aliases (i.e. `by c = cluster`)
         */
        TS_COMMAND_GROUP_ON_ALIASES,

        /**
         * Implicit SORT @timestamp DESC for TS queries without STATS or explicit SORT.
         */
        TS_IMPLICIT_TIMESTAMP_SORT,

        /**
         * Fixes https://github.com/elastic/elasticsearch/issues/139359
         */
        INLINE_STATS_DROP_GROUPINGS_FIX(INLINE_STATS.enabled),

        /**
         * Support for the MMR result diversification command
         */
        MMR_V2,

        /**
         * Supports the {@code URI_PARTS}) command.
         */
        URI_PARTS_COMMAND,

        /**
         * Support for the METRICS_INFO command.
         */
        METRICS_INFO_COMMAND,

        /**
         * Support for TBUCKET with numeric bucket count and optional from/to parameters.
         */
        TBUCKET_FROM_TO,

        /**
         * Supports the REGISTERED_DOMAIN command.
         */
        REGISTERED_DOMAIN_COMMAND,

        /**
         * The {@code GROK}, {@code DISSECT}, {@code URI_PARTS}, and {@code REGISTERED_DOMAIN}
         * commands accept {@code null} typed parameters and produce {@code null} results.
         */
        STR_COMMANDS_ACCEPT_NULL,

        /**
         * Support for the EXTERNAL command (datasource access). Snapshot-only: the grammar predicates in
         * {@code EsqlBaseParser.g4}/{@code From.g4} read this capability directly to gate the EXTERNAL
         * grammar surface, rather than this capability mirroring a separate build-type check.
         */
        EXTERNAL_COMMAND(Build.current().isSnapshot()),

        /**
         * Support for the EXTERNAL command (datasource access).
         */
        EXTERNAL_CSV_IP_SUPPORT,

        /**
         * Support for the {@code header_row} (and the related {@code column_prefix}) CSV options
         * on the {@code EXTERNAL} command, used to read headerless CSV files.
         */
        EXTERNAL_CSV_HEADER_ROW_OPTION,

        /**
         * The CSV/TSV file-level {@code datetime_format} option compiles to an Elasticsearch
         * {@code DateFormatter} rather than a raw JDK {@code DateTimeFormatter}: zone offsets are honored,
         * date-only patterns parse, and named formats and {@code a||b} composites are accepted.
         */
        EXTERNAL_CSV_DATETIME_FORMAT_ES_DATE_FORMATTER,

        /**
         * Per-file planner-resolved read schema is threaded down to runtime readers via
         * {@code FileSplit.readSchema()}. Pins each file's column layout to the planner's view,
         * preventing reader self-inference that drifts across files in a multi-file glob.
         */
        EXTERNAL_SOURCE_READ_SCHEMA,

        /**
         * Always-on {@code _file.*} virtual columns ({@code _file.path}, {@code _file.name}, {@code _file.directory},
         * {@code _file.size}, {@code _file.modified}) added to every external-source schema. Older coordinators do
         * not know these columns and fail verification with {@code Unknown column [_file.*]} when CCQ routes a query
         * against a remote cluster on a pre-feature build, so {@code fileMetadata*} csv-spec tests must be gated on
         * this capability rather than on {@link #EXTERNAL_COMMAND}.
         */
        EXTERNAL_SOURCE_FILE_METADATA_COLUMNS,

        /**
         * Standard ES metadata columns ({@code _id}, {@code _index}, {@code _version}, {@code _source}, ...)
         * accepted in the {@code METADATA} clause of external-dataset {@code FROM}. Pre-feature
         * coordinators reject the names with {@code Unknown column}; tests exercising these columns
         * gate on this capability.
         */
        EXTERNAL_SOURCE_STANDARD_METADATA_COLUMNS,

        /**
         * Support for projecting nested STRUCT subfields (e.g. {@code event.action}) from
         * Parquet (Java) and ORC external sources. Gated so format readers that do not yet
         * implement nested support (parquet-rs, csv, ndjson, etc.) skip the csv-spec tests
         * until they catch up.
         *
         * <p>Tracks: elastic/esql-planning#435 (this PR) and elastic/esql-planning#320
         * (correctness gap for Parquet-Java MAP/STRUCT/nested LIST).
         */
        EXTERNAL_SOURCE_NESTED_STRUCT_PROJECTION,

        /**
         * Correct decoding of a {@code LIST} leaf reached through a {@code STRUCT}
         * (e.g. {@code answers.text} where {@code answers} is {@code struct<text: list<string>>}).
         * Such leaves previously bound to no column descriptor and read as all-null on the Parquet
         * (Java) reader; this capability gates the csv-spec tests that assert the multivalues now
         * round-trip. Separate from {@link #EXTERNAL_SOURCE_NESTED_STRUCT_PROJECTION} so nodes that
         * predate the fix (and other format readers) skip the tests instead of failing them.
         *
         * <p>Tracks: elastic/esql-planning#1055 (correctness gap for Parquet-Java list-under-struct).
         */
        EXTERNAL_SOURCE_LIST_UNDER_STRUCT,

        /**
         * Multi-file external UNION_BY_NAME widens cross-file type disagreements to KEYWORD
         * instead of throwing at planning time. The reconciler emits a warning header per
         * affected column, the per-file ColumnMapping carries a stringification cast, and the
         * reader's output is adapted via SchemaAdaptingIterator. STRICT mode still throws.
         * See esql-planning#794.
         */
        EXTERNAL_UNION_BY_NAME_KEYWORD_FALLBACK,

        /**
         * {@code FROM <dataset>} resolved through the same pipeline as {@code FROM <index>} (Phase 1: dataset-only patterns).
         */
        DATASET_IN_FROM_COMMAND,

        /**
         * {@link org.elasticsearch.xpack.esql.optimizer.rules.logical.PruneRedundantAggregateGroupings} rebuilds a pruned
         * derived external grouping reading the attribute the aggregate actually exposes (e.g. a rename alias) instead of the
         * pre-aggregate attribute it no longer surfaces, fixing the {@code optimized incorrectly due to missing references}
         * verification failure that old coordinators in a mixed cluster still hit.
         */
        FIX_PRUNE_RENAMED_DERIVED_EXTERNAL_GROUPING,

        /**
         * A present-but-empty field on a string (KEYWORD/TEXT) column in an external CSV/TSV datasource reads as the empty
         * string {@code ""} instead of {@code null}. Genuinely missing fields (a row shorter than the schema) and empty
         * fields on non-string columns still read as {@code null}. Used to gate the affected external csv-spec tests so they
         * are skipped on mixed clusters where a pre-change node still maps empty string cells to {@code null}.
         */
        EXTERNAL_CSV_EMPTY_STRING_NOT_NULL,

        /**
         * Datasource file plugins (CSV, ORC, Parquet) no longer return {@code TEXT} types, only {@code KEYWORD}.
         * See <a href="https://github.com/elastic/elasticsearch/pull/145334">#145334</a>. Used to gate the affected
         * {@code external-basic.csv-spec} tests so they are skipped on mixed clusters where a pre-change coordinator
         * still maps string typed-schema/Parquet-String/ORC-String to {@code TEXT} - see
         * <a href="https://github.com/elastic/elasticsearch/issues/145352">#145352</a> and
         * <a href="https://github.com/elastic/elasticsearch/issues/145353">#145353</a>.
         */
        DATASOURCE_FILE_READERS_NO_TEXT_TYPE,

        /**
         * https://github.com/elastic/elasticsearch/issues/142219
         */
        INLINE_STATS_WITH_CONSTANTS(INLINE_STATS.enabled),

        /**
         * Fix for an ArrayIndexOutOfBoundsException in the aggregation framework when the same field is passed twice.
         * https://github.com/elastic/elasticsearch/issues/142180
         */
        FIX_AGGREGATION_FRAMEWORK_CHANNELS,

        /**
         * Support for the TS_INFO command — per-time-series granularity variant of METRICS_INFO.
         */
        TS_INFO_COMMAND,

        /**
         * Dense_vector SUM aggregation function
         */
        DENSE_VECTOR_SUM_FUNCTION,

        /**
         * Support passing constants and null in the second parameter of FIRST/LAST aggs.
         */
        FIX_AGG_FIRST_LAST_FOLDABLES_IN_SORT_FIELD,

        /**
         * Support for intra-row field references in ROW command.
         * https://github.com/elastic/elasticsearch/issues/140217
         */
        ROW_FIELD_RESOLUTION,

        /**
         * Support aggregating on integers in FIRST/LAST.
         */
        FIRST_LAST_AGG_ON_INTS,

        /**
         * Fix for KQL/QSTR functions failing when used with unmapped fields in NULLIFY mode.
         * Unmapped fields are now added directly to EsRelation output with NULL type instead of using Eval nodes.
         * https://github.com/elastic/elasticsearch/issues/142968
         */
        FIX_UNMAPPED_FIELDS_IN_ESRELATION,

        /**
         * Support for dense_vector equality and inequality operators (==, !=).
         */
        DENSE_VECTOR_EQUALITY,

        /**
         * Fix for not including metadata _doc_count in the _timeseries column
         * https://github.com/elastic/elasticsearch/issues/143464
         */
        FIX_DISPLAYING_TS_DIMENSIONS_IN_METRICS_GROUP_BY_ALL,

        /**
         * Support for the zero_terms_query option in the match function.
         * https://github.com/elastic/elasticsearch/issues/143070
         */
        MATCH_FUNCTION_ZERO_TERMS_QUERY,

        /**
         * Fix for full-text functions failing on renamed fields.
         * https://github.com/elastic/elasticsearch/issues/143859
         */
        FIX_FULL_TEXT_FUNCTIONS_ON_RENAMED_FIELDS,

        /**
         * TOP_SNIPPETS checks that the query is foldable
         */
        TOP_SNIPPETS_FOLDABLE_QUERY_CHECK,

        /**
         * Fixes an analysis bug in {@code FORK} with {@code unmapped_fields="nullify"}.
         * Preserve existing attribute {@code NameId}s so that references from upper plan nodes remain valid after
         * sub-plans are updated. Only genuinely new attributes get fresh NameIds.
         * Keeping the same attributes can have unintended side effects when applying optimizations like constant folding.
         * https://github.com/elastic/elasticsearch/issues/142762
         */
        FIX_FORK_UNMAPPED_NULLIFY,

        /**
         * Support for pushing the ROUND_TO function into field loading via {@code BlockLoaderExpression}.
         */
        ROUND_TO_BLOCK_LOADER(Build.current().isSnapshot()),

        /**
         * Fix for the STATS BY ALL with LIMIT 0.
         * https://github.com/elastic/elasticsearch/issues/144024
         */
        FIX_LIMIT_ZERO_IN_STATS_BY_ALL,

        /**
         * Fix field caps incorrectly synthesizing object parents under subobjects:false (passthrough) mappers,
         * causing false type conflicts in ES|QL when querying across indices.
         * https://github.com/elastic/elasticsearch/issues/144179
         */
        FIX_PASSTHROUGH_FIELD_CAPS_OBJECT_PARENT,

        /**
         * Support for highlight markup in {@code TOP_SNIPPETS} via the {@code highlight} option.
         */
        TOP_SNIPPETS_HIGHLIGHT,

        /**
         * Support for the {@code order} option in {@code TOP_SNIPPETS}.
         */
        TOP_SNIPPETS_ORDER,

        /**
         * Support for the {@code analyzer} option on {@code TOP_SNIPPETS}: choose a registered analyzer
         * (prebuilt or plugin-contributed) by name, or omit it to default to the standard analyzer.
         */
        TOP_SNIPPETS_ANALYZER,

        /**
         * Enables the feature LIMIT n BY expr1, expr2 for retaining at most n docs per group.
         * The feature will not work if we had SORT | LIMIT n BY
         */
        ESQL_LIMIT_BY,

        /**
         * Enables the SORT | LIMIT n BY expr1, expr2 support, see ESQL_LIMIT_BY for more context
         */
        ESQL_TOPN_BY,

        /**
         * Corrects a bug with ENRICH when a shard does not contain an index field and we use LIMIT BY on top
         */
        LIMIT_BY_ENRICH_FIX(ESQL_LIMIT_BY.isEnabled()),

        /**
         * Fix pushdown of LIMIT BY past MV_EXPAND when grouping on expanded fields.
         * See <a href="https://github.com/elastic/elasticsearch/issues/148513">#148513</a>.
         */
        LIMIT_BY_MV_EXPAND_GROUPING_FIX,

        /**
         * Fix window validation in time-series aggregations when TBUCKET uses a numeric target bucket count.
         */
        FIX_TBUCKET_TARGET_COUNT_WINDOW_VALIDATION,

        /**
         * TSDB Temporality support.
         */
        TSDB_TEMPORALITY_SUPPORT_V8,

        /**
         * Support cumulative exponential histograms in _over_time aggregations.
         */
        TSDB_TEMPORALITY_SUPPORT_V9,

        /**
         * Support the null column type for the CHANGE_POINT command
         * <a href="https://github.com/elastic/elasticsearch/pull/144388"></a>
         */
        CHANGE_POINT_SUPPORT_NULL_COLUMN,

        /**
         * MMR fixes for constant folding
         */
        MMR_FOLDABLE_QUERY_VECTOR_FIX,

        /**
         * Support the BY grouping clause in CHANGE_POINT to detect change points independently per group.
         */
        CHANGE_POINT_BY,

        FIX_DIV_ERROR_MESSAGE,

        /**
         * Added {@link org.elasticsearch.xpack.esql.planner.PlannerSettings#DOC_THRESHOLD_AUTO_PARTITIONING}
         */
        AUTO_PARTITION_DOCS_THRESHOLD,

        /**
         * Rename the {@code unmapped_fields} default setting from {@code "fail"} to {@code "default"}.
         * See https://github.com/elastic/elasticsearch/issues/144833
         */
        UNMAPPED_FIELDS_DEFAULT_SETTING_RENAME,

        /**
         * Support window durations that are larger than but not exact multiples of the time bucket
         * for time-series aggregations (e.g., rate(counter, 7 minutes) with TBUCKET(5 minutes)).
         */
        TIME_SERIES_WINDOW_NON_MULTIPLE,

        /**
         * Move rules for TS translation into the Analyzer
         */
        TIME_SERIES_TRANSLATION_IN_ANALYZER,

        /**
         * Fix for {@code SUM(null)} producing a type mismatch after surrogate expansion.
         * See https://github.com/elastic/elasticsearch/issues/144914
         */
        FIX_SUM_OF_NULL_OPTIMIZATION,

        PROPAGATE_EMPTY_RELATION_PAST_JOINS,

        /**
         * Supports the {@code USER_AGENT} command.
         */
        USER_AGENT_COMMAND,

        /**
         * Fix full-text functions being rejected after SAMPLE.
         */
        FIX_SAMPLE_AFTER_KQL_OR_QSTR,
        KEYWORDS_MV_COUNT_AS_SINGLE_VALUE_FIX,

        /**
         * Parquet and ORC filter pushdown for StartsWith (prefix range predicates).
         */
        PARQUET_ORC_STARTS_WITH_PUSHDOWN,

        /**
         * Alias for calling FIRST (or LAST) and only passing the search field. The sort field is implicitly set to @timestamp.
         * These are not time series agg functions.
         */
        EARLIEST_LATEST_AGGS,

        /**
         * Fix for full-text functions (MATCH, MATCH_PHRASE, :) on constant_keyword fields.
         * The optimizer no longer replaces their field arguments with literal constants.
         * See https://github.com/elastic/elasticsearch/issues/145570
         */
        FIX_FULL_TEXT_FUNCTIONS_ON_CONSTANT_KEYWORD,

        /**
         * Fix for {@code PropagateNullable} incorrectly discarding surviving OR branches when
         * a field is constrained by {@code IS NULL} or {@code IS NOT NULL} in the same AND conjunction.
         * Previously {@code (a IS NOT NULL OR p) AND a IS NULL} was optimized to {@code null AND a IS NULL}
         * (dropping {@code p}); now it correctly becomes {@code p AND a IS NULL}.
         * Symmetric fix: {@code (a IS NULL OR p) AND a IS NOT NULL} now correctly becomes
         * {@code p AND a IS NOT NULL} instead of remaining unoptimized.
         * See https://github.com/elastic/elasticsearch/issues/141579
         */
        FIX_PROPAGATE_NULLABLE_OR_DISJUNCTION,

        /**
         * Fix TBUCKET with a numeric bucket count returning a verification exception instead of empty results
         * when the top-level request filter covers a time range with no matching indices.
         * See https://github.com/elastic/elasticsearch/issues/146354
         */
        FIX_TBUCKET_NUMERIC_ON_EMPTY_RANGE,

        /**
         * Support for the {@code EMBEDDING} function for generating dense vector embeddings using the {@code embedding} task type.
         */
        EMBEDDING_FUNCTION,

        /**
         * Fix for {@code STARTS_WITH} and {@code ENDS_WITH} Lucene pushdown on {@code _index}: use wildcard escaping instead of
         * query-parser escaping, and honour the {@code stringLikeOnIndex} flag so the wildcard query forces string matching on metadata
         * fields.
         */
        FIX_STARTS_WITH_ENDS_WITH_PUSHDOWN_ON_INDEX,

        /**
         * Allow evaluatable grouping functions (such as {@code BUCKET}) inside {@code LIMIT ... BY}.
         * Stateful grouping functions (such as {@code CATEGORIZE}) remain restricted to {@code STATS}.
         */
        LIMIT_BY_ALLOW_EVALUATABLE_GROUPING_FUNCTIONS,

        /**
         * Fix for {@link org.elasticsearch.xpack.esql.optimizer.rules.physical.local.PushCountQueryAndTagsToSource} incorrectly
         * replacing an {@code AggregateExec} that has multiple aggregate functions (e.g. COUNT + MAX) with an
         * {@code EsStatsQueryExec} that only handles COUNT, when {@code CombineProjections} had removed the grouping key
         * from the aggregates list.
         * <p>
         *     See <a href="https://github.com/elastic/elasticsearch/issues/146479">#146479</a>
         * </p>
         */
        FIX_PUSH_COUNT_QUERY_AND_TAGS_WITH_MULTIPLE_AGGS,

        /**
         * Fix for column pruning in FORK.
         */
        FORK_PRUNE_ALL_COLUMNS_FIX,

        /**
         * Support query approximation with LOOKUP JOIN
         */
        APPROXIMATION_LOOKUP_JOIN_V2,

        /**
         * Support query approximation with INLINE STATS
         */
        APPROXIMATION_INLINE_STATS_V2,

        /**
         * Support for PromQL year() function.
         */
        PROMQL_YEAR,

        /**
         * Unknown PromQL functions now make the error message "Unknown PromQL function".
         */
        PROMQL_RESOLVE_UNKOWN,

        /**
         * Support for PromQL time extraction functions: month(), day_of_month(), day_of_week(), day_of_year(), hour(), minute().
         */
        PROMQL_TIME_FUNCTIONS,

        /**
         * Support for PromQL days_in_month() function.
         */
        PROMQL_DAYS_IN_MONTH,

        /**
         * Support for the {@code timeout} option in the {@code COMPLETION} and {@code RERANK} commands
         * and the {@code TEXT_EMBEDDING} function.
         */
        INFERENCE_ACCEPT_TIMEOUT,

        /**
         * Fix on multi-values that were unrolled and were still producing warnings in expressions
         * that do not accept multi-values
         *
         * See https://github.com/elastic/elasticsearch/issues/134706
         */
        FIX_UNROLLED_FOLDABLE_MV_WARNING,

        /**
         * Fix for SET reporting wrong line/column number (-1:-1) in validation errors.
         * see <a href="https://github.com/elastic/elasticsearch/issues/145873">ES|QL: wrong line/column number #145873</a>
         */
        FIX_SET_WRONG_LINE_COLUMN,

        /**
         * Fix for {@code _index LIKE} not supporting the {@code ?} wildcard character.
         * see <a href="https://github.com/elastic/elasticsearch/issues/146364">ES|QL: _index LIKE with ? #146364</a>
         */
        FIX_INDEX_LIKE_QUESTION_MARK_WILDCARD,

        /**
         * Fix query approximation for queries with few source rows, that are expanded
         * (e.g. by MV_EXPAND) into many rows reaching the STATS command.
         */
        APPROXIMATION_FIX_MIN_SOURCE_ROW_COUNT,

        /**
         * Match function and match operator support for runtime expressions, not just ES mapped fields.
         */
        MATCH_RUNTIME_SEARCH,

        /**
         * Fix for column pruning when FORK branches return no columns.
         */
        FORK_PROJECT_AWAY_COLUMNS_FIX,

        /**
         * Fix for histogram block loaders (tdigest, exponential_histogram) passing {@code nullsFiltered=true} to
         * sub-block-loaders for min, max and sum. Those sub-fields can be absent for empty histograms even when the
         * histogram field itself is present, so the null-filtered guarantee does not hold for them.
         * See <a href="https://github.com/elastic/elasticsearch/issues/147854">#147854</a>
         */
        FIX_HISTOGRAM_BLOCKLOADERS_ISNULL,

        /**
         * Fix for {@code CompoundOutputEval} commands not implementing {@code SortAgnostic}, causing {@code PruneRedundantOrderBy} to
         * fail when a SORT precedes these commands.
         */
        FIX_COMPOUND_OUTPUT_EVAL_SORT_AGNOSTIC,

        /**
         * Support for {@code unmapped_fields="load"} mode with {@code LOOKUP JOIN}.
         * Previously the combination was rejected at query validation time.
         * see <a href="https://github.com/elastic/elasticsearch/issues/142026">Issue #142026</a>
         */
        OPTIONAL_FIELDS_LOAD_WITH_LOOKUP_JOIN,

        /**
         * Support for {@code unmapped_fields="load"} with {@code FORK}, subqueries and views (previously rejected). See #142033.
         */
        OPTIONAL_FIELDS_LOAD_WITH_FORK_SUBQUERIES_AND_VIEWS,

        /**
         * Under {@code unmapped_fields="load"} or {@code "nullify"}, {@code DROP}ping an unmapped field in one {@code FORK} branch counts
         * as a mention, so the field is surfaced across the branches (materialized from {@code _source} under {@code load}, null-filled
         * under {@code nullify}) and null-filled in the dropping one. Dropping it in every branch surfaces nothing.
         */
        OPTIONAL_FIELDS_FORK_DROP_MATERIALIZES_SIBLINGS,

        /**
         * Support for the {@code ==} operator on the root of a {@code flattened} field in ES|QL.
         */
        FN_EQUALS_FLATTENED,

        /**
         * Support for the {@code !=} operator on the root of a {@code flattened} field in ES|QL.
         */
        FN_NOT_EQUALS_FLATTENED,

        /**
         * Support for using a {@code flattened} field as a grouping key in
         * {@code STATS … BY} and {@code LIMIT N BY}.
         */
        GROUP_BY_FLATTENED,

        /**
         * Fix for {@code ReorderLimitProjectAndOrderBy} unconditionally lifting an {@code OrderBy} above a renaming/dropping
         * {@code Project}: it now rewrites the {@code OrderBy}'s references through the {@code Project}'s aliases (so a sort on
         * a renamed column stays valid) and bails out of the swap if a referenced column is dropped altogether.
         * <p>
         *     See <a href="https://github.com/elastic/elasticsearch/issues/148612">#148612</a>.
         * </p>
         */
        FIX_REORDER_LIMIT_PROJECT_AND_ORDER_BY_PRESERVES_REFS,

        /**
         * Supports the {@code IP_LOCATION} command.
         */
        IP_LOCATION_COMMAND,

        /**
         * Support query approximation with FORK and subqueries.
         */
        APPROXIMATION_FORK,

        /**
         * Support FIRST aggregation on extended types: version, unsigned_long, geo_point,
         * cartesian_point, geo_shape, cartesian_shape, geohash, geotile, geohex.
         */
        FIRST_AGG_EXTENDED_TYPES,

        /**
         * Support FIRST and EARLIEST aggregation on the remaining types: dense_vector, exponential_histogram, tdigest.
         */
        FIRST_AGG_EXTENDED_TYPES_2,

        /**
         * Support FIRST and EARLIEST aggregation on the flattened data type.
         */
        FIRST_AGG_EXTENDED_TYPES_3,

        /**
         * FUSE uses FIRST(col, NULL) instead of VALUES for passthrough columns,
         * enabling dense_vector, exponential_histogram, and tdigest fields to
         * flow through FORK + FUSE hybrid-search pipelines.
         */
        FUSE_PASSTHROUGH_WITH_FIRST,

        /**
         * Support for the {@code DEDUP} command, which removes duplicate rows from the result set.
         * Snapshot-only.
         */
        DEDUP_COMMAND(Build.current().isSnapshot()),

        /**
         * Support for VALUES with date_range type.
         */
        VALUES_DATE_RANGE(DATE_RANGE_FIELD_TYPE_V6.isEnabled()),

        /**
         * Support for COALESCE with date_range type.
         */
        COALESCE_DATE_RANGE(DATE_RANGE_FIELD_TYPE_V6.isEnabled()),

        /**
         * Support for CASE with date_range type.
         */
        CASE_DATE_RANGE(DATE_RANGE_FIELD_TYPE_V6.isEnabled()),

        /**
         * Support for equality (==, !=) and IN with date_range type.
         */
        EQUALITY_DATE_RANGE(DATE_RANGE_FIELD_TYPE_V6.isEnabled()),

        /**
         * Fix TopN encoding/decoding of {@code long_range} values.
         * <a href="https://github.com/elastic/elasticsearch/issues/150383">#150383</a>
         */
        FIX_TOPN_LONG_RANGE_ENCODING(DATE_RANGE_FIELD_TYPE_V6.isEnabled()),

        /**
         * Support for MV_FIRST and MV_LAST with date_range type.
         */
        MV_FIRST_LAST_DATE_RANGE(DATE_RANGE_FIELD_TYPE_V6.isEnabled()),

        /**
         * Support for COUNT, COUNT_APPROXIMATE, PRESENT, ABSENT, MV_COUNT with date_range type.
         */
        AGG_BASIC_DATE_RANGE(DATE_RANGE_FIELD_TYPE_V6.isEnabled()),

        /**
         * Support for ESQL parameters in PromQL label matchers:
         * <a href="https://github.com/elastic/elasticsearch/issues/148620">#148620</a>
         */
        PROMQL_LABEL_MATCHER_PARAMS,

        /**
         * Fix for PromQL scalar integer division losing the fractional part.
         * Integer literals like {@code 4/6} were folded with integer division (result: 0)
         * instead of float64 division (result: ~0.667).
         * https://github.com/elastic/elasticsearch/issues/149792
         */
        FIX_PROMQL_SCALAR_FLOAT_DIV,

        /**
         * Fix for PromQL constant scalar expressions (e.g. {@code 3.14}, {@code pi()}) that were previously
         * evaluated through the full PromqlCommand pipeline and produced no results when the index was empty.
         * They are now folded at planning time and emitted as a {@code ROW} without touching the index.
         */
        FIX_PROMQL_SCALAR_CONSTANT_RESULTS,

        /**
         * PromQL {@code quantile} and {@code quantile_over_time} take the quantile φ in the range [0, 1], but the
         * φ value was passed straight through to the ES|QL {@code PERCENTILE} aggregation (which expects [0, 100]),
         * so e.g. {@code quantile(1.0, x)} returned ≈ the minimum instead of the maximum. φ is now scaled by 100.
         */
        FIX_PROMQL_QUANTILE_SCALE,

        /**
         * Bugfix in query approximation to not rewrite non-approximable FORK branches:
         * <a href="https://github.com/elastic/elasticsearch/issues/149501">#149501</a>
         */
        APPROXIMATION_FIX_NON_APPROXIMABLE_FORK_BRANCHES,

        /**
         * Bugfix in query approximation to not produce confidence intervals for multivalued functions.
         */
        APPROXIMATION_FIX_MV_FUNCTIONS,

        /**
         * Support for PromQL {@code histogram_count()}, {@code histogram_sum()} and {@code histogram_avg()} on native histograms.
         */
        PROMQL_HISTOGRAM_SUM_COUNT_AVG,

        /**
         * Support for PromQL {@code increase()} on exponential histograms.
         */
        PROMQL_INCREASE_ON_HISTOGRAM,

        /**
         * Support for PromQL {@code sum()} operator on exponential histograms.
         */
        PROMQL_SUM_ON_HISTOGRAM,

        /**
         * Support for the {@code HIGHLIGHT} command: grammar, plan nodes, serialization, and execution that exposes the
         * generated {@code highlight_*} columns. Snapshot-only.
         */
        HIGHLIGHT_V2(Build.current().isSnapshot()),

        /**
         * Support for PromQL {@code histogram_quantile()} over classic histograms with {@code le} buckets.
         */
        PROMQL_HISTOGRAM_QUANTILE,

        /**
         * Support for the top-level PromQL {@code or} (UNION) set operator between two instant vectors.
         */
        PROMQL_SET_OPERATOR_UNION,

        /**
         * Support for PromQL {@code histogram_quantile()} over classic histograms where {@code le} is not an explicit
         * child output.
         */
        PROMQL_HISTOGRAM_QUANTILE_IMPLICIT_LE,

        /**
         * Support for PromQL {@code histogram_quantile()} over exponential (native) histograms.
         */
        PROMQL_HISTOGRAM_QUANTILE_EXPONENTIAL,

        /**
         * Fixes a bug in the planner where {@code TS} queries without an outer aggregation (group by all)
         * would wrongly fail with an {@link IllegalStateException} if any aggregation had a filter.
         */
        FIX_GROUP_BY_ALL_AGGREGATION_FILTERS,

        /**
         * Fix for PromQL {@code without} and ES|QL {@code TS_WITHOUT}: passthrough alias names (e.g. OTel
         * {@code cpu} for the concrete dimension {@code attributes.cpu}) are now correctly resolved in the
         * {@code _timeseries} block loader so excluded labels are actually removed from the series key.
         * https://github.com/elastic/elasticsearch/issues/151540
         */
        FIX_TS_BLOCK_LOADER_PASSTHROUGH_ALIASING,

        /**
         * Support for the {@code _slice} metadata field in ES|QL, available on indices with
         * {@code index.slice.enabled: true}. Backed by the {@code _routing} sorted doc values field.
         * Enables {@code FROM index METADATA _slice}, {@code KEEP _slice}, and pushable
         * {@code WHERE _slice ==} / {@code LIKE} / {@code RLIKE} filters.
         */
        METADATA_SLICE(SliceIndexing.SLICE_FEATURE_FLAG),

        /**
         * Support LAST and LATEST aggregation on the same extended field types as FIRST and EARLIEST
         * (version, unsigned_long, spatial, spatial-grid, dense_vector, exponential_histogram, tdigest,
         * flattened).
         */
        LAST_AGG_EXTENDED_TYPES,

        /**
         * An empty list passed as a query parameter (named or positional) is treated as null
         * instead of producing an NPE. A defined-but-null param used in an identifier or pattern
         * position produces a clean parsing error instead of silently yielding an empty column name.
         * See <a href="https://github.com/elastic/elasticsearch/issues/147448">#147448</a>.
         */
        EMPTY_LIST_PARAM_AS_NULL,

        /**
         * Invalid BBOX envelopes (e.g. maxY &lt; minY, or maxX &lt; minX for cartesian coordinates) are now
         * consistently handled as a null result with a registered warning, both at fold time and at
         * runtime, instead of either being silently accepted (cartesian x-ordering was never validated) or
         * causing an uncaught exception in downstream consumers (ST_GEOHASH/ST_GEOTILE/ST_GEOHEX bounds,
         * ST_DISTANCE, ST_INTERSECTS/ST_DISJOINT/etc. pushdown to Lucene).
         * See <a href="https://github.com/elastic/elasticsearch/pull/152877">#152877</a>.
         */
        SPATIAL_BBOX_VALIDATION_FIX,

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

    /**
     * Convert a {@link NodeFeature} from {@link EsqlFeatures} into a
     * capability.
     */
    public static String cap(NodeFeature feature) {
        assert feature.id().startsWith("esql.") : "node feature must start with 'esql.' but was " + feature.id();
        return feature.id().substring("esql.".length());
    }

    private final Set<String> capabilities;

    private EsqlCapabilities(Set<String> capabilities) {
        this.capabilities = capabilities;
    }

    public Set<String> capabilities() {
        return capabilities;
    }
}
