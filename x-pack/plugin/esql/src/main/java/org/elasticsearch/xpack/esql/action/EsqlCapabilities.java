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
         * Support for function {@code CBRT}. Done in #108574.
         */
        FN_CBRT,

        /**
         * Support for {@code MV_APPEND} function. #107001
         */
        FN_MV_APPEND,

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
        LOOKUP_V4(true),

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
        MATCH_OPERATOR(true),

        /**
         * Add CombineBinaryComparisons rule.
         */
        COMBINE_BINARY_COMPARISONS,

        /**
         * MATCH command support
         */
        MATCH_COMMAND(true),

        /**
         * Support for nanosecond dates as a data type
         */
        DATE_NANOS_TYPE(EsqlCorePlugin.DATE_NANOS_FEATURE_FLAG),

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
        MIXED_NUMERIC_TYPES_IN_COALESCE;

        private final boolean snapshotOnly;
        private final FeatureFlag featureFlag;

        Cap() {
            this(false, null);
        };

        Cap(boolean snapshotOnly) {
            this(snapshotOnly, null);
        };

        Cap(FeatureFlag featureFlag) {
            this(false, featureFlag);
        }

        Cap(boolean snapshotOnly, FeatureFlag featureFlag) {
            assert featureFlag == null || snapshotOnly == false;
            this.snapshotOnly = snapshotOnly;
            this.featureFlag = featureFlag;
        }

        public boolean isEnabled() {
            if (featureFlag == null) {
                return Build.current().isSnapshot() || this.snapshotOnly == false;
            }
            return featureFlag.isEnabled();
        }

        public String capabilityName() {
            return name().toLowerCase(Locale.ROOT);
        }
    }

    public static final Set<String> CAPABILITIES = capabilities();

    private static Set<String> capabilities() {
        List<String> caps = new ArrayList<>();
        for (Cap cap : Cap.values()) {
            if (cap.isEnabled()) {
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
