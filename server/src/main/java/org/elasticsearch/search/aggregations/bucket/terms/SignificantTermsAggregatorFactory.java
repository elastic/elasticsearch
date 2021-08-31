/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.bucket.terms;

import org.apache.lucene.index.SortedSetDocValues;
import org.elasticsearch.common.logging.DeprecationCategory;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.Aggregator.SubAggCollectionMode;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.CardinalityUpperBound;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.NonCollectingAggregator;
import org.elasticsearch.search.aggregations.bucket.BucketUtils;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregator.BucketCountThresholds;
import org.elasticsearch.search.aggregations.bucket.terms.heuristic.SignificanceHeuristic;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.CoreValuesSourceType;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregatorFactory;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.search.aggregations.support.ValuesSourceRegistry;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;

public class SignificantTermsAggregatorFactory extends ValuesSourceAggregatorFactory {
    private static final DeprecationLogger deprecationLogger = DeprecationLogger.getLogger(SignificantTermsAggregatorFactory.class);

    static void registerAggregators(ValuesSourceRegistry.Builder builder) {
        builder.register(
            SignificantTermsAggregationBuilder.REGISTRY_KEY,
            Arrays.asList(CoreValuesSourceType.KEYWORD, CoreValuesSourceType.IP),
            SignificantTermsAggregatorFactory.bytesSupplier(),
            true
        );

        builder.register(
            SignificantTermsAggregationBuilder.REGISTRY_KEY,
            Arrays.asList(CoreValuesSourceType.DATE, CoreValuesSourceType.BOOLEAN, CoreValuesSourceType.NUMERIC),
            SignificantTermsAggregatorFactory.numericSupplier(),
            true
        );
    }

    /**
     * This supplier is used for all the field types that should be aggregated as bytes/strings,
     * including those that need global ordinals
     */
    private static SignificantTermsAggregatorSupplier bytesSupplier() {
        return new SignificantTermsAggregatorSupplier() {
            @Override
            public Aggregator build(
                String name,
                AggregatorFactories factories,
                ValuesSourceConfig valuesSourceConfig,
                DocValueFormat format,
                TermsAggregator.BucketCountThresholds bucketCountThresholds,
                IncludeExclude includeExclude,
                String executionHint,
                AggregationContext context,
                Aggregator parent,
                SignificanceHeuristic significanceHeuristic,
                SignificanceLookup lookup,
                CardinalityUpperBound cardinality,
                Map<String, Object> metadata
            ) throws IOException {

                ExecutionMode execution = null;
                if (executionHint != null) {
                    execution = ExecutionMode.fromString(executionHint, deprecationLogger);
                }
                if (valuesSourceConfig.hasOrdinals() == false) {
                    execution = ExecutionMode.MAP;
                }
                if (execution == null) {
                    execution = ExecutionMode.GLOBAL_ORDINALS;
                }

                if ((includeExclude != null) && (includeExclude.isRegexBased()) && format != DocValueFormat.RAW) {
                    throw new IllegalArgumentException(
                        "Aggregation ["
                            + name
                            + "] cannot support regular expression style "
                            + "include/exclude settings as they can only be applied to string fields. Use an array of values for "
                            + "include/exclude clauses"
                    );
                }

                return execution.create(
                    name,
                    factories,
                    valuesSourceConfig,
                    format,
                    bucketCountThresholds,
                    includeExclude,
                    context,
                    parent,
                    significanceHeuristic,
                    lookup,
                    cardinality,
                    metadata
                );
            }
        };
    }

    /**
     * This supplier is used for all fields that expect to be aggregated as a numeric value.
     * This includes floating points, and formatted types that use numerics internally for storage (date, boolean, etc)
     */
    private static SignificantTermsAggregatorSupplier numericSupplier() {
        return new SignificantTermsAggregatorSupplier() {
            @Override
            public Aggregator build(
                String name,
                AggregatorFactories factories,
                ValuesSourceConfig valuesSourceConfig,
                DocValueFormat format,
                TermsAggregator.BucketCountThresholds bucketCountThresholds,
                IncludeExclude includeExclude,
                String executionHint,
                AggregationContext context,
                Aggregator parent,
                SignificanceHeuristic significanceHeuristic,
                SignificanceLookup lookup,
                CardinalityUpperBound cardinality,
                Map<String, Object> metadata
            ) throws IOException {

                if ((includeExclude != null) && (includeExclude.isRegexBased())) {
                    throw new IllegalArgumentException(
                        "Aggregation ["
                            + name
                            + "] cannot support regular expression style include/exclude "
                            + "settings as they can only be applied to string fields. Use an array of numeric "
                            + "values for include/exclude clauses used to filter numeric fields"
                    );
                }

                ValuesSource.Numeric numericValuesSource = (ValuesSource.Numeric) valuesSourceConfig.getValuesSource();
                if (numericValuesSource.isFloatingPoint()) {
                    throw new UnsupportedOperationException("No support for examining floating point numerics");
                }

                IncludeExclude.LongFilter longFilter = null;
                if (includeExclude != null) {
                    longFilter = includeExclude.convertToLongFilter(format);
                }

                return new NumericTermsAggregator(
                    name,
                    factories,
                    agg -> agg.new SignificantLongTermsResults(lookup, significanceHeuristic, cardinality),
                    numericValuesSource,
                    format,
                    null,
                    bucketCountThresholds,
                    context,
                    parent,
                    SubAggCollectionMode.BREADTH_FIRST,
                    longFilter,
                    cardinality,
                    metadata
                );
            }
        };
    }

    private final SignificantTermsAggregatorSupplier aggregatorSupplier;
    private final IncludeExclude includeExclude;
    private final String executionHint;
    private final QueryBuilder backgroundFilter;
    private final TermsAggregator.BucketCountThresholds bucketCountThresholds;
    private final SignificanceHeuristic significanceHeuristic;

    SignificantTermsAggregatorFactory(
        String name,
        ValuesSourceConfig config,
        IncludeExclude includeExclude,
        String executionHint,
        QueryBuilder backgroundFilter,
        TermsAggregator.BucketCountThresholds bucketCountThresholds,
        SignificanceHeuristic significanceHeuristic,
        AggregationContext context,
        AggregatorFactory parent,
        AggregatorFactories.Builder subFactoriesBuilder,
        Map<String, Object> metadata,
        SignificantTermsAggregatorSupplier aggregatorSupplier
    ) throws IOException {
        super(name, config, context, parent, subFactoriesBuilder, metadata);

        if (config.hasValues()) {
            if (config.fieldContext().fieldType().isSearchable() == false) {
                throw new IllegalArgumentException(
                    "SignificantText aggregation requires fields to be searchable, but ["
                        + config.fieldContext().fieldType().name()
                        + "] is not"
                );
            }
        }

        this.aggregatorSupplier = aggregatorSupplier;
        this.includeExclude = includeExclude;
        this.executionHint = executionHint;
        this.backgroundFilter = backgroundFilter;
        this.bucketCountThresholds = bucketCountThresholds;
        this.significanceHeuristic = significanceHeuristic;
    }

    @Override
    protected Aggregator createUnmapped(Aggregator parent, Map<String, Object> metadata) throws IOException {
        final InternalAggregation aggregation = new UnmappedSignificantTerms(
            name,
            bucketCountThresholds.getRequiredSize(),
            bucketCountThresholds.getMinDocCount(),
            metadata
        );
        return new NonCollectingAggregator(name, context, parent, factories, metadata) {
            @Override
            public InternalAggregation buildEmptyAggregation() {
                return aggregation;
            }
        };
    }

    @Override
    protected Aggregator doCreateInternal(Aggregator parent, CardinalityUpperBound cardinality, Map<String, Object> metadata)
        throws IOException {
        BucketCountThresholds bucketCountThresholds = new BucketCountThresholds(this.bucketCountThresholds);
        if (bucketCountThresholds.getShardSize() == SignificantTermsAggregationBuilder.DEFAULT_BUCKET_COUNT_THRESHOLDS.getShardSize()) {
            // The user has not made a shardSize selection .
            // Use default heuristic to avoid any wrong-ranking caused by
            // distributed counting
            // but request double the usual amount.
            // We typically need more than the number of "top" terms requested
            // by other aggregations
            // as the significance algorithm is in less of a position to
            // down-select at shard-level -
            // some of the things we want to find have only one occurrence on
            // each shard and as
            // such are impossible to differentiate from non-significant terms
            // at that early stage.
            bucketCountThresholds.setShardSize(2 * BucketUtils.suggestShardSideQueueSize(bucketCountThresholds.getRequiredSize()));
        }

        SignificanceLookup lookup = new SignificanceLookup(context, config.fieldContext().fieldType(), config.format(), backgroundFilter);

        return aggregatorSupplier.build(
            name,
            factories,
            config,
            config.format(),
            bucketCountThresholds,
            includeExclude,
            executionHint,
            context,
            parent,
            significanceHeuristic,
            lookup,
            cardinality,
            metadata
        );
    }

    public enum ExecutionMode {

        MAP(new ParseField("map")) {

            @Override
            Aggregator create(
                String name,
                AggregatorFactories factories,
                ValuesSourceConfig valuesSourceConfig,
                DocValueFormat format,
                TermsAggregator.BucketCountThresholds bucketCountThresholds,
                IncludeExclude includeExclude,
                AggregationContext context,
                Aggregator parent,
                SignificanceHeuristic significanceHeuristic,
                SignificanceLookup lookup,
                CardinalityUpperBound cardinality,
                Map<String, Object> metadata
            ) throws IOException {

                final IncludeExclude.StringFilter filter = includeExclude == null ? null : includeExclude.convertToStringFilter(format);
                return new MapStringTermsAggregator(
                    name,
                    factories,
                    new MapStringTermsAggregator.ValuesSourceCollectorSource(valuesSourceConfig),
                    a -> a.new SignificantTermsResults(lookup, significanceHeuristic, cardinality),
                    null,
                    format,
                    bucketCountThresholds,
                    filter,
                    context,
                    parent,
                    SubAggCollectionMode.BREADTH_FIRST,
                    false,
                    cardinality,
                    metadata
                );

            }

        },
        GLOBAL_ORDINALS(new ParseField("global_ordinals")) {

            @Override
            Aggregator create(
                String name,
                AggregatorFactories factories,
                ValuesSourceConfig valuesSourceConfig,
                DocValueFormat format,
                TermsAggregator.BucketCountThresholds bucketCountThresholds,
                IncludeExclude includeExclude,
                AggregationContext context,
                Aggregator parent,
                SignificanceHeuristic significanceHeuristic,
                SignificanceLookup lookup,
                CardinalityUpperBound cardinality,
                Map<String, Object> metadata
            ) throws IOException {

                boolean remapGlobalOrd = true;
                if (cardinality == CardinalityUpperBound.ONE && factories == AggregatorFactories.EMPTY && includeExclude == null) {
                    /*
                     * We don't need to remap global ords iff this aggregator:
                     *    - collects from a single bucket AND
                     *    - has no include/exclude rules AND
                     *    - has no sub-aggregator
                     */
                    remapGlobalOrd = false;
                }

                ValuesSource.Bytes.WithOrdinals.FieldData ordinalsValuesSource =
                    (ValuesSource.Bytes.WithOrdinals.FieldData) valuesSourceConfig.getValuesSource();
                SortedSetDocValues values = TermsAggregatorFactory.globalOrdsValues(context, ordinalsValuesSource);
                return new GlobalOrdinalsStringTermsAggregator(
                    name,
                    factories,
                    a -> a.new SignificantTermsResults(lookup, significanceHeuristic, cardinality),
                    ordinalsValuesSource,
                    values,
                    null,
                    format,
                    bucketCountThresholds,
                    TermsAggregatorFactory.gloabalOrdsFilter(includeExclude, format, values),
                    context,
                    parent,
                    remapGlobalOrd,
                    SubAggCollectionMode.BREADTH_FIRST,
                    false,
                    cardinality,
                    metadata
                );
            }
        };

        public static ExecutionMode fromString(String value, final DeprecationLogger deprecationLogger) {
            if ("global_ordinals".equals(value)) {
                return GLOBAL_ORDINALS;
            } else if ("global_ordinals_hash".equals(value)) {
                deprecationLogger.deprecate(
                    DeprecationCategory.AGGREGATIONS,
                    "global_ordinals_hash",
                    "global_ordinals_hash is deprecated. Please use [global_ordinals] instead."
                );
                return GLOBAL_ORDINALS;
            } else if ("map".equals(value)) {
                return MAP;
            }
            throw new IllegalArgumentException("Unknown `execution_hint`: [" + value + "], expected any of [map, global_ordinals]");
        }

        private final ParseField parseField;

        ExecutionMode(ParseField parseField) {
            this.parseField = parseField;
        }

        abstract Aggregator create(
            String name,
            AggregatorFactories factories,
            ValuesSourceConfig valuesSourceConfig,
            DocValueFormat format,
            TermsAggregator.BucketCountThresholds bucketCountThresholds,
            IncludeExclude includeExclude,
            AggregationContext context,
            Aggregator parent,
            SignificanceHeuristic significanceHeuristic,
            SignificanceLookup lookup,
            CardinalityUpperBound cardinality,
            Map<String, Object> metadata
        ) throws IOException;

        @Override
        public String toString() {
            return parseField.getPreferredName();
        }
    }
}
