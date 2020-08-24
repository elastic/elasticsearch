/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.search.aggregations.bucket.terms;

import org.apache.lucene.search.IndexSearcher;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.AggregationExecutionException;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.Aggregator.SubAggCollectionMode;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.CardinalityUpperBound;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalOrder;
import org.elasticsearch.search.aggregations.InternalOrder.CompoundOrder;
import org.elasticsearch.search.aggregations.NonCollectingAggregator;
import org.elasticsearch.search.aggregations.bucket.BucketUtils;
import org.elasticsearch.search.aggregations.bucket.terms.NumericTermsAggregator.ResultStrategy;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregator.BucketCountThresholds;
import org.elasticsearch.search.aggregations.support.CoreValuesSourceType;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregatorFactory;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.search.aggregations.support.ValuesSourceRegistry;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

public class TermsAggregatorFactory extends ValuesSourceAggregatorFactory {
    static Boolean REMAP_GLOBAL_ORDS, COLLECT_SEGMENT_ORDS;

    static void registerAggregators(ValuesSourceRegistry.Builder builder) {
        builder.register(TermsAggregationBuilder.REGISTRY_KEY,
            List.of(CoreValuesSourceType.BYTES, CoreValuesSourceType.IP),
            TermsAggregatorFactory.bytesSupplier(), true);

        builder.register(TermsAggregationBuilder.REGISTRY_KEY,
            List.of(CoreValuesSourceType.DATE, CoreValuesSourceType.BOOLEAN, CoreValuesSourceType.NUMERIC),
            TermsAggregatorFactory.numericSupplier(), true);
    }

    /**
     * This supplier is used for all the field types that should be aggregated as bytes/strings,
     * including those that need global ordinals
     */
    private static TermsAggregatorSupplier bytesSupplier() {
        return new TermsAggregatorSupplier() {
            @Override
            public Aggregator build(String name,
                                    AggregatorFactories factories,
                                    ValuesSource valuesSource,
                                    BucketOrder order,
                                    DocValueFormat format,
                                    TermsAggregator.BucketCountThresholds bucketCountThresholds,
                                    IncludeExclude includeExclude,
                                    String executionHint,
                                    SearchContext context,
                                    Aggregator parent,
                                    SubAggCollectionMode subAggCollectMode,
                                    boolean showTermDocCountError,
                                    CardinalityUpperBound cardinality,
                                    Map<String, Object> metadata) throws IOException {
                ExecutionMode execution = null;
                if (executionHint != null) {
                    execution = ExecutionMode.fromString(executionHint);
                }
                // In some cases, using ordinals is just not supported: override it
                if (valuesSource instanceof ValuesSource.Bytes.WithOrdinals == false) {
                    execution = ExecutionMode.MAP;
                }
                if (execution == null) {
                    execution = ExecutionMode.GLOBAL_ORDINALS;
                }
                final long maxOrd = execution == ExecutionMode.GLOBAL_ORDINALS ? getMaxOrd(valuesSource, context.searcher()) : -1;
                if (subAggCollectMode == null) {
                    subAggCollectMode = pickSubAggColectMode(factories, bucketCountThresholds.getShardSize(), maxOrd);
                }

                if ((includeExclude != null) && (includeExclude.isRegexBased()) && format != DocValueFormat.RAW) {
                    // TODO this exception message is not really accurate for the string case.  It's really disallowing regex + formatter
                    throw new AggregationExecutionException("Aggregation [" + name + "] cannot support regular expression style "
                        + "include/exclude settings as they can only be applied to string fields. Use an array of values for "
                        + "include/exclude clauses");
                }

                // TODO: [Zach] we might want refactor and remove ExecutionMode#create(), moving that logic outside the enum
                return execution.create(name, factories, valuesSource, order, format, bucketCountThresholds, includeExclude,
                    context, parent, subAggCollectMode, showTermDocCountError, cardinality, metadata);

            }
        };
    }

    /**
     * This supplier is used for all fields that expect to be aggregated as a numeric value.
     * This includes floating points, and formatted types that use numerics internally for storage (date, boolean, etc)
     */
    private static TermsAggregatorSupplier numericSupplier() {
        return new TermsAggregatorSupplier() {
            @Override
            public Aggregator build(String name,
                                    AggregatorFactories factories,
                                    ValuesSource valuesSource,
                                    BucketOrder order,
                                    DocValueFormat format,
                                    TermsAggregator.BucketCountThresholds bucketCountThresholds,
                                    IncludeExclude includeExclude,
                                    String executionHint,
                                    SearchContext context,
                                    Aggregator parent,
                                    SubAggCollectionMode subAggCollectMode,
                                    boolean showTermDocCountError,
                                    CardinalityUpperBound cardinality,
                                    Map<String, Object> metadata) throws IOException {

                if ((includeExclude != null) && (includeExclude.isRegexBased())) {
                    throw new AggregationExecutionException("Aggregation [" + name + "] cannot support regular expression style "
                        + "include/exclude settings as they can only be applied to string fields. Use an array of numeric values for "
                        + "include/exclude clauses used to filter numeric fields");
                }

                if (subAggCollectMode == null) {
                    subAggCollectMode = pickSubAggColectMode(factories, bucketCountThresholds.getShardSize(), -1);
                }

                ValuesSource.Numeric numericValuesSource = (ValuesSource.Numeric) valuesSource;
                IncludeExclude.LongFilter longFilter = null;
                Function<NumericTermsAggregator, ResultStrategy<?, ?>> resultStrategy;
                if (numericValuesSource.isFloatingPoint()) {
                    if (includeExclude != null) {
                        longFilter = includeExclude.convertToDoubleFilter();
                    }
                    resultStrategy = agg -> agg.new DoubleTermsResults(showTermDocCountError);
                } else {
                    if (includeExclude != null) {
                        longFilter = includeExclude.convertToLongFilter(format);
                    }
                    resultStrategy = agg -> agg.new LongTermsResults(showTermDocCountError);
                }
                return new NumericTermsAggregator(name, factories, resultStrategy, numericValuesSource, format, order,
                    bucketCountThresholds, context, parent, subAggCollectMode, longFilter, cardinality, metadata);
            }
        };
    }

    private final BucketOrder order;
    private final IncludeExclude includeExclude;
    private final String executionHint;
    private final SubAggCollectionMode collectMode;
    private final TermsAggregator.BucketCountThresholds bucketCountThresholds;
    private final boolean showTermDocCountError;

    TermsAggregatorFactory(String name,
                           ValuesSourceConfig config,
                           BucketOrder order,
                           IncludeExclude includeExclude,
                           String executionHint,
                           SubAggCollectionMode collectMode,
                           TermsAggregator.BucketCountThresholds bucketCountThresholds,
                           boolean showTermDocCountError,
                           QueryShardContext queryShardContext,
                           AggregatorFactory parent,
                           AggregatorFactories.Builder subFactoriesBuilder,
                           Map<String, Object> metadata) throws IOException {
        super(name, config, queryShardContext, parent, subFactoriesBuilder, metadata);
        this.order = order;
        this.includeExclude = includeExclude;
        this.executionHint = executionHint;
        this.collectMode = collectMode;
        this.bucketCountThresholds = bucketCountThresholds;
        this.showTermDocCountError = showTermDocCountError;
    }

    @Override
    protected Aggregator createUnmapped(SearchContext searchContext,
                                            Aggregator parent,
                                            Map<String, Object> metadata) throws IOException {
        final InternalAggregation aggregation = new UnmappedTerms(name, order, bucketCountThresholds.getRequiredSize(),
                bucketCountThresholds.getMinDocCount(), metadata);
        Aggregator agg = new NonCollectingAggregator(name, searchContext, parent, factories, metadata) {
            @Override
            public InternalAggregation buildEmptyAggregation() {
                return aggregation;
            }
        };
        // even in the case of an unmapped aggregator, validate the order
        order.validate(agg);
        return agg;
    }

    private static boolean isAggregationSort(BucketOrder order) {
        if (order instanceof InternalOrder.Aggregation) {
            return true;
        } else if (order instanceof InternalOrder.CompoundOrder) {
            InternalOrder.CompoundOrder compoundOrder = (CompoundOrder) order;
            return compoundOrder.orderElements().stream().anyMatch(TermsAggregatorFactory::isAggregationSort);
        } else {
            return false;
        }
    }

    @Override
    protected Aggregator doCreateInternal(
        SearchContext searchContext,
        Aggregator parent,
        CardinalityUpperBound cardinality,
        Map<String, Object> metadata
    ) throws IOException {
        TermsAggregatorSupplier aggregatorSupplier = queryShardContext.getValuesSourceRegistry()
            .getAggregator(TermsAggregationBuilder.REGISTRY_KEY, config);
        BucketCountThresholds bucketCountThresholds = new BucketCountThresholds(this.bucketCountThresholds);
        if (InternalOrder.isKeyOrder(order) == false
            && bucketCountThresholds.getShardSize() == TermsAggregationBuilder.DEFAULT_BUCKET_COUNT_THRESHOLDS.getShardSize()) {
            // The user has not made a shardSize selection. Use default
            // heuristic to avoid any wrong-ranking caused by distributed
            // counting
            bucketCountThresholds.setShardSize(BucketUtils.suggestShardSideQueueSize(bucketCountThresholds.getRequiredSize()));
        }
        bucketCountThresholds.ensureValidity();

        return aggregatorSupplier.build(
            name,
            factories,
            config.getValuesSource(),
            order,
            config.format(),
            bucketCountThresholds,
            includeExclude,
            executionHint,
            searchContext,
            parent,
            collectMode,
            showTermDocCountError,
            cardinality,
            metadata
        );
    }

    /**
     * Pick a {@link SubAggCollectionMode} based on heuristics about what
     * we're collecting.
     */
    static SubAggCollectionMode pickSubAggColectMode(AggregatorFactories factories, int expectedSize, long maxOrd) {
        if (factories.countAggregators() == 0) {
            // Without sub-aggregations we pretty much ignore this field value so just pick something
            return SubAggCollectionMode.DEPTH_FIRST;
        }
        if (expectedSize == Integer.MAX_VALUE) {
            // We expect to return all buckets so delaying them won't save any time
            return SubAggCollectionMode.DEPTH_FIRST;
        }
        if (maxOrd == -1 || maxOrd > expectedSize) {
            /*
             * We either don't know how many buckets we expect there to be
             * (maxOrd == -1) or we expect there to be more buckets than
             * we will collect from this shard. So delaying collection of
             * the sub-buckets *should* save time.
             */
            return SubAggCollectionMode.BREADTH_FIRST;
        }
        // We expect to collect so many buckets that we may as well collect them all.
        return SubAggCollectionMode.DEPTH_FIRST;
    }

    /**
     * Get the maximum global ordinal value for the provided {@link ValuesSource} or -1
     * if the values source is not an instance of {@link ValuesSource.Bytes.WithOrdinals}.
     */
    private static long getMaxOrd(ValuesSource source, IndexSearcher searcher) throws IOException {
        if (source instanceof ValuesSource.Bytes.WithOrdinals) {
            ValuesSource.Bytes.WithOrdinals valueSourceWithOrdinals = (ValuesSource.Bytes.WithOrdinals) source;
            return valueSourceWithOrdinals.globalMaxOrd(searcher);
        } else {
            return -1;
        }
    }

    public enum ExecutionMode {

        MAP(new ParseField("map")) {

            @Override
            Aggregator create(String name,
                              AggregatorFactories factories,
                              ValuesSource valuesSource,
                              BucketOrder order,
                              DocValueFormat format,
                              TermsAggregator.BucketCountThresholds bucketCountThresholds,
                              IncludeExclude includeExclude,
                              SearchContext context,
                              Aggregator parent,
                              SubAggCollectionMode subAggCollectMode,
                              boolean showTermDocCountError,
                              CardinalityUpperBound cardinality,
                              Map<String, Object> metadata) throws IOException {
                final IncludeExclude.StringFilter filter = includeExclude == null ? null : includeExclude.convertToStringFilter(format);
                return new MapStringTermsAggregator(
                    name,
                    factories,
                    new MapStringTermsAggregator.ValuesSourceCollectorSource(valuesSource),
                    a -> a.new StandardTermsResults(valuesSource),
                    order,
                    format,
                    bucketCountThresholds,
                    filter,
                    context,
                    parent,
                    subAggCollectMode,
                    showTermDocCountError,
                    cardinality,
                    metadata
                );
            }
        },
        GLOBAL_ORDINALS(new ParseField("global_ordinals")) {

            @Override
            Aggregator create(String name,
                              AggregatorFactories factories,
                              ValuesSource valuesSource,
                              BucketOrder order,
                              DocValueFormat format,
                              TermsAggregator.BucketCountThresholds bucketCountThresholds,
                              IncludeExclude includeExclude,
                              SearchContext context, Aggregator parent,
                              SubAggCollectionMode subAggCollectMode,
                              boolean showTermDocCountError,
                              CardinalityUpperBound cardinality,
                              Map<String, Object> metadata) throws IOException {

                final long maxOrd = getMaxOrd(valuesSource, context.searcher());
                assert maxOrd != -1;
                final double ratio = maxOrd / ((double) context.searcher().getIndexReader().numDocs());

                assert valuesSource instanceof ValuesSource.Bytes.WithOrdinals;
                ValuesSource.Bytes.WithOrdinals ordinalsValuesSource = (ValuesSource.Bytes.WithOrdinals) valuesSource;

                if (factories == AggregatorFactories.EMPTY &&
                        includeExclude == null &&
                        cardinality == CardinalityUpperBound.ONE &&
                        ordinalsValuesSource.supportsGlobalOrdinalsMapping() &&
                        // we use the static COLLECT_SEGMENT_ORDS to allow tests to force specific optimizations
                        (COLLECT_SEGMENT_ORDS!= null ? COLLECT_SEGMENT_ORDS.booleanValue() : ratio <= 0.5 && maxOrd <= 2048)) {
                    /*
                     * We can use the low cardinality execution mode iff this aggregator:
                     *  - has no sub-aggregator AND
                     *  - collects from a single bucket AND
                     *  - has a values source that can map from segment to global ordinals
                     *  - At least we reduce the number of global ordinals look-ups by half (ration <= 0.5) AND
                     *  - the maximum global ordinal is less than 2048 (LOW_CARDINALITY has additional memory usage,
                     *  which directly linked to maxOrd, so we need to limit).
                     */
                    return new GlobalOrdinalsStringTermsAggregator.LowCardinality(name, factories,
                            ordinalsValuesSource, order, format, bucketCountThresholds, context, parent, false,
                            subAggCollectMode, showTermDocCountError, metadata);

                }
                final IncludeExclude.OrdinalsFilter filter = includeExclude == null ? null : includeExclude.convertToOrdinalsFilter(format);
                boolean remapGlobalOrds;
                if (cardinality == CardinalityUpperBound.ONE && REMAP_GLOBAL_ORDS != null) {
                    /*
                     * We use REMAP_GLOBAL_ORDS to allow tests to force
                     * specific optimizations but this particular one
                     * is only possible if we're collecting from a single
                     * bucket.
                     */
                    remapGlobalOrds = REMAP_GLOBAL_ORDS.booleanValue();
                } else {
                    remapGlobalOrds = true;
                    if (includeExclude == null &&
                            cardinality == CardinalityUpperBound.ONE &&
                            (factories == AggregatorFactories.EMPTY ||
                                (isAggregationSort(order) == false && subAggCollectMode == SubAggCollectionMode.BREADTH_FIRST))) {
                        /*
                         * We don't need to remap global ords iff this aggregator:
                         *    - has no include/exclude rules AND
                         *    - only collects from a single bucket AND
                         *    - has no sub-aggregator or only sub-aggregator that can be deferred
                         *      ({@link SubAggCollectionMode#BREADTH_FIRST}).
                         */
                         remapGlobalOrds = false;
                    }
                }
                return new GlobalOrdinalsStringTermsAggregator(
                    name,
                    factories,
                    a -> a.new StandardTermsResults(),
                    ordinalsValuesSource,
                    order,
                    format,
                    bucketCountThresholds,
                    filter,
                    context,
                    parent,
                    remapGlobalOrds,
                    subAggCollectMode,
                    showTermDocCountError,
                    cardinality,
                    metadata
                );
            }
        };

        public static ExecutionMode fromString(String value) {
            switch (value) {
                case "global_ordinals":
                    return GLOBAL_ORDINALS;
                case "map":
                    return MAP;
                default:
                    throw new IllegalArgumentException("Unknown `execution_hint`: [" + value + "], expected any of [map, global_ordinals]");
            }
        }

        private final ParseField parseField;

        ExecutionMode(ParseField parseField) {
            this.parseField = parseField;
        }

        abstract Aggregator create(String name,
                                   AggregatorFactories factories,
                                   ValuesSource valuesSource,
                                   BucketOrder order,
                                   DocValueFormat format,
                                   TermsAggregator.BucketCountThresholds bucketCountThresholds,
                                   IncludeExclude includeExclude,
                                   SearchContext context,
                                   Aggregator parent,
                                   SubAggCollectionMode subAggCollectMode,
                                   boolean showTermDocCountError,
                                   CardinalityUpperBound cardinality,
                                   Map<String, Object> metadata) throws IOException;

        @Override
        public String toString() {
            return parseField.getPreferredName();
        }
    }

}
