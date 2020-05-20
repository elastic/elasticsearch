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

import org.apache.logging.log4j.LogManager;
import org.apache.lucene.search.IndexSearcher;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.AggregationExecutionException;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.Aggregator.SubAggCollectionMode;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalOrder;
import org.elasticsearch.search.aggregations.InternalOrder.CompoundOrder;
import org.elasticsearch.search.aggregations.NonCollectingAggregator;
import org.elasticsearch.search.aggregations.bucket.BucketUtils;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregator.BucketCountThresholds;
import org.elasticsearch.search.aggregations.support.AggregatorSupplier;
import org.elasticsearch.search.aggregations.support.CoreValuesSourceType;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregatorFactory;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.search.aggregations.support.ValuesSourceRegistry;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class TermsAggregatorFactory extends ValuesSourceAggregatorFactory {
    private static final DeprecationLogger deprecationLogger = new DeprecationLogger(LogManager.getLogger(TermsAggregatorFactory.class));

    static Boolean REMAP_GLOBAL_ORDS, COLLECT_SEGMENT_ORDS;

    private final BucketOrder order;
    private final IncludeExclude includeExclude;
    private final String executionHint;
    private final SubAggCollectionMode collectMode;
    private final TermsAggregator.BucketCountThresholds bucketCountThresholds;
    private final boolean showTermDocCountError;

    static void registerAggregators(ValuesSourceRegistry.Builder builder) {
        builder.register(TermsAggregationBuilder.NAME,
            List.of(CoreValuesSourceType.BYTES, CoreValuesSourceType.IP),
            TermsAggregatorFactory.bytesSupplier());

        builder.register(TermsAggregationBuilder.NAME,
            List.of(CoreValuesSourceType.DATE, CoreValuesSourceType.BOOLEAN, CoreValuesSourceType.NUMERIC),
            TermsAggregatorFactory.numericSupplier());
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
                                    boolean collectsFromSingleBucket,
                                    Map<String, Object> metadata) throws IOException {
                assert collectsFromSingleBucket;

                ExecutionMode execution = null;
                if (executionHint != null) {
                    execution = ExecutionMode.fromString(executionHint, deprecationLogger);
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
                    subAggCollectMode = SubAggCollectionMode.DEPTH_FIRST;
                    // TODO can we remove concept of AggregatorFactories.EMPTY?
                    if (factories != AggregatorFactories.EMPTY) {
                        subAggCollectMode = subAggCollectionMode(bucketCountThresholds.getShardSize(), maxOrd);
                    }
                }

                if ((includeExclude != null) && (includeExclude.isRegexBased()) && format != DocValueFormat.RAW) {
                    // TODO this exception message is not really accurate for the string case.  It's really disallowing regex + formatter
                    throw new AggregationExecutionException("Aggregation [" + name + "] cannot support regular expression style "
                        + "include/exclude settings as they can only be applied to string fields. Use an array of values for "
                        + "include/exclude clauses");
                }

                // TODO: [Zach] we might want refactor and remove ExecutionMode#create(), moving that logic outside the enum
                return execution.create(name, factories, valuesSource, order, format, bucketCountThresholds, includeExclude,
                    context, parent, subAggCollectMode, showTermDocCountError, metadata);

            }

            @Override
            public boolean needsToCollectFromSingleBucket() {
                return true;
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
                                    boolean collectsFromSingleBucket,
                                    Map<String, Object> metadata) throws IOException {

                if ((includeExclude != null) && (includeExclude.isRegexBased())) {
                    throw new AggregationExecutionException("Aggregation [" + name + "] cannot support regular expression style "
                        + "include/exclude settings as they can only be applied to string fields. Use an array of numeric values for "
                        + "include/exclude clauses used to filter numeric fields");
                }

                IncludeExclude.LongFilter longFilter = null;
                if (subAggCollectMode == null) {
                    // TODO can we remove concept of AggregatorFactories.EMPTY?
                    if (factories != AggregatorFactories.EMPTY) {
                        subAggCollectMode = subAggCollectionMode(bucketCountThresholds.getShardSize(), -1);
                    } else {
                        subAggCollectMode = SubAggCollectionMode.DEPTH_FIRST;
                    }
                }
                if (((ValuesSource.Numeric) valuesSource).isFloatingPoint()) {
                    if (includeExclude != null) {
                        longFilter = includeExclude.convertToDoubleFilter();
                    }
                    return new DoubleTermsAggregator(name, factories, (ValuesSource.Numeric) valuesSource, format, order,
                        bucketCountThresholds, context, parent, subAggCollectMode, showTermDocCountError, longFilter,
                        collectsFromSingleBucket, metadata);
                }
                if (includeExclude != null) {
                    longFilter = includeExclude.convertToLongFilter(format);
                }
                return new LongTermsAggregator(name, factories, (ValuesSource.Numeric) valuesSource, format, order,
                    bucketCountThresholds, context, parent, subAggCollectMode, showTermDocCountError, longFilter,
                    collectsFromSingleBucket, metadata);
            }

            @Override
            public boolean needsToCollectFromSingleBucket() {
                return false;
            }
        };
    }

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
    protected Aggregator doCreateInternal(ValuesSource valuesSource,
                                          SearchContext searchContext,
                                          Aggregator parent,
                                          boolean collectsFromSingleBucket,
                                          Map<String, Object> metadata) throws IOException {
        AggregatorSupplier aggregatorSupplier = queryShardContext.getValuesSourceRegistry().getAggregator(config.valueSourceType(),
            TermsAggregationBuilder.NAME);
        if (aggregatorSupplier instanceof TermsAggregatorSupplier == false) {
            throw new AggregationExecutionException("Registry miss-match - expected TermsAggregatorSupplier, found [" +
                aggregatorSupplier.getClass().toString() + "]");
        }

        TermsAggregatorSupplier termsAggregatorSupplier = (TermsAggregatorSupplier) aggregatorSupplier;
        if (collectsFromSingleBucket == false && termsAggregatorSupplier.needsToCollectFromSingleBucket()) {
            return asMultiBucketAggregator(this, searchContext, parent);
        }

        BucketCountThresholds bucketCountThresholds = new BucketCountThresholds(this.bucketCountThresholds);
        if (InternalOrder.isKeyOrder(order) == false
            && bucketCountThresholds.getShardSize() == TermsAggregationBuilder.DEFAULT_BUCKET_COUNT_THRESHOLDS.getShardSize()) {
            // The user has not made a shardSize selection. Use default
            // heuristic to avoid any wrong-ranking caused by distributed
            // counting
            bucketCountThresholds.setShardSize(BucketUtils.suggestShardSideQueueSize(bucketCountThresholds.getRequiredSize()));
        }
        bucketCountThresholds.ensureValidity();

        return termsAggregatorSupplier.build(name, factories, valuesSource, order, config.format(),
            bucketCountThresholds, includeExclude, executionHint, searchContext, parent, collectMode,
            showTermDocCountError, collectsFromSingleBucket, metadata);
    }

    // return the SubAggCollectionMode that this aggregation should use based on the expected size
    // and the cardinality of the field
    static SubAggCollectionMode subAggCollectionMode(int expectedSize, long maxOrd) {
        if (expectedSize == Integer.MAX_VALUE) {
            // return all buckets
            return SubAggCollectionMode.DEPTH_FIRST;
        }
        if (maxOrd == -1 || maxOrd > expectedSize) {
            // use breadth_first if the cardinality is bigger than the expected size or unknown (-1)
            return SubAggCollectionMode.BREADTH_FIRST;
        }
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
                              Map<String, Object> metadata) throws IOException {
                final IncludeExclude.StringFilter filter = includeExclude == null ? null : includeExclude.convertToStringFilter(format);
                return new StringTermsAggregator(name, factories, valuesSource, order, format, bucketCountThresholds, filter,
                        context, parent, subAggCollectMode, showTermDocCountError, metadata);
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
                              Map<String, Object> metadata) throws IOException {

                final long maxOrd = getMaxOrd(valuesSource, context.searcher());
                assert maxOrd != -1;
                final double ratio = maxOrd / ((double) context.searcher().getIndexReader().numDocs());

                assert valuesSource instanceof ValuesSource.Bytes.WithOrdinals;
                ValuesSource.Bytes.WithOrdinals ordinalsValuesSource = (ValuesSource.Bytes.WithOrdinals) valuesSource;

                if (factories == AggregatorFactories.EMPTY &&
                        includeExclude == null &&
                        Aggregator.descendsFromBucketAggregator(parent) == false &&
                        ordinalsValuesSource.supportsGlobalOrdinalsMapping() &&
                        // we use the static COLLECT_SEGMENT_ORDS to allow tests to force specific optimizations
                        (COLLECT_SEGMENT_ORDS!= null ? COLLECT_SEGMENT_ORDS.booleanValue() : ratio <= 0.5 && maxOrd <= 2048)) {
                    /**
                     * We can use the low cardinality execution mode iff this aggregator:
                     *  - has no sub-aggregator AND
                     *  - is not a child of a bucket aggregator AND
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
                if (REMAP_GLOBAL_ORDS != null) {
                    // We use REMAP_GLOBAL_ORDS to allow tests to force specific optimizations
                    remapGlobalOrds = REMAP_GLOBAL_ORDS.booleanValue();
                } else {
                    remapGlobalOrds = true;
                    if (includeExclude == null &&
                            Aggregator.descendsFromBucketAggregator(parent) == false &&
                            (factories == AggregatorFactories.EMPTY ||
                                (isAggregationSort(order) == false && subAggCollectMode == SubAggCollectionMode.BREADTH_FIRST))) {
                        /**
                         * We don't need to remap global ords iff this aggregator:
                         *    - has no include/exclude rules AND
                         *    - is not a child of a bucket aggregator AND
                         *    - has no sub-aggregator or only sub-aggregator that can be deferred
                         *      ({@link SubAggCollectionMode#BREADTH_FIRST}).
                         **/
                         remapGlobalOrds = false;
                    }
                }
                return new GlobalOrdinalsStringTermsAggregator(name, factories, ordinalsValuesSource, order,
                        format, bucketCountThresholds, filter, context, parent, remapGlobalOrds, subAggCollectMode, showTermDocCountError,
                        metadata);
            }
        };

        public static ExecutionMode fromString(String value, final DeprecationLogger deprecationLogger) {
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
                                   Map<String, Object> metadata) throws IOException;

        @Override
        public String toString() {
            return parseField.getPreferredName();
        }
    }

}
