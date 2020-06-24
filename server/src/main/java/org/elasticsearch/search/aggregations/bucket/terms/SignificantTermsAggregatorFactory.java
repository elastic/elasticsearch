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
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.AggregationExecutionException;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.Aggregator.SubAggCollectionMode;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.NonCollectingAggregator;
import org.elasticsearch.search.aggregations.bucket.BucketUtils;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregator.BucketCountThresholds;
import org.elasticsearch.search.aggregations.bucket.terms.heuristic.SignificanceHeuristic;
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

public class SignificantTermsAggregatorFactory extends ValuesSourceAggregatorFactory {
    private static final DeprecationLogger deprecationLogger = new DeprecationLogger(
            LogManager.getLogger(SignificantTermsAggregatorFactory.class));

    static void registerAggregators(ValuesSourceRegistry.Builder builder) {
        builder.register(SignificantTermsAggregationBuilder.NAME,
            List.of(CoreValuesSourceType.BYTES, CoreValuesSourceType.IP),
            SignificantTermsAggregatorFactory.bytesSupplier());

        builder.register(SignificantTermsAggregationBuilder.NAME,
            List.of(CoreValuesSourceType.DATE, CoreValuesSourceType.BOOLEAN, CoreValuesSourceType.NUMERIC),
            SignificantTermsAggregatorFactory.numericSupplier());
    }

    /**
     * This supplier is used for all the field types that should be aggregated as bytes/strings,
     * including those that need global ordinals
     */
    private static SignificantTermsAggregatorSupplier bytesSupplier() {
        return new SignificantTermsAggregatorSupplier() {
            @Override
            public Aggregator build(String name,
                                    AggregatorFactories factories,
                                    ValuesSource valuesSource,
                                    DocValueFormat format,
                                    TermsAggregator.BucketCountThresholds bucketCountThresholds,
                                    IncludeExclude includeExclude,
                                    String executionHint,
                                    SearchContext context,
                                    Aggregator parent,
                                    SignificanceHeuristic significanceHeuristic,
                                    SignificanceLookup lookup,
                                    boolean collectsFromSingleBucket,
                                    Map<String, Object> metadata) throws IOException {

                ExecutionMode execution = null;
                if (executionHint != null) {
                    execution = ExecutionMode.fromString(executionHint, deprecationLogger);
                }
                if (valuesSource instanceof ValuesSource.Bytes.WithOrdinals == false) {
                    execution = ExecutionMode.MAP;
                }
                if (execution == null) {
                    execution = ExecutionMode.GLOBAL_ORDINALS;
                }

                if ((includeExclude != null) && (includeExclude.isRegexBased()) && format != DocValueFormat.RAW) {
                    throw new IllegalArgumentException("Aggregation [" + name + "] cannot support regular expression style "
                        + "include/exclude settings as they can only be applied to string fields. Use an array of values for "
                        + "include/exclude clauses");
                }

                return execution.create(name, factories, valuesSource, format, bucketCountThresholds, includeExclude, context, parent,
                    significanceHeuristic, lookup, collectsFromSingleBucket, metadata);
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
            public Aggregator build(String name,
                                    AggregatorFactories factories,
                                    ValuesSource valuesSource,
                                    DocValueFormat format,
                                    TermsAggregator.BucketCountThresholds bucketCountThresholds,
                                    IncludeExclude includeExclude,
                                    String executionHint,
                                    SearchContext context,
                                    Aggregator parent,
                                    SignificanceHeuristic significanceHeuristic,
                                    SignificanceLookup lookup,
                                    boolean collectsFromSingleBucket,
                                    Map<String, Object> metadata) throws IOException {

                if ((includeExclude != null) && (includeExclude.isRegexBased())) {
                    throw new IllegalArgumentException("Aggregation [" + name + "] cannot support regular expression style include/exclude "
                        + "settings as they can only be applied to string fields. Use an array of numeric " +
                        "values for include/exclude clauses used to filter numeric fields");
                }

                ValuesSource.Numeric numericValuesSource = (ValuesSource.Numeric) valuesSource;
                if (numericValuesSource.isFloatingPoint()) {
                    throw new UnsupportedOperationException("No support for examining floating point numerics");
                }

                IncludeExclude.LongFilter longFilter = null;
                if (includeExclude != null) {
                    longFilter = includeExclude.convertToLongFilter(format);
                }

                return new NumericTermsAggregator(name, factories,
                    agg -> agg.new SignificantLongTermsResults(lookup, significanceHeuristic, collectsFromSingleBucket),
                    numericValuesSource, format, null, bucketCountThresholds, context, parent, SubAggCollectionMode.BREADTH_FIRST,
                    longFilter, collectsFromSingleBucket, metadata);
            }
        };
    }

    private final IncludeExclude includeExclude;
    private final String executionHint;
    private final QueryBuilder backgroundFilter;
    private final TermsAggregator.BucketCountThresholds bucketCountThresholds;
    private final SignificanceHeuristic significanceHeuristic;

    SignificantTermsAggregatorFactory(String name,
                                      ValuesSourceConfig config,
                                      IncludeExclude includeExclude,
                                      String executionHint,
                                      QueryBuilder backgroundFilter,
                                      TermsAggregator.BucketCountThresholds bucketCountThresholds,
                                      SignificanceHeuristic significanceHeuristic,
                                      QueryShardContext queryShardContext,
                                      AggregatorFactory parent,
                                      AggregatorFactories.Builder subFactoriesBuilder,
                                      Map<String, Object> metadata) throws IOException {
        super(name, config, queryShardContext, parent, subFactoriesBuilder, metadata);

        if (config.hasValues()) {
            if (config.fieldContext().fieldType().isSearchable() == false) {
                throw new IllegalArgumentException("SignificantText aggregation requires fields to be searchable, but ["
                    + config.fieldContext().fieldType().name() + "] is not");
            }
        }

        this.includeExclude = includeExclude;
        this.executionHint = executionHint;
        this.backgroundFilter = backgroundFilter;
        this.bucketCountThresholds = bucketCountThresholds;
        this.significanceHeuristic = significanceHeuristic;
    }

    @Override
    protected Aggregator createUnmapped(SearchContext searchContext,
                                            Aggregator parent,
                                            Map<String, Object> metadata) throws IOException {
        final InternalAggregation aggregation = new UnmappedSignificantTerms(name, bucketCountThresholds.getRequiredSize(),
                bucketCountThresholds.getMinDocCount(), metadata);
        return new NonCollectingAggregator(name, searchContext, parent, metadata) {
            @Override
            public InternalAggregation buildEmptyAggregation() {
                return aggregation;
            }
        };
    }

    @Override
    protected Aggregator doCreateInternal(SearchContext searchContext,
                                          Aggregator parent,
                                          boolean collectsFromSingleBucket,
                                          Map<String, Object> metadata) throws IOException {
        AggregatorSupplier aggregatorSupplier = queryShardContext.getValuesSourceRegistry().getAggregator(config,
            SignificantTermsAggregationBuilder.NAME);
        if (aggregatorSupplier instanceof SignificantTermsAggregatorSupplier == false) {
            throw new AggregationExecutionException("Registry miss-match - expected SignificantTermsAggregatorSupplier, found [" +
                aggregatorSupplier.getClass().toString() + "]");
        }
        SignificantTermsAggregatorSupplier sigTermsAggregatorSupplier = (SignificantTermsAggregatorSupplier) aggregatorSupplier;

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

        SignificanceLookup lookup = new SignificanceLookup(
            queryShardContext,
            config.fieldContext().fieldType(),
            config.format(),
            backgroundFilter
        );

        return sigTermsAggregatorSupplier.build(name, factories, config.getValuesSource(), config.format(),
            bucketCountThresholds, includeExclude, executionHint, searchContext, parent,
            significanceHeuristic, lookup, collectsFromSingleBucket, metadata);
    }

    public enum ExecutionMode {

        MAP(new ParseField("map")) {

            @Override
            Aggregator create(String name,
                              AggregatorFactories factories,
                              ValuesSource valuesSource,
                              DocValueFormat format,
                              TermsAggregator.BucketCountThresholds bucketCountThresholds,
                              IncludeExclude includeExclude,
                              SearchContext aggregationContext,
                              Aggregator parent,
                              SignificanceHeuristic significanceHeuristic,
                              SignificanceLookup lookup,
                              boolean collectsFromSingleBucket,
                              Map<String, Object> metadata) throws IOException {

                final IncludeExclude.StringFilter filter = includeExclude == null ? null : includeExclude.convertToStringFilter(format);
                return new MapStringTermsAggregator(
                    name,
                    factories,
                    new MapStringTermsAggregator.ValuesSourceCollectorSource(valuesSource),
                    a -> a.new SignificantTermsResults(lookup, significanceHeuristic, collectsFromSingleBucket),
                    null,
                    format,
                    bucketCountThresholds,
                    filter,
                    aggregationContext,
                    parent,
                    SubAggCollectionMode.BREADTH_FIRST,
                    false,
                    collectsFromSingleBucket,
                    metadata
                );

            }

        },
        GLOBAL_ORDINALS(new ParseField("global_ordinals")) {

            @Override
            Aggregator create(String name,
                              AggregatorFactories factories,
                              ValuesSource valuesSource,
                              DocValueFormat format,
                              TermsAggregator.BucketCountThresholds bucketCountThresholds,
                              IncludeExclude includeExclude,
                              SearchContext aggregationContext,
                              Aggregator parent,
                              SignificanceHeuristic significanceHeuristic,
                              SignificanceLookup lookup,
                              boolean collectsFromSingleBucket,
                              Map<String, Object> metadata) throws IOException {

                final IncludeExclude.OrdinalsFilter filter = includeExclude == null ? null : includeExclude.convertToOrdinalsFilter(format);
                boolean remapGlobalOrd = true;
                if (Aggregator.descendsFromBucketAggregator(parent) == false &&
                        factories == AggregatorFactories.EMPTY &&
                        includeExclude == null) {
                    /**
                     * We don't need to remap global ords iff this aggregator:
                     *    - is not a child of a bucket aggregator AND
                     *    - has no include/exclude rules AND
                     *    - has no sub-aggregator
                     **/
                    remapGlobalOrd = false;
                }

                return new GlobalOrdinalsStringTermsAggregator(
                    name,
                    factories,
                    a -> a.new SignificantTermsResults(lookup, significanceHeuristic, collectsFromSingleBucket),
                    (ValuesSource.Bytes.WithOrdinals.FieldData) valuesSource,
                    null,
                    format,
                    bucketCountThresholds,
                    filter,
                    aggregationContext,
                    parent,
                    remapGlobalOrd,
                    SubAggCollectionMode.BREADTH_FIRST,
                    false,
                    collectsFromSingleBucket,
                    metadata
                );
            }
        };

        public static ExecutionMode fromString(String value, final DeprecationLogger deprecationLogger) {
            if ("global_ordinals".equals(value)) {
                return GLOBAL_ORDINALS;
            } else if ("global_ordinals_hash".equals(value)) {
                deprecationLogger.deprecate("global_ordinals_hash",
                    "global_ordinals_hash is deprecated. Please use [global_ordinals] instead.");
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

        abstract Aggregator create(String name,
                                   AggregatorFactories factories,
                                   ValuesSource valuesSource,
                                   DocValueFormat format,
                                   TermsAggregator.BucketCountThresholds bucketCountThresholds,
                                   IncludeExclude includeExclude,
                                   SearchContext aggregationContext,
                                   Aggregator parent,
                                   SignificanceHeuristic significanceHeuristic,
                                   SignificanceLookup lookup,
                                   boolean collectsFromSingleBucket,
                                   Map<String, Object> metadata) throws IOException;

        @Override
        public String toString() {
            return parseField.getPreferredName();
        }
    }
}
