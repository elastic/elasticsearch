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
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.lucene.index.FilterableTermsEnum;
import org.elasticsearch.common.lucene.index.FreqTermsEnum;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.AggregationExecutionException;
import org.elasticsearch.search.aggregations.Aggregator;
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

public class SignificantTermsAggregatorFactory extends ValuesSourceAggregatorFactory
    implements Releasable {
    private static final DeprecationLogger deprecationLogger = new DeprecationLogger(
            LogManager.getLogger(SignificantTermsAggregatorFactory.class));

    private final IncludeExclude includeExclude;
    private final String executionHint;
    private String indexedFieldName;
    private MappedFieldType fieldType;
    private FilterableTermsEnum termsEnum;
    private int numberOfAggregatorsCreated;
    final Query filter;
    private final int supersetNumDocs;
    private final TermsAggregator.BucketCountThresholds bucketCountThresholds;
    private final SignificanceHeuristic significanceHeuristic;

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
                                    SignificantTermsAggregatorFactory sigTermsFactory,
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
                    significanceHeuristic, sigTermsFactory, metadata);

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
                                    SignificantTermsAggregatorFactory sigTermsFactory,
                                    Map<String, Object> metadata) throws IOException {

                if ((includeExclude != null) && (includeExclude.isRegexBased())) {
                    throw new IllegalArgumentException("Aggregation [" + name + "] cannot support regular expression style include/exclude "
                        + "settings as they can only be applied to string fields. Use an array of numeric " +
                        "values for include/exclude clauses used to filter numeric fields");
                }

                if (((ValuesSource.Numeric) valuesSource).isFloatingPoint()) {
                    throw new UnsupportedOperationException("No support for examining floating point numerics");
                }

                IncludeExclude.LongFilter longFilter = null;
                if (includeExclude != null) {
                    longFilter = includeExclude.convertToLongFilter(format);
                }

                return new SignificantLongTermsAggregator(name, factories, (ValuesSource.Numeric) valuesSource, format,
                    bucketCountThresholds, context, parent, significanceHeuristic, sigTermsFactory, longFilter, metadata);

            }
        };
    }

    SignificantTermsAggregatorFactory(String name,
                                      ValuesSourceConfig config,
                                      IncludeExclude includeExclude,
                                      String executionHint,
                                      QueryBuilder filterBuilder,
                                      TermsAggregator.BucketCountThresholds bucketCountThresholds,
                                      SignificanceHeuristic significanceHeuristic,
                                      QueryShardContext queryShardContext,
                                      AggregatorFactory parent,
                                      AggregatorFactories.Builder subFactoriesBuilder,
                                      Map<String, Object> metadata) throws IOException {
        super(name, config, queryShardContext, parent, subFactoriesBuilder, metadata);

        if (config.unmapped() == false) {
            if (config.fieldContext().fieldType().isSearchable() == false) {
                throw new IllegalArgumentException("SignificantText aggregation requires fields to be searchable, but ["
                    + config.fieldContext().fieldType().name() + "] is not");
            }

            this.fieldType = config.fieldContext().fieldType();
            this.indexedFieldName = fieldType.name();
        }

        this.includeExclude = includeExclude;
        this.executionHint = executionHint;
        this.filter = filterBuilder == null
                ? null
                : filterBuilder.toQuery(queryShardContext);
        IndexSearcher searcher = queryShardContext.searcher();
        this.supersetNumDocs = filter == null
                // Important - need to use the doc count that includes deleted docs
                // or we have this issue: https://github.com/elastic/elasticsearch/issues/7951
                ? searcher.getIndexReader().maxDoc()
                : searcher.count(filter);
        this.bucketCountThresholds = bucketCountThresholds;
        this.significanceHeuristic = significanceHeuristic;
    }

    /**
     * Get the number of docs in the superset.
     */
    long getSupersetNumDocs() {
        return supersetNumDocs;
    }

    private FilterableTermsEnum getTermsEnum(String field) throws IOException {
        if (termsEnum != null) {
            return termsEnum;
        }
        IndexReader reader = queryShardContext.getIndexReader();
        if (numberOfAggregatorsCreated > 1) {
            termsEnum = new FreqTermsEnum(reader, field, true, false, filter, queryShardContext.bigArrays());
        } else {
            termsEnum = new FilterableTermsEnum(reader, indexedFieldName, PostingsEnum.NONE, filter);
        }
        return termsEnum;
    }

    private long getBackgroundFrequency(String value) throws IOException {
        // fieldType can be null if the field is unmapped, but theoretically this method should only be called
        // when constructing buckets.  Assert to ensure this is the case
        // TODO this is a bad setup and it should be refactored
        assert fieldType != null;
        Query query = fieldType.termQuery(value, queryShardContext);
        if (query instanceof TermQuery) {
            // for types that use the inverted index, we prefer using a caching terms
            // enum that will do a better job at reusing index inputs
            Term term = ((TermQuery) query).getTerm();
            FilterableTermsEnum termsEnum = getTermsEnum(term.field());
            if (termsEnum.seekExact(term.bytes())) {
                return termsEnum.docFreq();
            } else {
                return 0;
            }
        }
        // otherwise do it the naive way
        if (filter != null) {
            query = new BooleanQuery.Builder()
                    .add(query, Occur.FILTER)
                    .add(filter, Occur.FILTER)
                    .build();
        }
        return queryShardContext.searcher().count(query);
    }

    long getBackgroundFrequency(BytesRef termBytes) throws IOException {
        String value = config.format().format(termBytes).toString();
        return getBackgroundFrequency(value);
    }

    long getBackgroundFrequency(long termNum) throws IOException {
        String value = config.format().format(termNum).toString();
        return getBackgroundFrequency(value);
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
    protected Aggregator doCreateInternal(ValuesSource valuesSource,
                                            SearchContext searchContext,
                                            Aggregator parent,
                                            boolean collectsFromSingleBucket,
                                            Map<String, Object> metadata) throws IOException {
        if (collectsFromSingleBucket == false) {
            return asMultiBucketAggregator(this, searchContext, parent);
        }

        AggregatorSupplier aggregatorSupplier = queryShardContext.getValuesSourceRegistry().getAggregator(config.valueSourceType(),
            SignificantTermsAggregationBuilder.NAME);
        if (aggregatorSupplier instanceof SignificantTermsAggregatorSupplier == false) {
            throw new AggregationExecutionException("Registry miss-match - expected SignificantTermsAggregatorSupplier, found [" +
                aggregatorSupplier.getClass().toString() + "]");
        }

        numberOfAggregatorsCreated++;
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

        SignificantTermsAggregatorSupplier sigTermsAggregatorSupplier = (SignificantTermsAggregatorSupplier) aggregatorSupplier;

        // TODO we should refactor so that we don't need to use this Factory as a singleton (e.g. stop passing `this` to the aggregators)
        return sigTermsAggregatorSupplier.build(name, factories, valuesSource, config.format(),
            bucketCountThresholds, includeExclude, executionHint, searchContext, parent,
            significanceHeuristic, this, metadata);
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
                              SignificantTermsAggregatorFactory termsAggregatorFactory,
                              Map<String, Object> metadata) throws IOException {

                final IncludeExclude.StringFilter filter = includeExclude == null ? null : includeExclude.convertToStringFilter(format);
                return new SignificantStringTermsAggregator(name, factories, valuesSource, format, bucketCountThresholds, filter,
                        aggregationContext, parent, significanceHeuristic, termsAggregatorFactory, metadata);

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
                              SignificantTermsAggregatorFactory termsAggregatorFactory,
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
                return new GlobalOrdinalsSignificantTermsAggregator(name, factories,
                        (ValuesSource.Bytes.WithOrdinals.FieldData) valuesSource, format, bucketCountThresholds, filter,
                        aggregationContext, parent, remapGlobalOrd, significanceHeuristic, termsAggregatorFactory, metadata);

            }
        };

        public static ExecutionMode fromString(String value, final DeprecationLogger deprecationLogger) {
            if ("global_ordinals".equals(value)) {
                return GLOBAL_ORDINALS;
            } else if ("global_ordinals_hash".equals(value)) {
                deprecationLogger.deprecatedAndMaybeLog("global_ordinals_hash",
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
                                   SignificantTermsAggregatorFactory termsAggregatorFactory,
                                   Map<String, Object> metadata) throws IOException;

        @Override
        public String toString() {
            return parseField.getPreferredName();
        }
    }

    @Override
    public void close() {
        try {
            if (termsEnum instanceof Releasable) {
                ((Releasable) termsEnum).close();
            }
        } finally {
            termsEnum = null;
        }
    }
}
