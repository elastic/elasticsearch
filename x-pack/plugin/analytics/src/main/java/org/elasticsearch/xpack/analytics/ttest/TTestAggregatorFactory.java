/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.analytics.ttest;

import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Weight;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.AggregationInitializationException;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.CardinalityUpperBound;
import org.elasticsearch.search.aggregations.support.MultiValuesSource;
import org.elasticsearch.search.aggregations.support.MultiValuesSourceAggregatorFactory;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.Map;

import static org.elasticsearch.xpack.analytics.ttest.TTestAggregationBuilder.A_FIELD;

class TTestAggregatorFactory extends MultiValuesSourceAggregatorFactory {

    private final TTestType testType;
    private final int tails;
    private final Query filterA;
    private final Query filterB;
    private Tuple<Weight, Weight> weights;

    TTestAggregatorFactory(String name, Map<String, ValuesSourceConfig> configs, TTestType testType, int tails,
                           QueryBuilder filterA, QueryBuilder filterB,
                           DocValueFormat format, QueryShardContext queryShardContext, AggregatorFactory parent,
                           AggregatorFactories.Builder subFactoriesBuilder,
                           Map<String, Object> metadata) throws IOException {
        super(name, configs, format, queryShardContext, parent, subFactoriesBuilder, metadata);
        this.testType = testType;
        this.tails = tails;
        this.filterA = filterA == null ? null : filterA.toQuery(queryShardContext);
        this.filterB = filterB == null ? null : filterB.toQuery(queryShardContext);
    }

    @Override
    protected Aggregator createUnmapped(SearchContext searchContext,
                                        Aggregator parent,
                                        Map<String, Object> metadata) throws IOException {
        switch (testType) {
            case PAIRED:
                return new PairedTTestAggregator(name, null, tails, format, searchContext, parent, metadata);
            case HOMOSCEDASTIC:
                return new UnpairedTTestAggregator(name, null, tails, true, this::getWeights, format, searchContext, parent, metadata);
            case HETEROSCEDASTIC:
                return new UnpairedTTestAggregator(name, null, tails, false, this::getWeights, format, searchContext, parent, metadata);
            default:
                throw new IllegalArgumentException("Unsupported t-test type " + testType);
        }
    }

    @Override
    protected Aggregator doCreateInternal(SearchContext searchContext,
                                          Map<String, ValuesSourceConfig> configs,
                                          DocValueFormat format,
                                          Aggregator parent,
                                          CardinalityUpperBound cardinality,
                                          Map<String, Object> metadata) throws IOException {
        MultiValuesSource.NumericMultiValuesSource numericMultiVS
            = new MultiValuesSource.NumericMultiValuesSource(configs, queryShardContext);
        if (numericMultiVS.areValuesSourcesEmpty()) {
            return createUnmapped(searchContext, parent, metadata);
        }
        switch (testType) {
            case PAIRED:
                if (filterA != null || filterB != null) {
                    throw new IllegalArgumentException("Paired t-test doesn't support filters");
                }
                return new PairedTTestAggregator(name, numericMultiVS, tails, format, searchContext, parent, metadata);
            case HOMOSCEDASTIC:
                return new UnpairedTTestAggregator(name, numericMultiVS, tails, true, this::getWeights, format, searchContext, parent,
                    metadata);
            case HETEROSCEDASTIC:
                return new UnpairedTTestAggregator(name, numericMultiVS, tails, false, this::getWeights, format, searchContext,
                    parent, metadata);
            default:
                throw new IllegalArgumentException("Unsupported t-test type " + testType);
        }
    }

    /**
     * Returns the {@link Weight}s for this filters, creating it if
     * necessary. This is done lazily so that the {@link Weight} is only created
     * if the aggregation collects documents reducing the overhead of the
     * aggregation in the case where no documents are collected.
     *
     * Note that as aggregations are initialsed and executed in a serial manner,
     * no concurrency considerations are necessary here.
     */
    public Tuple<Weight, Weight> getWeights() {
        if (weights == null) {
            weights = new Tuple<>(getWeight(filterA), getWeight(filterB));
        }
        return weights;
    }

    public Weight getWeight(Query filter) {
        if (filter != null) {
            IndexSearcher contextSearcher = queryShardContext.searcher();
            try {
                return contextSearcher.createWeight(contextSearcher.rewrite(filter), ScoreMode.COMPLETE_NO_SCORES, 1f);
            } catch (IOException e) {
                throw new AggregationInitializationException("Failed to initialize filter", e);
            }
        }
        return null;
    }

    @Override
    public String getStatsSubtype() {
        return configs.get(A_FIELD.getPreferredName()).valueSourceType().typeName();
    }
}
