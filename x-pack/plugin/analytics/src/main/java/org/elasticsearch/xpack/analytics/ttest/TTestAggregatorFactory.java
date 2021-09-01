/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.analytics.ttest;

import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Weight;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.AggregationInitializationException;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.CardinalityUpperBound;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.MultiValuesSource;
import org.elasticsearch.search.aggregations.support.MultiValuesSourceAggregatorFactory;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;

import java.io.IOException;
import java.util.Map;

import static org.elasticsearch.xpack.analytics.ttest.TTestAggregationBuilder.A_FIELD;

class TTestAggregatorFactory extends MultiValuesSourceAggregatorFactory {

    private final TTestType testType;
    private final int tails;
    private final Query filterA;
    private final Query filterB;
    private Tuple<Weight, Weight> weights;

    TTestAggregatorFactory(
        String name,
        Map<String, ValuesSourceConfig> configs,
        TTestType testType,
        int tails,
        QueryBuilder filterA,
        QueryBuilder filterB,
        DocValueFormat format,
        AggregationContext context,
        AggregatorFactory parent,
        AggregatorFactories.Builder subFactoriesBuilder,
        Map<String, Object> metadata
    ) throws IOException {
        super(name, configs, format, context, parent, subFactoriesBuilder, metadata);
        this.testType = testType;
        this.tails = tails;
        this.filterA = filterA == null ? null : context.buildQuery(filterA);
        this.filterB = filterB == null ? null : context.buildQuery(filterB);
    }

    @Override
    protected Aggregator createUnmapped(Aggregator parent, Map<String, Object> metadata) throws IOException {
        switch (testType) {
            case PAIRED:
                return new PairedTTestAggregator(name, null, tails, format, context, parent, metadata);
            case HOMOSCEDASTIC:
                return new UnpairedTTestAggregator(name, null, tails, true, this::getWeights, format, context, parent, metadata);
            case HETEROSCEDASTIC:
                return new UnpairedTTestAggregator(name, null, tails, false, this::getWeights, format, context, parent, metadata);
            default:
                throw new IllegalArgumentException("Unsupported t-test type " + testType);
        }
    }

    @Override
    protected Aggregator doCreateInternal(
        Map<String, ValuesSourceConfig> configs,
        DocValueFormat format,
        Aggregator parent,
        CardinalityUpperBound cardinality,
        Map<String, Object> metadata
    ) throws IOException {
        MultiValuesSource.NumericMultiValuesSource numericMultiVS = new MultiValuesSource.NumericMultiValuesSource(configs);
        if (numericMultiVS.areValuesSourcesEmpty()) {
            return createUnmapped(parent, metadata);
        }
        switch (testType) {
            case PAIRED:
                if (filterA != null || filterB != null) {
                    throw new IllegalArgumentException("Paired t-test doesn't support filters");
                }
                return new PairedTTestAggregator(name, numericMultiVS, tails, format, context, parent, metadata);
            case HOMOSCEDASTIC:
                return new UnpairedTTestAggregator(name, numericMultiVS, tails, true, this::getWeights, format, context, parent, metadata);
            case HETEROSCEDASTIC:
                return new UnpairedTTestAggregator(name, numericMultiVS, tails, false, this::getWeights, format, context, parent, metadata);
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
            IndexSearcher contextSearcher = context.searcher();
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
