/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.bucket.sampler.random;

import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Weight;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.CardinalityUpperBound;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.SamplingContext;

import java.io.IOException;
import java.util.Map;
import java.util.Optional;

public class RandomSamplerAggregatorFactory extends AggregatorFactory {

    private final int seed;
    private final double probability;
    private final SamplingContext samplingContext;
    private Weight weight;

    RandomSamplerAggregatorFactory(
        String name,
        int seed,
        double probability,
        AggregationContext context,
        AggregatorFactory parent,
        AggregatorFactories.Builder subFactories,
        Map<String, Object> metadata
    ) throws IOException {
        super(name, context, parent, subFactories, metadata);
        this.probability = probability;
        this.seed = seed;
        this.samplingContext = new SamplingContext(probability, seed);
    }

    @Override
    public Optional<SamplingContext> getSamplingContext() {
        return Optional.of(samplingContext);
    }

    @Override
    public Aggregator createInternal(Aggregator parent, CardinalityUpperBound cardinality, Map<String, Object> metadata)
        throws IOException {
        return new RandomSamplerAggregator(name, seed, probability, this::getWeight, factories, context, parent, cardinality, metadata);
    }

    /**
     * This creates the query weight which will be used in the aggregator.
     *
     * This weight is a boolean query between {@link RandomSamplingQuery} and the configured top level query of the search. This allows
     * the aggregation to iterate the documents directly, thus sampling in the background instead of the foreground.
     * @return weight to be used, is cached for additional usages
     * @throws IOException when building the weight or queries fails;
     */
    private Weight getWeight() throws IOException {
        if (weight == null) {
            RandomSamplingQuery query = new RandomSamplingQuery(probability, seed, context.shardRandomSeed());
            BooleanQuery booleanQuery = new BooleanQuery.Builder().add(query, BooleanClause.Occur.FILTER)
                .add(context.query(), BooleanClause.Occur.FILTER)
                .build();
            weight = context.searcher().createWeight(context.searcher().rewrite(booleanQuery), ScoreMode.COMPLETE_NO_SCORES, 1f);
        }
        return weight;
    }

}
