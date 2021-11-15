/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.aggs.betterrandom;

import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.CardinalityUpperBound;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.xpack.ml.aggs.PCG;

import java.io.IOException;
import java.util.Map;

public class RandomSamplerAggregatorFactory extends AggregatorFactory {

    private final PCG rng;
    private final double probability;

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
        this.rng = new PCG(seed, context.shardRandomSeed());
    }

    @Override
    public Aggregator createInternal(Aggregator parent, CardinalityUpperBound cardinality, Map<String, Object> metadata)
        throws IOException {
        return new RandomSamplerAggregator(name, rng.nextInt(), probability, factories, context, parent, cardinality, metadata);
    }

}
