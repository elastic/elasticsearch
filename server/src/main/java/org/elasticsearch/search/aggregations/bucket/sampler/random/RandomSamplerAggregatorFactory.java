/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.aggregations.bucket.sampler.random;

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
    private final Integer shardSeed;
    private final double probability;
    private final SamplingContext samplingContext;

    RandomSamplerAggregatorFactory(
        String name,
        int seed,
        Integer shardSeed,
        double probability,
        AggregationContext context,
        AggregatorFactory parent,
        AggregatorFactories.Builder subFactories,
        Map<String, Object> metadata
    ) throws IOException {
        super(name, context, parent, subFactories, metadata);
        this.probability = probability;
        this.seed = seed;
        this.samplingContext = new SamplingContext(probability, seed, shardSeed);
        this.shardSeed = shardSeed;
    }

    @Override
    public Optional<SamplingContext> getSamplingContext() {
        return Optional.of(samplingContext);
    }

    @Override
    public Aggregator createInternal(Aggregator parent, CardinalityUpperBound cardinality, Map<String, Object> metadata)
        throws IOException {
        return new RandomSamplerAggregator(name, seed, shardSeed, probability, factories, context, parent, cardinality, metadata);
    }
}
