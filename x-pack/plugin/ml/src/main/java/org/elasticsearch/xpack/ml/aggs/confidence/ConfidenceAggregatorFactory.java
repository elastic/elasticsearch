/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.aggs.confidence;

import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.CardinalityUpperBound;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.xpack.ml.aggs.PCG;

import java.io.IOException;
import java.util.Map;

public class ConfidenceAggregatorFactory extends AggregatorFactory {

    private final int confidenceInterval;
    private final double probability;
    private final Map<String, String> bucketPaths;
    private final boolean keyed;
    private final PCG rng;

    public ConfidenceAggregatorFactory(
        String name,
        int confidenceInterval,
        double probability,
        int seed,
        boolean keyed,
        Map<String, String> bucketPaths,
        AggregationContext context,
        AggregatorFactory parent,
        AggregatorFactories.Builder subFactoriesBuilder,
        Map<String, Object> metadata
    ) throws IOException {
        super(name, context, parent, subFactoriesBuilder, metadata);
        this.confidenceInterval = confidenceInterval;
        this.probability = probability;
        this.rng = new PCG(seed, context.shardRandomSeed());
        this.bucketPaths = bucketPaths;
        this.keyed = keyed;
    }

    @Override
    protected Aggregator createInternal(Aggregator parent, CardinalityUpperBound cardinality, Map<String, Object> metadata)
        throws IOException {
        return new ConfidenceAggregator(
            name,
            factories,
            context,
            parent,
            confidenceInterval,
            probability,
            keyed,
            this.rng.nextInt(),
            bucketPaths,
            metadata
        );
    }

}
