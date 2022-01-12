/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.aggs.confidence;

import org.apache.lucene.util.hppc.BitMixer;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.CardinalityUpperBound;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.SamplingContext;

import java.io.IOException;
import java.util.Map;

public class ConfidenceAggregatorFactory extends AggregatorFactory {

    private final int confidenceInterval;
    private final boolean keyed;
    private final boolean debug;

    public ConfidenceAggregatorFactory(
        String name,
        int confidenceInterval,
        boolean keyed,
        boolean debug,
        AggregationContext context,
        AggregatorFactory parent,
        AggregatorFactories.Builder subFactoriesBuilder,
        Map<String, Object> metadata
    ) throws IOException {
        super(name, context, parent, subFactoriesBuilder, metadata);
        this.confidenceInterval = confidenceInterval;
        this.keyed = keyed;
        this.debug = debug;
    }

    @Override
    protected Aggregator createInternal(Aggregator parent, CardinalityUpperBound cardinality, Map<String, Object> metadata)
        throws IOException {
        SamplingContext samplingContext = getSamplingContext().orElseThrow(
            () -> new UnsupportedOperationException(
                ConfidenceAggregationBuilder.NAME
                    + " aggregation ["
                    + name()
                    + "] must be within a sampling context, please verify it is nested under a random_sampler aggregation"
            )
        );
        return new ConfidenceAggregator(
            name,
            factories,
            context,
            parent,
            confidenceInterval,
            samplingContext.probability(),
            keyed,
            debug,
            BitMixer.mix(context.shardRandomSeed() ^ samplingContext.seed()),
            metadata
        );
    }

}
