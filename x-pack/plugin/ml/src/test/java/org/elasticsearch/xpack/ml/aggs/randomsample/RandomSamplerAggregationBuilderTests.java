/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.aggs.randomsample;

import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.BaseAggregationTestCase;
import org.elasticsearch.xpack.ml.MachineLearning;

import java.util.Collection;
import java.util.Collections;

public class RandomSamplerAggregationBuilderTests extends BaseAggregationTestCase<RandomSamplerAggregationBuilder> {

    @Override
    protected Collection<Class<? extends Plugin>> getExtraPlugins() {
        return Collections.singletonList(MachineLearning.class);
    }

    @Override
    protected RandomSamplerAggregationBuilder createTestAggregatorBuilder() {
        RandomSamplerAggregationBuilder builder = new RandomSamplerAggregationBuilder(randomAlphaOfLength(10));
        if (randomBoolean()) {
            builder.setSeed(randomInt());
        }
        builder.subAggregation(AggregationBuilders.max(randomAlphaOfLength(10)).field(randomAlphaOfLength(10)));
        return builder;
    }

}
