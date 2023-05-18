/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.bucket.sampler.random;

import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.BaseAggregationTestCase;

public class RandomSamplerAggregationBuilderTests extends BaseAggregationTestCase<RandomSamplerAggregationBuilder> {

    @Override
    protected RandomSamplerAggregationBuilder createTestAggregatorBuilder() {
        RandomSamplerAggregationBuilder builder = new RandomSamplerAggregationBuilder(randomAlphaOfLength(10));
        if (randomBoolean()) {
            builder.setSeed(randomInt());
        }
        builder.setProbability(randomFrom(1.0, randomDoubleBetween(0.0, 0.5, false)));
        builder.subAggregation(AggregationBuilders.max(randomAlphaOfLength(10)).field(randomAlphaOfLength(10)));
        return builder;
    }

}
