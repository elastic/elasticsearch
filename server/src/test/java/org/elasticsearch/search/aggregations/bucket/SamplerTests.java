/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.bucket;

import org.elasticsearch.search.aggregations.BaseAggregationTestCase;
import org.elasticsearch.search.aggregations.bucket.sampler.SamplerAggregationBuilder;

public class SamplerTests extends BaseAggregationTestCase<SamplerAggregationBuilder> {

    @Override
    protected final SamplerAggregationBuilder createTestAggregatorBuilder() {
        SamplerAggregationBuilder factory = new SamplerAggregationBuilder(randomAlphaOfLengthBetween(3, 10));
        if (randomBoolean()) {
            factory.shardSize(randomIntBetween(1, 1000));
        }
        return factory;
    }

}
