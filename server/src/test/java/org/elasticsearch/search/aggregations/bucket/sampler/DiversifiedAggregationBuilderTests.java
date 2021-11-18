/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.bucket.sampler;

import org.elasticsearch.search.aggregations.BaseAggregationTestCase;
import org.elasticsearch.search.aggregations.bucket.sampler.SamplerAggregator.ExecutionMode;

public class DiversifiedAggregationBuilderTests extends BaseAggregationTestCase<DiversifiedAggregationBuilder> {

    @Override
    protected final DiversifiedAggregationBuilder createTestAggregatorBuilder() {
        DiversifiedAggregationBuilder factory = new DiversifiedAggregationBuilder(randomAlphaOfLengthBetween(3, 10));
        String field = randomNumericField();
        randomFieldOrScript(factory, field);
        if (randomBoolean()) {
            factory.missing("MISSING");
        }
        if (randomBoolean()) {
            factory.maxDocsPerValue(randomIntBetween(1, 1000));
        }
        if (randomBoolean()) {
            factory.shardSize(randomIntBetween(1, 1000));
        }
        if (randomBoolean()) {
            factory.executionHint(randomFrom(ExecutionMode.values()).toString());
        }
        return factory;
    }

}
