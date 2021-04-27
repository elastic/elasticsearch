/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.metrics;

import org.elasticsearch.search.aggregations.BaseAggregationTestCase;

public class ValueCountTests extends BaseAggregationTestCase<ValueCountAggregationBuilder> {

    @Override
    protected final ValueCountAggregationBuilder createTestAggregatorBuilder() {
        ValueCountAggregationBuilder factory = new ValueCountAggregationBuilder(randomAlphaOfLengthBetween(3, 10));
        String field = randomNumericField();
        randomFieldOrScript(factory, field);
        if (randomBoolean()) {
            factory.missing("MISSING");
        }
        return factory;
    }

}
