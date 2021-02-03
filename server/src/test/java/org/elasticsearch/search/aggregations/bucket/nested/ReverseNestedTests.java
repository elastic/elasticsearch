/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.bucket.nested;

import org.elasticsearch.search.aggregations.BaseAggregationTestCase;

public class ReverseNestedTests extends BaseAggregationTestCase<ReverseNestedAggregationBuilder> {

    @Override
    protected ReverseNestedAggregationBuilder createTestAggregatorBuilder() {
        ReverseNestedAggregationBuilder factory = new ReverseNestedAggregationBuilder(randomAlphaOfLengthBetween(1, 20));
        if (randomBoolean()) {
            factory.path(randomAlphaOfLengthBetween(3, 40));
        }
        return factory;
    }

}
