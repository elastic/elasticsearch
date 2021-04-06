/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.metrics;

import org.elasticsearch.search.aggregations.BaseAggregationTestCase;

public class GeoBoundsTests extends BaseAggregationTestCase<GeoBoundsAggregationBuilder> {

    @Override
    protected GeoBoundsAggregationBuilder createTestAggregatorBuilder() {
        GeoBoundsAggregationBuilder factory = new GeoBoundsAggregationBuilder(randomAlphaOfLengthBetween(1, 20));
        String field = randomAlphaOfLengthBetween(3, 20);
        factory.field(field);
        if (randomBoolean()) {
            factory.wrapLongitude(randomBoolean());
    }
        if (randomBoolean()) {
            factory.missing("0,0");
    }
        return factory;
    }

}
