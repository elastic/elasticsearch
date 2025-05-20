/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.aggregations.metrics;

import org.elasticsearch.search.aggregations.BaseAggregationTestCase;

public class GeoCentroidTests extends BaseAggregationTestCase<GeoCentroidAggregationBuilder> {

    @Override
    protected GeoCentroidAggregationBuilder createTestAggregatorBuilder() {
        GeoCentroidAggregationBuilder factory = new GeoCentroidAggregationBuilder(randomAlphaOfLengthBetween(1, 20));
        String field = randomNumericField();
        randomFieldOrScript(factory, field);
        if (randomBoolean()) {
            factory.missing("0,0");
        }
        return factory;
    }

}
