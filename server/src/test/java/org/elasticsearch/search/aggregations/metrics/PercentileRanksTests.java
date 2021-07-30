/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.metrics;

import org.elasticsearch.search.aggregations.BaseAggregationTestCase;

public class PercentileRanksTests extends BaseAggregationTestCase<PercentileRanksAggregationBuilder> {

    @Override
    protected PercentileRanksAggregationBuilder createTestAggregatorBuilder() {
        int valuesSize = randomIntBetween(1, 20);
        double[] values = new double[valuesSize];
        for (int i = 0; i < valuesSize; i++) {
            values[i] = randomDouble() * 100;
        }
        PercentileRanksAggregationBuilder factory = new PercentileRanksAggregationBuilder(randomAlphaOfLengthBetween(1, 20), values);
        if (randomBoolean()) {
            factory.keyed(randomBoolean());
        }

        if (randomBoolean()) {
            factory.numberOfSignificantValueDigits(randomIntBetween(0, 5));
        } else if (randomBoolean()) {
            factory.compression(randomIntBetween(1, 50000));
        }
        String field = randomNumericField();
        randomFieldOrScript(factory, field);
        if (randomBoolean()) {
            factory.missing("MISSING");
        }
        return factory;
    }

}
