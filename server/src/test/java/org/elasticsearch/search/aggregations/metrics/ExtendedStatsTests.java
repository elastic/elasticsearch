/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.metrics;

public class ExtendedStatsTests extends AbstractNumericMetricTestCase<ExtendedStatsAggregationBuilder> {

    @Override
    protected ExtendedStatsAggregationBuilder doCreateTestAggregatorFactory() {
        ExtendedStatsAggregationBuilder factory = new ExtendedStatsAggregationBuilder(randomAlphaOfLengthBetween(3, 10));
        if (randomBoolean()) {
            factory.sigma(randomDoubleBetween(0.0, 10.0, true));
        }
        return factory;
    }

}
