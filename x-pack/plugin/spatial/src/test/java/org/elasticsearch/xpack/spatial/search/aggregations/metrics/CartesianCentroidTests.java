/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.spatial.search.aggregations.metrics;

import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.aggregations.BaseAggregationTestCase;
import org.elasticsearch.xpack.spatial.LocalStateSpatialPlugin;

import java.util.Collection;
import java.util.List;

public class CartesianCentroidTests extends BaseAggregationTestCase<CartesianCentroidAggregationBuilder> {

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return List.of(LocalStateSpatialPlugin.class);
    }

    @Override
    protected CartesianCentroidAggregationBuilder createTestAggregatorBuilder() {
        CartesianCentroidAggregationBuilder factory = new CartesianCentroidAggregationBuilder(randomAlphaOfLengthBetween(1, 20));
        String field = randomNumericField();
        randomFieldOrScript(factory, field);
        if (randomBoolean()) {
            factory.missing("0,0");
        }
        return factory;
    }
}
