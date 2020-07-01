/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.spatial.search.aggregations.metrics;

import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.aggregations.BaseAggregationTestCase;
import org.elasticsearch.xpack.spatial.LocalStateSpatialPlugin;

import java.util.Collection;
import java.util.Collections;

public class BoundsTests extends BaseAggregationTestCase<BoundsAggregationBuilder> {

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return Collections.singletonList(LocalStateSpatialPlugin.class);
    }

    @Override
    protected BoundsAggregationBuilder createTestAggregatorBuilder() {
        BoundsAggregationBuilder factory = new BoundsAggregationBuilder(randomAlphaOfLengthBetween(1, 20));
        String field = randomAlphaOfLengthBetween(3, 20);
        factory.field(field);
        if (randomBoolean()) {
            factory.missing("0,0");
        }
        return factory;
    }

}
