/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.test.delayedshard;

import org.elasticsearch.core.TimeValue;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.aggregations.BaseAggregationTestCase;
import org.elasticsearch.test.TestGeoShapeFieldMapperPlugin;

import java.util.Arrays;
import java.util.Collection;

public class DelayedShardAggregationBuilderTests extends BaseAggregationTestCase<DelayedShardAggregationBuilder> {
    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return Arrays.asList(DelayedShardAggregationPlugin.class, TestGeoShapeFieldMapperPlugin.class);
    }

    @Override
    protected DelayedShardAggregationBuilder createTestAggregatorBuilder() {
        return new DelayedShardAggregationBuilder(randomAlphaOfLength(10), TimeValue.timeValueMillis(100));
    }
}
