/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.aggregations.bucket;

import org.elasticsearch.aggregations.AggregationsPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.aggregations.AbstractAggregationBuilder;
import org.elasticsearch.search.aggregations.BaseAggregationTestCase;

import java.util.Collection;
import java.util.List;

/**
 * Base class for aggregation builders unit tests that reside in aggregation module.
 *
 * @param <AB> Aggregation builder type
 */
public abstract class AggregationBuilderTestCase<AB extends AbstractAggregationBuilder<AB>> extends BaseAggregationTestCase<AB> {

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return List.of(AggregationsPlugin.class);
    }

}
