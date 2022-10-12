/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.aggregations.bucket.histogram;

import org.elasticsearch.aggregations.AggregationsPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.aggregations.BaseAggregationTestCase;

import java.util.Collection;
import java.util.List;

public class AutoDateHistogramTests extends BaseAggregationTestCase<AutoDateHistogramAggregationBuilder> {

    // TODO: maybe add base class that overwrites getPlugins(...) for all tests that will be added to this module.
    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return List.of(AggregationsPlugin.class);
    }

    @Override
    protected AutoDateHistogramAggregationBuilder createTestAggregatorBuilder() {
        AutoDateHistogramAggregationBuilder builder = new AutoDateHistogramAggregationBuilder(randomAlphaOfLengthBetween(1, 10));
        builder.field(INT_FIELD_NAME);
        builder.setNumBuckets(randomIntBetween(1, 100000));
        // TODO[PCS]: add builder pattern here
        if (randomBoolean()) {
            builder.format("###.##");
        }
        if (randomBoolean()) {
            builder.missing(randomIntBetween(0, 10));
        }
        if (randomBoolean()) {
            builder.timeZone(randomZone());
        }
        return builder;
    }

}
