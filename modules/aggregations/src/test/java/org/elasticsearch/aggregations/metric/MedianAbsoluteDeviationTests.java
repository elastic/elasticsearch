/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.aggregations.metric;

import org.elasticsearch.aggregations.AggregationsPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.aggregations.metrics.AbstractNumericMetricTestCase;
import org.elasticsearch.test.ESTestCase;

import java.util.Collection;
import java.util.List;

public class MedianAbsoluteDeviationTests extends AbstractNumericMetricTestCase<MedianAbsoluteDeviationAggregationBuilder> {

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return List.of(AggregationsPlugin.class);
    }

    @Override
    protected MedianAbsoluteDeviationAggregationBuilder doCreateTestAggregatorFactory() {
        MedianAbsoluteDeviationAggregationBuilder builder = new MedianAbsoluteDeviationAggregationBuilder(
            ESTestCase.randomAlphaOfLengthBetween(1, 20)
        );

        if (ESTestCase.randomBoolean()) {
            builder.compression(ESTestCase.randomDoubleBetween(0, 1000.0, false));
        }

        if (ESTestCase.randomBoolean()) {
            builder.missing("MISSING");
        }

        if (ESTestCase.randomBoolean()) {
            builder.format("###.00");
        }

        return builder;
    }
}
