/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.aggs.changepoint;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.SearchPlugin;
import org.elasticsearch.search.aggregations.BasePipelineAggregationTestCase;
import org.elasticsearch.xpack.ml.MachineLearning;

import java.util.Collections;
import java.util.List;

public class ChangePointAggregationBuilderTests extends BasePipelineAggregationTestCase<ChangePointAggregationBuilder> {
    @Override
    protected List<SearchPlugin> plugins() {
        return Collections.singletonList(new MachineLearning(Settings.EMPTY));
    }

    @Override
    protected ChangePointAggregationBuilder createTestAggregatorFactory() {
        return new ChangePointAggregationBuilder(randomAlphaOfLength(10), randomAlphaOfLength(10));
    }
}
