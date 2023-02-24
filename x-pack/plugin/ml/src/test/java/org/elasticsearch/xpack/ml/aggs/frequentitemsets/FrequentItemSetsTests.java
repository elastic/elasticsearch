/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.aggs.frequentitemsets;

import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.aggregations.BaseAggregationTestCase;
import org.elasticsearch.xpack.ml.MachineLearningTests;

import java.util.Collection;
import java.util.List;

import static org.elasticsearch.xpack.ml.aggs.frequentitemsets.FrequentItemSetsAggregationBuilderTests.randomFrequentItemsSetsAggregationBuilder;

public class FrequentItemSetsTests extends BaseAggregationTestCase<FrequentItemSetsAggregationBuilder> {

    @Override
    protected Collection<Class<? extends Plugin>> getExtraPlugins() {
        return List.of(MachineLearningTests.TrialLicensedMachineLearning.class);
    }

    @Override
    protected FrequentItemSetsAggregationBuilder createTestAggregatorBuilder() {
        return randomFrequentItemsSetsAggregationBuilder();
    }

}
