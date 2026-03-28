/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.aggs.outlierdetection;

import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.aggregations.BaseAggregationTestCase;
import org.elasticsearch.xpack.ml.MachineLearningTests;

import java.util.Collection;
import java.util.List;

public class OutlierDetectionAggregationBuilderTests extends BaseAggregationTestCase<OutlierDetectionAggregationBuilder> {

    @Override
    protected Collection<Class<? extends Plugin>> getExtraPlugins() {
        return List.of(MachineLearningTests.TrialLicensedMachineLearning.class);
    }

    @Override
    protected OutlierDetectionAggregationBuilder createTestAggregatorBuilder() {
        OutlierDetectionAggregationBuilder builder = new OutlierDetectionAggregationBuilder(randomAlphaOfLength(10));
        builder.setField(randomAlphaOfLength(10));
        if (randomBoolean()) {
            builder.setTopN(randomIntBetween(1, 100));
        }
        if (randomBoolean()) {
            builder.setNNeighbors(randomIntBetween(1, 50));
        }
        if (randomBoolean()) {
            builder.setSampleSize(randomIntBetween(10, 10000));
        }
        if (randomBoolean()) {
            builder.setProjectionDim(randomIntBetween(1, 100));
        }
        if (randomBoolean()) {
            builder.setSeed(randomLong());
        }
        if (randomBoolean()) {
            builder.setOverfetchFactor(randomIntBetween(1, 10));
        }
        if (randomBoolean()) {
            builder.setMaxCoordSample(randomIntBetween(50, 2000));
        }
        if (randomBoolean()) {
            builder.setNormalize(randomFrom(ScoreNormalization.values()));
        }
        return builder;
    }
}
