/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.aggs.categorization;

import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.aggregations.BaseAggregationTestCase;
import org.elasticsearch.xpack.ml.MachineLearningTests;
import org.elasticsearch.xpack.ml.job.config.CategorizationAnalyzerConfigTests;

import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class CategorizeTextAggregationBuilderTests extends BaseAggregationTestCase<CategorizeTextAggregationBuilder> {

    @Override
    protected Collection<Class<? extends Plugin>> getExtraPlugins() {
        return List.of(MachineLearningTests.TrialLicensedMachineLearning.class);
    }

    @Override
    protected CategorizeTextAggregationBuilder createTestAggregatorBuilder() {
        CategorizeTextAggregationBuilder builder = new CategorizeTextAggregationBuilder(randomAlphaOfLength(10), randomAlphaOfLength(10));
        final boolean setFilters = randomBoolean();
        if (setFilters) {
            builder.setCategorizationFilters(Stream.generate(() -> randomAlphaOfLength(10)).limit(5).collect(Collectors.toList()));
        }
        if (setFilters == false) {
            builder.setCategorizationAnalyzerConfig(CategorizationAnalyzerConfigTests.createRandomized().build());
        }
        if (randomBoolean()) {
            builder.setSimilarityThreshold(randomIntBetween(1, 100));
        }
        if (randomBoolean()) {
            builder.minDocCount(randomLongBetween(1, 100));
        }
        if (randomBoolean()) {
            builder.shardMinDocCount(randomLongBetween(1, 100));
        }
        if (randomBoolean()) {
            builder.size(randomIntBetween(1, 100));
        }
        if (randomBoolean()) {
            builder.shardSize(randomIntBetween(1, 100));
        }
        return builder;
    }
}
