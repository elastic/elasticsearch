/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.aggs.categorization;

import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.aggregations.BaseAggregationTestCase;
import org.elasticsearch.xpack.ml.MachineLearning;
import org.elasticsearch.xpack.ml.job.config.CategorizationAnalyzerConfigTests;

import java.util.Collection;
import java.util.Collections;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.elasticsearch.xpack.ml.aggs.categorization.CategorizeTextAggregationBuilder.MAX_MAX_MATCHED_TOKENS;
import static org.elasticsearch.xpack.ml.aggs.categorization.CategorizeTextAggregationBuilder.MAX_MAX_UNIQUE_TOKENS;

public class CategorizeTextAggregationBuilderTests extends BaseAggregationTestCase<CategorizeTextAggregationBuilder> {

    @Override
    protected Collection<Class<? extends Plugin>> getExtraPlugins() {
        return Collections.singletonList(MachineLearning.class);
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
            builder.setMaxUniqueTokens(randomIntBetween(1, MAX_MAX_UNIQUE_TOKENS));
        }
        if (randomBoolean()) {
            builder.setMaxMatchedTokens(randomIntBetween(1, MAX_MAX_MATCHED_TOKENS));
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
