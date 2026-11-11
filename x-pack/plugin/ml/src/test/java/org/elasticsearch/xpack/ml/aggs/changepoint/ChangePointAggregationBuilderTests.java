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
import org.elasticsearch.search.aggregations.pipeline.BucketHelpers;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.ml.MachineLearningTests;

import java.io.IOException;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;

public class ChangePointAggregationBuilderTests extends BasePipelineAggregationTestCase<ChangePointAggregationBuilder> {
    @Override
    protected List<SearchPlugin> plugins() {
        return List.of(MachineLearningTests.createTrialLicensedMachineLearning(Settings.EMPTY));
    }

    @Override
    protected ChangePointAggregationBuilder createTestAggregatorFactory() {
        return new ChangePointAggregationBuilder(
            randomAlphaOfLength(10),
            randomAlphaOfLength(10),
            randomFrom(BucketHelpers.GapPolicy.values())
        );
    }

    /**
     * The parser treats {@code gap_policy} as an optional constructor argument, so a request that omits it
     * hands the builder a null policy. That has to settle on {@code skip} rather than staying null, because
     * the policy is written to the wire unconditionally and would otherwise fail the search.
     * {@link #testFromXContent()} cannot cover this: it round-trips a builder whose policy is always set.
     */
    public void testOmittedGapPolicyDefaultsToSkip() throws IOException {
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, """
            {"changes":{"change_point":{"buckets_path":"time>sum"}}}""")) {
            ChangePointAggregationBuilder parsed = asInstanceOf(ChangePointAggregationBuilder.class, parse(parser));
            assertThat(parsed.gapPolicy(), equalTo(BucketHelpers.GapPolicy.SKIP));
        }
    }
}
