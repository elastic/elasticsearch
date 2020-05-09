/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.analytics.normalize;

import org.elasticsearch.plugins.SearchPlugin;
import org.elasticsearch.search.aggregations.BasePipelineAggregationTestCase;

import java.util.Collections;
import java.util.List;

import static java.util.Collections.singletonList;
import static org.hamcrest.Matchers.equalTo;

public class NormalizeTests extends BasePipelineAggregationTestCase<NormalizePipelineAggregationBuilder> {

    @Override
    protected List<SearchPlugin> plugins() {
        return singletonList(new SearchPlugin() {
            @Override
            public List<PipelineAggregationSpec> getPipelineAggregations() {
                return singletonList(new PipelineAggregationSpec(
                    NormalizePipelineAggregationBuilder.NAME,
                    NormalizePipelineAggregationBuilder::new,
                    NormalizePipelineAggregationBuilder.PARSER));
            }
        });
    }

    public void testInvalidNormalizer() {
        NormalizePipelineAggregationBuilder builder = createTestAggregatorFactory();
        String invalidNormalizer = randomFrom(NormalizePipelineAggregationBuilder.NAME_MAP.keySet()) + randomAlphaOfLength(10);
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class,
            () -> new NormalizePipelineAggregationBuilder(builder.getName(), builder.format(), invalidNormalizer,
                List.of(builder.getBucketsPaths())));
        assertThat(exception.getMessage(), equalTo("invalid normalizer [" + invalidNormalizer + "]"));
    }

    @Override
    protected NormalizePipelineAggregationBuilder createTestAggregatorFactory() {
        String name = randomAlphaOfLengthBetween(3, 20);
        String bucketsPath = randomAlphaOfLengthBetween(3, 20);
        String format = null;
        if (randomBoolean()) {
            format = randomAlphaOfLengthBetween(1, 10);
        }
        String normalizer = randomFrom(NormalizePipelineAggregationBuilder.NAME_MAP.keySet());
        return new NormalizePipelineAggregationBuilder(name, format, normalizer, Collections.singletonList(bucketsPath));
    }
}
