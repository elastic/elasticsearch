/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.analytics.normalize;

import org.elasticsearch.plugins.SearchPlugin;
import org.elasticsearch.search.aggregations.BasePipelineAggregationTestCase;
import org.hamcrest.CoreMatchers;

import java.util.Collections;
import java.util.List;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.hamcrest.Matchers.equalTo;

public class NormalizeTests extends BasePipelineAggregationTestCase<NormalizePipelineAggregationBuilder> {

    @Override
    protected List<SearchPlugin> plugins() {
        return singletonList(new SearchPlugin() {
            @Override
            public List<PipelineAggregationSpec> getPipelineAggregations() {
                return singletonList(
                    new PipelineAggregationSpec(
                        NormalizePipelineAggregationBuilder.NAME,
                        NormalizePipelineAggregationBuilder::new,
                        NormalizePipelineAggregationBuilder.PARSER
                    )
                );
            }
        });
    }

    public void testInvalidNormalizer() {
        NormalizePipelineAggregationBuilder builder = createTestAggregatorFactory();
        String invalidNormalizer = randomFrom(NormalizePipelineAggregationBuilder.NAME_MAP.keySet()) + randomAlphaOfLength(10);
        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> new NormalizePipelineAggregationBuilder(
                builder.getName(),
                builder.format(),
                invalidNormalizer,
                org.elasticsearch.core.List.of(builder.getBucketsPaths())
            )
        );
        assertThat(exception.getMessage(), equalTo("invalid method [" + invalidNormalizer + "]"));
    }

    public void testHasParentValidation() {
        NormalizePipelineAggregationBuilder builder = createTestAggregatorFactory();
        assertThat(
            validate(emptyList(), builder),
            CoreMatchers.equalTo(
                "Validation Failed: 1: normalize aggregation ["
                    + builder.getName()
                    + "] must be declared inside"
                    + " of another aggregation;"
            )
        );
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
