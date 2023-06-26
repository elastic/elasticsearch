/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.aggregations.pipeline;

import org.elasticsearch.aggregations.AggregationsPlugin;
import org.elasticsearch.plugins.SearchPlugin;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.BasePipelineAggregationTestCase;
import org.elasticsearch.search.aggregations.PipelineAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.pipeline.BucketHelpers.GapPolicy;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class DerivativePipelinesAggregationBuilderTests extends BasePipelineAggregationTestCase<DerivativePipelineAggregationBuilder> {
    @Override
    protected List<SearchPlugin> plugins() {
        return List.of(new AggregationsPlugin());
    }

    @Override
    protected DerivativePipelineAggregationBuilder createTestAggregatorFactory() {
        String name = randomAlphaOfLengthBetween(3, 20);
        String bucketsPath = randomAlphaOfLengthBetween(3, 20);
        DerivativePipelineAggregationBuilder factory = new DerivativePipelineAggregationBuilder(name, bucketsPath);
        if (randomBoolean()) {
            factory.format(randomAlphaOfLengthBetween(1, 10));
        }
        if (randomBoolean()) {
            factory.gapPolicy(randomFrom(GapPolicy.values()));
        }
        if (randomBoolean()) {
            if (randomBoolean()) {
                factory.unit(String.valueOf(randomInt()));
            } else {
                factory.unit(String.valueOf(randomIntBetween(1, 10) + randomFrom("s", "m", "h", "d", "w", "M", "y")));
            }
        }
        return factory;
    }

    /**
     * The validation should verify the parent aggregation is allowed.
     */
    public void testValidate() throws IOException {
        assertThat(
            validate(
                PipelineAggregationHelperTests.getRandomSequentiallyOrderedParentAgg(),
                new DerivativePipelineAggregationBuilder("name", "valid")
            ),
            nullValue()
        );
    }

    /**
     * The validation should throw an IllegalArgumentException, since parent
     * aggregation is not a type of HistogramAggregatorFactory,
     * DateHistogramAggregatorFactory or AutoDateHistogramAggregatorFactory.
     */
    public void testValidateException() throws IOException {
        final Set<PipelineAggregationBuilder> aggBuilders = new HashSet<>();
        aggBuilders.add(new DerivativePipelineAggregationBuilder("deriv", "der"));
        AggregationBuilder parent = new TermsAggregationBuilder("name");
        assertThat(
            validate(parent, new DerivativePipelineAggregationBuilder("name", "invalid_agg>metric")),
            equalTo(
                "Validation Failed: 1: derivative aggregation [name] must have a histogram, "
                    + "date_histogram or auto_date_histogram as parent;"
            )
        );
    }

}
