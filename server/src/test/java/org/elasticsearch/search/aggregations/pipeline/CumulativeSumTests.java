/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.pipeline;

import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.BasePipelineAggregationTestCase;

import java.io.IOException;

import static java.util.Collections.emptyList;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class CumulativeSumTests extends BasePipelineAggregationTestCase<CumulativeSumPipelineAggregationBuilder> {
    @Override
    protected CumulativeSumPipelineAggregationBuilder createTestAggregatorFactory() {
        String name = randomAlphaOfLengthBetween(3, 20);
        String bucketsPath = randomAlphaOfLengthBetween(3, 20);
        CumulativeSumPipelineAggregationBuilder factory = new CumulativeSumPipelineAggregationBuilder(name, bucketsPath);
        if (randomBoolean()) {
            factory.format(randomAlphaOfLengthBetween(1, 10));
        }
        return factory;
    }

    public void testValidate() throws IOException {
        assertThat(
            validate(
                PipelineAggregationHelperTests.getRandomSequentiallyOrderedParentAgg(),
                new CumulativeSumPipelineAggregationBuilder("name", "valid")
            ),
            nullValue()
        );
    }

    public void testInvalidParent() throws IOException {
        AggregationBuilder parent = mock(AggregationBuilder.class);
        when(parent.getName()).thenReturn("name");

        assertThat(
            validate(parent, new CumulativeSumPipelineAggregationBuilder("name", "invalid_agg>metric")),
            equalTo(
                "Validation Failed: 1: cumulative_sum aggregation [name] must have a histogram, date_histogram "
                    + "or auto_date_histogram as parent;"
            )
        );
    }

    public void testNoParent() throws IOException {
        assertThat(
            validate(emptyList(), new CumulativeSumPipelineAggregationBuilder("name", "invalid_agg>metric")),
            equalTo(
                "Validation Failed: 1: cumulative_sum aggregation [name] must have a histogram, date_histogram "
                    + "or auto_date_histogram as parent but doesn't have a parent;"
            )
        );
    }
}
