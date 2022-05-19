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
import org.elasticsearch.search.aggregations.PipelineAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.pipeline.BucketHelpers.GapPolicy;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import static java.util.Collections.emptyList;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class SerialDifferenceTests extends BasePipelineAggregationTestCase<SerialDiffPipelineAggregationBuilder> {

    @Override
    protected SerialDiffPipelineAggregationBuilder createTestAggregatorFactory() {
        String name = randomAlphaOfLengthBetween(3, 20);
        String bucketsPath = randomAlphaOfLengthBetween(3, 20);
        SerialDiffPipelineAggregationBuilder factory = new SerialDiffPipelineAggregationBuilder(name, bucketsPath);
        if (randomBoolean()) {
            factory.format(randomAlphaOfLengthBetween(1, 10));
        }
        if (randomBoolean()) {
            factory.gapPolicy(randomFrom(GapPolicy.values()));
        }
        if (randomBoolean()) {
            factory.lag(randomIntBetween(1, 1000));
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
                new SerialDiffPipelineAggregationBuilder("name", "valid")
            ),
            nullValue()
        );
    }

    public void testInvalidParent() throws IOException {
        final Set<PipelineAggregationBuilder> aggBuilders = new HashSet<>();
        aggBuilders.add(createTestAggregatorFactory());
        AggregationBuilder parent = new TermsAggregationBuilder("t");
        assertThat(
            validate(parent, new SerialDiffPipelineAggregationBuilder("name", "invalid_agg>metric")),
            equalTo(
                "Validation Failed: 1: serial_diff aggregation [name] must have a histogram, "
                    + "date_histogram or auto_date_histogram as parent;"
            )
        );
    }

    public void testNoParent() {
        assertThat(
            validate(emptyList(), new SerialDiffPipelineAggregationBuilder("name", "invalid_agg>metric")),
            equalTo(
                "Validation Failed: 1: serial_diff aggregation [name] must have a histogram, "
                    + "date_histogram or auto_date_histogram as parent but doesn't have a parent;"
            )
        );
    }
}
