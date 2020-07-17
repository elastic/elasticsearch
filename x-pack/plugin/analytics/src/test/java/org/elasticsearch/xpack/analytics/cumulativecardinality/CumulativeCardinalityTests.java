/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.analytics.cumulativecardinality;

import org.elasticsearch.plugins.SearchPlugin;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.BasePipelineAggregationTestCase;
import org.elasticsearch.search.aggregations.bucket.histogram.AutoDateHistogramAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.HistogramAggregationBuilder;

import java.io.IOException;
import java.util.List;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class CumulativeCardinalityTests extends BasePipelineAggregationTestCase<CumulativeCardinalityPipelineAggregationBuilder> {
    @Override
    protected List<SearchPlugin> plugins() {
        return singletonList(new SearchPlugin() {
            @Override
            public List<PipelineAggregationSpec> getPipelineAggregations() {
                return singletonList(new PipelineAggregationSpec(
                        CumulativeCardinalityPipelineAggregationBuilder.NAME,
                        CumulativeCardinalityPipelineAggregationBuilder::new,
                        CumulativeCardinalityPipelineAggregationBuilder.PARSER));
            }
        });
    }

    @Override
    protected CumulativeCardinalityPipelineAggregationBuilder createTestAggregatorFactory() {
        String name = randomAlphaOfLengthBetween(3, 20);
        String bucketsPath = randomAlphaOfLengthBetween(3, 20);
        CumulativeCardinalityPipelineAggregationBuilder builder =
                new CumulativeCardinalityPipelineAggregationBuilder(name, bucketsPath);
        if (randomBoolean()) {
            builder.format(randomAlphaOfLengthBetween(1, 10));
        }
        return builder;
    }


    public void testParentValidations() throws IOException {
        CumulativeCardinalityPipelineAggregationBuilder builder =
                new CumulativeCardinalityPipelineAggregationBuilder("name", randomAlphaOfLength(5));

        assertThat(validate(new HistogramAggregationBuilder("name"), builder), nullValue());
        assertThat(validate(new DateHistogramAggregationBuilder("name"), builder), nullValue());
        assertThat(validate(new AutoDateHistogramAggregationBuilder("name"), builder), nullValue());

        // Mocked "test" agg, should fail validation
        AggregationBuilder stubParent = mock(AggregationBuilder.class);
        when(stubParent.getName()).thenReturn("name");
        assertThat(validate(stubParent, builder), equalTo(
                "Validation Failed: 1: cumulative_cardinality aggregation [name] must have a histogram, "
                + "date_histogram or auto_date_histogram as parent;"));

        assertThat(validate(emptyList(), builder), equalTo(
                "Validation Failed: 1: cumulative_cardinality aggregation [name] must have a histogram, "
                + "date_histogram or auto_date_histogram as parent but doesn't have a parent;"));
    }
}
