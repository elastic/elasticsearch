/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.analytics.movingPercentiles;

import org.apache.lucene.util.TestUtil;
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

public class MovingPercentilesTests extends BasePipelineAggregationTestCase<MovingPercentilesPipelineAggregationBuilder> {
    @Override
    protected List<SearchPlugin> plugins() {
        return singletonList(new SearchPlugin() {
            @Override
            public List<PipelineAggregationSpec> getPipelineAggregations() {
                return singletonList(new PipelineAggregationSpec(
                        MovingPercentilesPipelineAggregationBuilder.NAME,
                        MovingPercentilesPipelineAggregationBuilder::new,
                        MovingPercentilesPipelineAggregationBuilder.PARSER));
            }
        });
    }

    @Override
    protected MovingPercentilesPipelineAggregationBuilder createTestAggregatorFactory() {
        String name = randomAlphaOfLengthBetween(3, 20);
        String bucketsPath = randomAlphaOfLengthBetween(3, 20);
        MovingPercentilesPipelineAggregationBuilder builder =
                new MovingPercentilesPipelineAggregationBuilder(name, bucketsPath, TestUtil.nextInt(random(), 1, 10));
        if (randomBoolean()) {
            builder.setShift(randomIntBetween(0, 10));
        }
        return builder;
    }


    public void testParentValidations() throws IOException {
        MovingPercentilesPipelineAggregationBuilder builder =
                new MovingPercentilesPipelineAggregationBuilder("name", randomAlphaOfLength(5), TestUtil.nextInt(random(), 1, 10));

        assertThat(validate(new HistogramAggregationBuilder("name"), builder), nullValue());
        assertThat(validate(new DateHistogramAggregationBuilder("name"), builder), nullValue());
        assertThat(validate(new AutoDateHistogramAggregationBuilder("name"), builder), nullValue());

        // Mocked "test" agg, should fail validation
        AggregationBuilder stubParent = mock(AggregationBuilder.class);
        when(stubParent.getName()).thenReturn("name");
        assertThat(validate(stubParent, builder), equalTo(
                "Validation Failed: 1: moving_percentiles aggregation [name] must have a histogram, "
                + "date_histogram or auto_date_histogram as parent;"));

        assertThat(validate(emptyList(), builder), equalTo(
                "Validation Failed: 1: moving_percentiles aggregation [name] must have a histogram, "
                + "date_histogram or auto_date_histogram as parent but doesn't have a parent;"));
    }
}
