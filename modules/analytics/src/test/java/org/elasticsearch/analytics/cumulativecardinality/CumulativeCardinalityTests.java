/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.analytics.cumulativecardinality;

import org.elasticsearch.plugins.SearchPlugin;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.BasePipelineAggregationTestCase;
import org.elasticsearch.search.aggregations.bucket.histogram.AutoDateHistogramAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.HistogramAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;

import java.io.IOException;
import java.util.List;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class CumulativeCardinalityTests extends BasePipelineAggregationTestCase<CumulativeCardinalityPipelineAggregationBuilder> {
    @Override
    protected List<SearchPlugin> plugins() {
        return singletonList(new SearchPlugin() {
            @Override
            public List<PipelineAggregationSpec> getPipelineAggregations() {
                return singletonList(
                    new PipelineAggregationSpec(
                        CumulativeCardinalityPipelineAggregationBuilder.NAME,
                        CumulativeCardinalityPipelineAggregationBuilder::new,
                        CumulativeCardinalityPipelineAggregationBuilder.PARSER
                    )
                );
            }
        });
    }

    @Override
    protected CumulativeCardinalityPipelineAggregationBuilder createTestAggregatorFactory() {
        String name = randomAlphaOfLengthBetween(3, 20);
        String bucketsPath = randomAlphaOfLengthBetween(3, 20);
        CumulativeCardinalityPipelineAggregationBuilder builder = new CumulativeCardinalityPipelineAggregationBuilder(name, bucketsPath);
        if (randomBoolean()) {
            builder.format(randomAlphaOfLengthBetween(1, 10));
        }
        return builder;
    }

    public void testParentValidations() throws IOException {
        CumulativeCardinalityPipelineAggregationBuilder builder = new CumulativeCardinalityPipelineAggregationBuilder(
            "name",
            randomAlphaOfLength(5)
        );

        assertThat(validate(new HistogramAggregationBuilder("name"), builder), nullValue());
        assertThat(validate(new DateHistogramAggregationBuilder("name"), builder), nullValue());
        assertThat(validate(new AutoDateHistogramAggregationBuilder("name"), builder), nullValue());

        // Mocked "test" agg, should fail validation
        AggregationBuilder stubParent = new TermsAggregationBuilder("name");
        assertThat(
            validate(stubParent, builder),
            equalTo(
                "Validation Failed: 1: cumulative_cardinality aggregation [name] must have a histogram, "
                    + "date_histogram or auto_date_histogram as parent;"
            )
        );

        assertThat(
            validate(emptyList(), builder),
            equalTo(
                "Validation Failed: 1: cumulative_cardinality aggregation [name] must have a histogram, "
                    + "date_histogram or auto_date_histogram as parent but doesn't have a parent;"
            )
        );
    }
}
