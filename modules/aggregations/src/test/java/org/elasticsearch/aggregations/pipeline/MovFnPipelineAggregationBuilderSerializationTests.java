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
import org.elasticsearch.script.Script;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.BasePipelineAggregationTestCase;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class MovFnPipelineAggregationBuilderSerializationTests extends BasePipelineAggregationTestCase<MovFnPipelineAggregationBuilder> {
    @Override
    protected List<SearchPlugin> plugins() {
        return List.of(new AggregationsPlugin());
    }

    @Override
    protected MovFnPipelineAggregationBuilder createTestAggregatorFactory() {
        MovFnPipelineAggregationBuilder builder = new MovFnPipelineAggregationBuilder(
            randomAlphaOfLength(10),
            "foo",
            new Script("foo"),
            randomIntBetween(1, 10)
        );
        builder.setShift(randomIntBetween(1, 10));
        return builder;
    }

    public void testValidParent() throws IOException {
        Script script = new Script(Script.DEFAULT_SCRIPT_TYPE, "painless", "test", emptyMap());
        assertThat(
            validate(
                PipelineAggregationHelperTests.getRandomSequentiallyOrderedParentAgg(),
                new MovFnPipelineAggregationBuilder("mov_fn", "avg", script, 3)
            ),
            nullValue()
        );
    }

    public void testInvalidParent() throws IOException {
        Script script = new Script(Script.DEFAULT_SCRIPT_TYPE, "painless", "test", Collections.emptyMap());
        AggregationBuilder parent = new TermsAggregationBuilder("name");
        assertThat(
            validate(parent, new MovFnPipelineAggregationBuilder("name", "invalid_agg>metric", script, 1)),
            equalTo(
                "Validation Failed: 1: moving_fn aggregation [name] must have a histogram, date_histogram"
                    + " or auto_date_histogram as parent;"
            )
        );
    }

    public void testNoParent() throws IOException {
        Script script = new Script(Script.DEFAULT_SCRIPT_TYPE, "painless", "test", Collections.emptyMap());
        assertThat(
            validate(emptyList(), new MovFnPipelineAggregationBuilder("name", "invalid_agg>metric", script, 1)),
            equalTo(
                "Validation Failed: 1: moving_fn aggregation [name] must have a histogram, date_histogram"
                    + " or auto_date_histogram as parent but doesn't have a parent;"
            )
        );
    }
}
