/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.search.aggregations.pipeline;

import org.elasticsearch.script.Script;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.BasePipelineAggregationTestCase;

import java.io.IOException;
import java.util.Collections;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class MovFnPipelineAggregationBuilderSerializationTests extends BasePipelineAggregationTestCase<MovFnPipelineAggregationBuilder> {
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
        assertThat(validate(PipelineAggregationHelperTests.getRandomSequentiallyOrderedParentAgg(),
                new MovFnPipelineAggregationBuilder("mov_fn", "avg", script, 3)), nullValue());
    }

    public void testInvalidParent() throws IOException {
        Script script = new Script(Script.DEFAULT_SCRIPT_TYPE, "painless", "test", Collections.emptyMap());
        AggregationBuilder parent = mock(AggregationBuilder.class);
        when(parent.getName()).thenReturn("name");

        assertThat(validate(parent, new MovFnPipelineAggregationBuilder("name", "invalid_agg>metric", script, 1)), equalTo(
                "Validation Failed: 1: moving_fn aggregation [name] must have a histogram, date_histogram"
                + " or auto_date_histogram as parent;"));
    }

    public void testNoParent() throws IOException {
        Script script = new Script(Script.DEFAULT_SCRIPT_TYPE, "painless", "test", Collections.emptyMap());
                assertThat(validate(emptyList(), new MovFnPipelineAggregationBuilder("name", "invalid_agg>metric", script, 1)), equalTo(
                "Validation Failed: 1: moving_fn aggregation [name] must have a histogram, date_histogram"
                + " or auto_date_histogram as parent but doesn't have a parent;"));
    }
}

