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
        assertThat(validate(PipelineAggregationHelperTests.getRandomSequentiallyOrderedParentAgg(),
                new CumulativeSumPipelineAggregationBuilder("name", "valid")), nullValue());
    }

    public void testInvalidParent() throws IOException {
        AggregationBuilder parent = mock(AggregationBuilder.class);
        when(parent.getName()).thenReturn("name");

        assertThat(validate(parent, new CumulativeSumPipelineAggregationBuilder("name", "invalid_agg>metric")), equalTo(
                "Validation Failed: 1: cumulative_sum aggregation [name] must have a histogram, date_histogram "
                + "or auto_date_histogram as parent;"));
    }

    public void testNoParent() throws IOException {
        assertThat(validate(emptyList(), new CumulativeSumPipelineAggregationBuilder("name", "invalid_agg>metric")), equalTo(
                "Validation Failed: 1: cumulative_sum aggregation [name] must have a histogram, date_histogram "
                + "or auto_date_histogram as parent but doesn't have a parent;"));
    }
}
