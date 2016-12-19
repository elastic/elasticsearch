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
package org.elasticsearch.search.aggregations;

import org.elasticsearch.search.aggregations.pipeline.PipelineAggregatorBuilders;
import org.elasticsearch.test.ESTestCase;

import java.util.List;

import static org.hamcrest.Matchers.equalTo;

public class AggregatorFactoriesTests extends ESTestCase {

    public void testGetAggregatorFactories_returnsUnmodifiableList() {
        AggregatorFactories.Builder builder = new AggregatorFactories.Builder().addAggregator(AggregationBuilders.avg("foo"));
        List<AggregationBuilder> aggregatorFactories = builder.getAggregatorFactories();
        assertThat(aggregatorFactories.size(), equalTo(1));
        expectThrows(UnsupportedOperationException.class, () -> aggregatorFactories.add(AggregationBuilders.avg("bar")));
    }

    public void testGetPipelineAggregatorFactories_returnsUnmodifiableList() {
        AggregatorFactories.Builder builder = new AggregatorFactories.Builder().addPipelineAggregator(
            PipelineAggregatorBuilders.avgBucket("foo", "path1"));
        List<PipelineAggregationBuilder> pipelineAggregatorFactories = builder.getPipelineAggregatorFactories();
        assertThat(pipelineAggregatorFactories.size(), equalTo(1));
        expectThrows(UnsupportedOperationException.class,
            () -> pipelineAggregatorFactories.add(PipelineAggregatorBuilders.avgBucket("bar", "path2")));
    }
}
