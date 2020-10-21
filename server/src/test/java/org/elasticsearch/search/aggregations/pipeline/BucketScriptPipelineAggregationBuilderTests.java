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
import org.elasticsearch.search.aggregations.BasePipelineAggregationTestCase;

import java.util.HashMap;
import java.util.Map;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static org.hamcrest.CoreMatchers.equalTo;

public class BucketScriptPipelineAggregationBuilderTests extends BasePipelineAggregationTestCase<BucketScriptPipelineAggregationBuilder> {
    @Override
    protected BucketScriptPipelineAggregationBuilder createTestAggregatorFactory() {
        Map<String, String> bucketsPathsMap = new HashMap<>();
        int targetBucketsPathsMapSize = randomInt(5);
        while (bucketsPathsMap.size() < targetBucketsPathsMapSize) {
            bucketsPathsMap.put(randomAlphaOfLength(5), randomAlphaOfLength(5));
        }
        Script script = new Script(randomAlphaOfLength(4));
        return new BucketScriptPipelineAggregationBuilder(randomAlphaOfLength(3), bucketsPathsMap, script);
    }

    public void testNoParent() {
        assertThat(validate(emptyList(), new BucketScriptPipelineAggregationBuilder("foo", emptyMap(), new Script("foo"))), 
            equalTo("Validation Failed: 1: bucket_script aggregation [foo] must be declared inside of another aggregation;"));
    }
}
