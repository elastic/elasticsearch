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
import org.elasticsearch.script.ScriptService.ScriptType;
import org.elasticsearch.search.aggregations.BasePipelineAggregationTestCase;
import org.elasticsearch.search.aggregations.pipeline.BucketHelpers.GapPolicy;
import org.elasticsearch.search.aggregations.pipeline.bucketscript.BucketScriptPipelineAggregationBuilder;

import java.util.HashMap;
import java.util.Map;

public class BucketScriptTests extends BasePipelineAggregationTestCase<BucketScriptPipelineAggregationBuilder> {

    @Override
    protected BucketScriptPipelineAggregationBuilder createTestAggregatorFactory() {
        String name = randomAsciiOfLengthBetween(3, 20);
        Map<String, String> bucketsPaths = new HashMap<>();
        int numBucketPaths = randomIntBetween(1, 10);
        for (int i = 0; i < numBucketPaths; i++) {
            bucketsPaths.put(randomAsciiOfLengthBetween(1, 20), randomAsciiOfLengthBetween(1, 40));
        }
        Script script;
        if (randomBoolean()) {
            script = new Script("script");
        } else {
            Map<String, Object> params = null;
            if (randomBoolean()) {
                params = new HashMap<String, Object>();
                params.put("foo", "bar");
            }
            script = new Script("script", randomFrom(ScriptType.values()), randomFrom("my_lang", null), params);
        }
        BucketScriptPipelineAggregationBuilder factory = new BucketScriptPipelineAggregationBuilder(name, bucketsPaths, script);
        if (randomBoolean()) {
            factory.format(randomAsciiOfLengthBetween(1, 10));
        }
        if (randomBoolean()) {
            factory.gapPolicy(randomFrom(GapPolicy.values()));
        }
        return factory;
    }

}
