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

package org.elasticsearch.search.aggregations.pipeline.moving.function;

import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.aggregations.BasePipelineAggregationTestCase;
import org.elasticsearch.search.aggregations.pipeline.BucketHelpers;
import org.elasticsearch.search.aggregations.pipeline.moving.MovFunctionPipelineAggregationBuilder;
import org.elasticsearch.search.aggregations.pipeline.moving.models.MaxModel;
import org.elasticsearch.search.aggregations.pipeline.moving.models.MedianModel;
import org.elasticsearch.search.aggregations.pipeline.moving.models.MinModel;
import org.elasticsearch.search.aggregations.pipeline.moving.models.MovModel;

import java.util.HashMap;
import java.util.Map;

public class MovFnTests extends BasePipelineAggregationTestCase<MovFunctionPipelineAggregationBuilder> {

    @Override
    protected MovFunctionPipelineAggregationBuilder createTestAggregatorFactory() {
        String name = randomAlphaOfLengthBetween(3, 20);
        String bucketsPath = randomAlphaOfLengthBetween(3, 20);
        Script script;
        MovModel model;

        if (randomBoolean()) {
            model = null;
            if (randomBoolean()) {
                script = mockScript("script");
            } else {
                Map<String, Object> params = new HashMap<>();
                if (randomBoolean()) {
                    params.put("foo", "bar");
                }
                ScriptType type = randomFrom(ScriptType.values());
                script = new Script(type, type == ScriptType.STORED
                    ? null
                    : randomFrom("my_lang", Script.DEFAULT_SCRIPT_LANG),
                    "script", params);
            }
        } else {
            script = null;
            switch (randomInt(3)) {
                case 0:
                    model = new MinModel();
                    break;
                case 1:
                    model = new MaxModel();
                    break;
                case 2:
                    model = new MedianModel();
                    break;
                default:
                    model = new MinModel();
                    break;
            }
        }
        MovFunctionPipelineAggregationBuilder factory
            = new MovFunctionPipelineAggregationBuilder(name, bucketsPath, script);
        if (model != null) {
            factory.function(model);
        }
        if (randomBoolean()) {
            factory.window(randomIntBetween(1, 100));
        }
        if (randomBoolean()) {
            factory.format(randomAlphaOfLengthBetween(1, 10));
        }
        if (randomBoolean()) {
            factory.gapPolicy(randomFrom(BucketHelpers.GapPolicy.values()));
        }
        return factory;
    }

}
