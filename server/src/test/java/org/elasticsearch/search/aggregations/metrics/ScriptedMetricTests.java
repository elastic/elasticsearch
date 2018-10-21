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

package org.elasticsearch.search.aggregations.metrics;

import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.aggregations.BaseAggregationTestCase;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class ScriptedMetricTests extends BaseAggregationTestCase<ScriptedMetricAggregationBuilder> {

    @Override
    protected ScriptedMetricAggregationBuilder createTestAggregatorBuilder() {
        ScriptedMetricAggregationBuilder factory = new ScriptedMetricAggregationBuilder(randomAlphaOfLengthBetween(1, 20));
        if (randomBoolean()) {
            factory.initScript(randomScript("initScript"));
        }
        factory.mapScript(randomScript("mapScript"));
        if (randomBoolean()) {
            factory.combineScript(randomScript("combineScript"));
        }
        if (randomBoolean()) {
            factory.reduceScript(randomScript("reduceScript"));
        }
        if (randomBoolean()) {
            Map<String, Object> params = new HashMap<>();
            params.put("foo", "bar");
            factory.params(params);
        }
        return factory;
    }

    private Script randomScript(String script) {
        if (randomBoolean()) {
            return mockScript(script);
        } else {
            ScriptType type = randomFrom(ScriptType.values());
            return new Script(
                type, type == ScriptType.STORED ? null : randomFrom("my_lang", Script.DEFAULT_SCRIPT_LANG), script, Collections.emptyMap());
        }
    }

}
