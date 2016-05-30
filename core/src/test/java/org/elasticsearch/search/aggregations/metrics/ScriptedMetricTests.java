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
import org.elasticsearch.script.ScriptService.ScriptType;
import org.elasticsearch.search.aggregations.BaseAggregationTestCase;
import org.elasticsearch.search.aggregations.metrics.scripted.ScriptedMetricAggregationBuilder;

import java.util.HashMap;
import java.util.Map;

public class ScriptedMetricTests extends BaseAggregationTestCase<ScriptedMetricAggregationBuilder> {

    @Override
    protected ScriptedMetricAggregationBuilder createTestAggregatorBuilder() {
        ScriptedMetricAggregationBuilder factory = new ScriptedMetricAggregationBuilder(randomAsciiOfLengthBetween(1, 20));
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
            Map<String, Object> params = new HashMap<String, Object>();
            params.put("foo", "bar");
            factory.params(params);
        }
        return factory;
    }

    private Script randomScript(String script) {
        if (randomBoolean()) {
            return new Script(script);
        } else {
            return new Script(script, randomFrom(ScriptType.values()), randomFrom("my_lang", null), null);
        }
    }

}
