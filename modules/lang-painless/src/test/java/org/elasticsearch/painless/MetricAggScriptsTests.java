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

package org.elasticsearch.painless;

import org.elasticsearch.painless.spi.Whitelist;
import org.elasticsearch.script.MetricAggScripts;
import org.elasticsearch.script.ScriptContext;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MetricAggScriptsTests extends ScriptTestCase {
    @Override
    protected Map<ScriptContext<?>, List<Whitelist>> scriptContexts() {
        Map<ScriptContext<?>, List<Whitelist>> contexts = new HashMap<>();
        contexts.put(MetricAggScripts.InitScript.CONTEXT, Whitelist.BASE_WHITELISTS);
        contexts.put(MetricAggScripts.MapScript.CONTEXT, Whitelist.BASE_WHITELISTS);
        contexts.put(MetricAggScripts.CombineScript.CONTEXT, Whitelist.BASE_WHITELISTS);
        contexts.put(MetricAggScripts.ReduceScript.CONTEXT, Whitelist.BASE_WHITELISTS);
        return contexts;
    }

    public void testInitBasic() {
        MetricAggScripts.InitScript.Factory factory = scriptEngine.compile("test",
            "agg.testField = params.initialVal", MetricAggScripts.InitScript.CONTEXT, Collections.emptyMap());

        Map<String, Object> params = new HashMap<>();
        Map<String, Object> agg = new HashMap<>();

        params.put("initialVal", 10);

        MetricAggScripts.InitScript script = factory.newInstance(params, agg);
        script.execute();

        assert(agg.containsKey("testField"));
        assertEquals(10, agg.get("testField"));
    }

    public void testMapBasic() {
        MetricAggScripts.MapScript.Factory factory = scriptEngine.compile("test",
            "agg.testField = 2*_score", MetricAggScripts.MapScript.CONTEXT, Collections.emptyMap());

        Map<String, Object> params = new HashMap<>();
        Map<String, Object> agg = new HashMap<>();
        double _score = 0.5;

        MetricAggScripts.MapScript.LeafFactory leafFactory = factory.newFactory(params, agg, null);
        MetricAggScripts.MapScript script = leafFactory.newInstance(null);

        script.execute(_score);

        assert(agg.containsKey("testField"));
        assertEquals(1.0, agg.get("testField"));
    }

    public void testCombineBasic() {
        MetricAggScripts.CombineScript.Factory factory = scriptEngine.compile("test",
            "agg.testField = params.initialVal; return agg.testField + params.inc", MetricAggScripts.CombineScript.CONTEXT,
            Collections.emptyMap());

        Map<String, Object> params = new HashMap<>();
        Map<String, Object> agg = new HashMap<>();

        params.put("initialVal", 10);
        params.put("inc", 2);

        MetricAggScripts.CombineScript script = factory.newInstance(params, agg);
        Object res = script.execute();

        assert(agg.containsKey("testField"));
        assertEquals(10, agg.get("testField"));
        assertEquals(12, res);
    }

    public void testReduceBasic() {
        MetricAggScripts.ReduceScript.Factory factory = scriptEngine.compile("test",
            "aggs[0].testField + aggs[1].testField", MetricAggScripts.ReduceScript.CONTEXT, Collections.emptyMap());

        Map<String, Object> params = new HashMap<>();
        List<Object> aggs = new ArrayList<>();

        Map<String, Object> agg1 = new HashMap<>(), agg2 = new HashMap<>();
        agg1.put("testField", 1);
        agg2.put("testField", 2);

        aggs.add(agg1);
        aggs.add(agg2);

        MetricAggScripts.ReduceScript script = factory.newInstance(params, aggs);
        Object res = script.execute();
        assertEquals(3, res);
    }
}
