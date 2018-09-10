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

import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Scorer;
import org.elasticsearch.painless.spi.Whitelist;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.script.ScriptedMetricAggContexts;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ScriptedMetricAggContextsTests extends ScriptTestCase {
    @Override
    protected Map<ScriptContext<?>, List<Whitelist>> scriptContexts() {
        Map<ScriptContext<?>, List<Whitelist>> contexts = new HashMap<>();
        contexts.put(ScriptedMetricAggContexts.InitScript.CONTEXT, Whitelist.BASE_WHITELISTS);
        contexts.put(ScriptedMetricAggContexts.MapScript.CONTEXT, Whitelist.BASE_WHITELISTS);
        contexts.put(ScriptedMetricAggContexts.CombineScript.CONTEXT, Whitelist.BASE_WHITELISTS);
        contexts.put(ScriptedMetricAggContexts.ReduceScript.CONTEXT, Whitelist.BASE_WHITELISTS);
        return contexts;
    }

    public void testInitBasic() {
        ScriptedMetricAggContexts.InitScript.Factory factory = scriptEngine.compile("test",
            "state.testField = params.initialVal", ScriptedMetricAggContexts.InitScript.CONTEXT, Collections.emptyMap());

        Map<String, Object> params = new HashMap<>();
        Map<String, Object> state = new HashMap<>();

        params.put("initialVal", 10);

        ScriptedMetricAggContexts.InitScript script = factory.newInstance(params, state);
        script.execute();

        assert(state.containsKey("testField"));
        assertEquals(10, state.get("testField"));
    }

    public void testMapBasic() {
        ScriptedMetricAggContexts.MapScript.Factory factory = scriptEngine.compile("test",
            "state.testField = 2*_score", ScriptedMetricAggContexts.MapScript.CONTEXT, Collections.emptyMap());

        Map<String, Object> params = new HashMap<>();
        Map<String, Object> state = new HashMap<>();

        Scorer scorer = new Scorer(null) {
            @Override
            public int docID() { return 0; }

            @Override
            public float score() { return 0.5f; }

            @Override
            public DocIdSetIterator iterator() { return null; }

            @Override
            public float getMaxScore(int upTo) throws IOException {
                return 0.5f;
            }
        };

        ScriptedMetricAggContexts.MapScript.LeafFactory leafFactory = factory.newFactory(params, state, null);
        ScriptedMetricAggContexts.MapScript script = leafFactory.newInstance(null);

        script.setScorer(scorer);
        script.execute();

        assert(state.containsKey("testField"));
        assertEquals(1.0, state.get("testField"));
    }

    public void testCombineBasic() {
        ScriptedMetricAggContexts.CombineScript.Factory factory = scriptEngine.compile("test",
            "state.testField = params.initialVal; return state.testField + params.inc", ScriptedMetricAggContexts.CombineScript.CONTEXT,
            Collections.emptyMap());

        Map<String, Object> params = new HashMap<>();
        Map<String, Object> state = new HashMap<>();

        params.put("initialVal", 10);
        params.put("inc", 2);

        ScriptedMetricAggContexts.CombineScript script = factory.newInstance(params, state);
        Object res = script.execute();

        assert(state.containsKey("testField"));
        assertEquals(10, state.get("testField"));
        assertEquals(12, res);
    }

    public void testReduceBasic() {
        ScriptedMetricAggContexts.ReduceScript.Factory factory = scriptEngine.compile("test",
            "states[0].testField + states[1].testField", ScriptedMetricAggContexts.ReduceScript.CONTEXT, Collections.emptyMap());

        Map<String, Object> params = new HashMap<>();
        List<Object> states = new ArrayList<>();

        Map<String, Object> state1 = new HashMap<>(), state2 = new HashMap<>();
        state1.put("testField", 1);
        state2.put("testField", 2);

        states.add(state1);
        states.add(state2);

        ScriptedMetricAggContexts.ReduceScript script = factory.newInstance(params, states);
        Object res = script.execute();
        assertEquals(3, res);
    }
}
