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

import org.elasticsearch.script.CompiledScript;
import org.elasticsearch.script.ExecutableScript;
import org.elasticsearch.script.ScriptService;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class ScriptEngineTests extends ScriptTestCase {

    public void testSimpleEquation() {
        final Object value = exec("return 1 + 2;");
        assertEquals(3, ((Number)value).intValue());
    }

    @SuppressWarnings("unchecked") // We know its Map<String, Object> because we put them there in the test
    public void testMapAccess() {
        Map<String, Object> vars = new HashMap<>();
        Map<String, Object> obj2 = new HashMap<>();
        obj2.put("prop2", "value2");
        Map<String, Object> obj1 = new HashMap<>();
        obj1.put("prop1", "value1");
        obj1.put("obj2", obj2);
        obj1.put("l", Arrays.asList("2", "1"));
        vars.put("obj1", obj1);

        Object value = exec("return params['obj1'];", vars, true);
        obj1 = (Map<String, Object>)value;
        assertEquals("value1", obj1.get("prop1"));
        assertEquals("value2", ((Map<String, Object>) obj1.get("obj2")).get("prop2"));

        value = exec("return params.obj1.l.0;", vars, true);
        assertEquals("2", value);
    }

    @SuppressWarnings("unchecked") // We know its Map<String, Object> because we put them there ourselves
    public void testAccessListInScript() {
        Map<String, Object> vars = new HashMap<>();
        Map<String, Object> obj2 = new HashMap<>();
        obj2.put("prop2", "value2");
        Map<String, Object> obj1 = new HashMap<>();
        obj1.put("prop1", "value1");
        obj1.put("obj2", obj2);
        vars.put("l", Arrays.asList("1", "2", "3", obj1));

        assertEquals(4, exec("return params.l.size();", vars, true));
        assertEquals("1", exec("return params.l.0;", vars, true));

        Object value = exec("return params.l.3;", vars, true);
        obj1 = (Map<String, Object>)value;
        assertEquals("value1", obj1.get("prop1"));
        assertEquals("value2", ((Map<String, Object>)obj1.get("obj2")).get("prop2"));

        assertEquals("value1", exec("return params.l.3.prop1;", vars, true));
    }

    public void testChangingVarsCrossExecution1() {
        Map<String, Object> vars = new HashMap<>();
        Map<String, Object> ctx = new HashMap<>();
        vars.put("ctx", ctx);

        Object compiledScript = scriptEngine.compile(null,
                "return ctx.value;", Collections.emptyMap());
        ExecutableScript script = scriptEngine.executable(new CompiledScript(ScriptService.ScriptType.INLINE,
                "testChangingVarsCrossExecution1", "painless", compiledScript), vars);

        ctx.put("value", 1);
        Object o = script.run();
        assertEquals(1, ((Number) o).intValue());

        ctx.put("value", 2);
        o = script.run();
        assertEquals(2, ((Number) o).intValue());
    }

    public void testChangingVarsCrossExecution2() {
        Map<String, Object> vars = new HashMap<>();
        Object compiledScript = scriptEngine.compile(null, "return params['value'];", Collections.emptyMap());

        ExecutableScript script = scriptEngine.executable(new CompiledScript(ScriptService.ScriptType.INLINE,
                "testChangingVarsCrossExecution2", "painless", compiledScript), vars);

        script.setNextVar("value", 1);
        Object value = script.run();
        assertEquals(1, ((Number)value).intValue());

        script.setNextVar("value", 2);
        value = script.run();
        assertEquals(2, ((Number)value).intValue());
    }
}
