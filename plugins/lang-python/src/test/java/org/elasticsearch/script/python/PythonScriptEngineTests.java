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

package org.elasticsearch.script.python;

import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.script.CompiledScript;
import org.elasticsearch.script.ExecutableScript;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.test.ESTestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

/**
 *
 */
public class PythonScriptEngineTests extends ESTestCase {

    private PythonScriptEngineService se;

    @Before
    public void setup() {
        se = new PythonScriptEngineService(Settings.Builder.EMPTY_SETTINGS);
    }

    @After
    public void close() {
        // We need to clear some system properties
        System.clearProperty("python.cachedir.skip");
        System.clearProperty("python.console.encoding");
        se.close();
    }

    @Test
    public void testSimpleEquation() {
        Map<String, Object> vars = new HashMap<String, Object>();
        Object o = se.execute(new CompiledScript(ScriptService.ScriptType.INLINE, "testSimpleEquation", "python", se.compile("1 + 2")), vars);
        assertThat(((Number) o).intValue(), equalTo(3));
    }

    @Test
    public void testMapAccess() {
        Map<String, Object> vars = new HashMap<String, Object>();

        Map<String, Object> obj2 = MapBuilder.<String, Object>newMapBuilder().put("prop2", "value2").map();
        Map<String, Object> obj1 = MapBuilder.<String, Object>newMapBuilder().put("prop1", "value1").put("obj2", obj2).put("l", Arrays.asList("2", "1")).map();
        vars.put("obj1", obj1);
        Object o = se.execute(new CompiledScript(ScriptService.ScriptType.INLINE, "testMapAccess", "python", se.compile("obj1")), vars);
        assertThat(o, instanceOf(Map.class));
        obj1 = (Map<String, Object>) o;
        assertThat((String) obj1.get("prop1"), equalTo("value1"));
        assertThat((String) ((Map<String, Object>) obj1.get("obj2")).get("prop2"), equalTo("value2"));

        o = se.execute(new CompiledScript(ScriptService.ScriptType.INLINE, "testMapAccess", "python", se.compile("obj1['l'][0]")), vars);
        assertThat(((String) o), equalTo("2"));
    }

    @Test
    public void testObjectMapInter() {
        Map<String, Object> vars = new HashMap<String, Object>();
        Map<String, Object> ctx = new HashMap<String, Object>();
        Map<String, Object> obj1 = new HashMap<String, Object>();
        obj1.put("prop1", "value1");
        ctx.put("obj1", obj1);
        vars.put("ctx", ctx);

        se.execute(new CompiledScript(ScriptService.ScriptType.INLINE, "testObjectInterMap", "python",
                se.compile("ctx['obj2'] = { 'prop2' : 'value2' }; ctx['obj1']['prop1'] = 'uvalue1'")), vars);
        ctx = (Map<String, Object>) se.unwrap(vars.get("ctx"));
        assertThat(ctx.containsKey("obj1"), equalTo(true));
        assertThat((String) ((Map<String, Object>) ctx.get("obj1")).get("prop1"), equalTo("uvalue1"));
        assertThat(ctx.containsKey("obj2"), equalTo(true));
        assertThat((String) ((Map<String, Object>) ctx.get("obj2")).get("prop2"), equalTo("value2"));
    }

    @Test
    public void testAccessListInScript() {

        Map<String, Object> vars = new HashMap<String, Object>();
        Map<String, Object> obj2 = MapBuilder.<String, Object>newMapBuilder().put("prop2", "value2").map();
        Map<String, Object> obj1 = MapBuilder.<String, Object>newMapBuilder().put("prop1", "value1").put("obj2", obj2).map();
        vars.put("l", Arrays.asList("1", "2", "3", obj1));

//        Object o = se.execute(se.compile("l.length"), vars);
//        assertThat(((Number) o).intValue(), equalTo(4));

        Object o = se.execute(new CompiledScript(ScriptService.ScriptType.INLINE, "testAccessListInScript", "python", se.compile("l[0]")), vars);
        assertThat(((String) o), equalTo("1"));

        o = se.execute(new CompiledScript(ScriptService.ScriptType.INLINE, "testAccessListInScript", "python", se.compile("l[3]")), vars);
        obj1 = (Map<String, Object>) o;
        assertThat((String) obj1.get("prop1"), equalTo("value1"));
        assertThat((String) ((Map<String, Object>) obj1.get("obj2")).get("prop2"), equalTo("value2"));

        o = se.execute(new CompiledScript(ScriptService.ScriptType.INLINE, "testAccessListInScript", "python", se.compile("l[3]['prop1']")), vars);
        assertThat(((String) o), equalTo("value1"));
    }

    @Test
    public void testChangingVarsCrossExecution1() {
        Map<String, Object> vars = new HashMap<String, Object>();
        Map<String, Object> ctx = new HashMap<String, Object>();
        vars.put("ctx", ctx);
        Object compiledScript = se.compile("ctx['value']");

        ExecutableScript script = se.executable(new CompiledScript(ScriptService.ScriptType.INLINE, "testChangingVarsCrossExecution1", "python", compiledScript), vars);
        ctx.put("value", 1);
        Object o = script.run();
        assertThat(((Number) o).intValue(), equalTo(1));

        ctx.put("value", 2);
        o = script.run();
        assertThat(((Number) o).intValue(), equalTo(2));
    }

    @Test
    public void testChangingVarsCrossExecution2() {
        Map<String, Object> vars = new HashMap<String, Object>();
        Map<String, Object> ctx = new HashMap<String, Object>();
        Object compiledScript = se.compile("value");

        ExecutableScript script = se.executable(new CompiledScript(ScriptService.ScriptType.INLINE, "testChangingVarsCrossExecution2", "python", compiledScript), vars);
        script.setNextVar("value", 1);
        Object o = script.run();
        assertThat(((Number) o).intValue(), equalTo(1));

        script.setNextVar("value", 2);
        o = script.run();
        assertThat(((Number) o).intValue(), equalTo(2));
    }
}
