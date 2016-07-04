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

package org.elasticsearch.script.javascript;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.script.CompiledScript;
import org.elasticsearch.script.ExecutableScript;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.test.ESTestCase;
import org.junit.After;
import org.junit.Before;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

/**
 *
 */
public class JavaScriptScriptEngineTests extends ESTestCase {
    private JavaScriptScriptEngineService se;

    @Before
    public void setup() {
        se = new JavaScriptScriptEngineService(Settings.Builder.EMPTY_SETTINGS);
    }

    @After
    public void close() {
        se.close();
    }

    public void testSimpleEquation() {
        Map<String, Object> vars = new HashMap<String, Object>();
        Object o = se.executable(new CompiledScript(ScriptService.ScriptType.INLINE, "testSimpleEquation", "js", se.compile(null, "1 + 2", Collections.emptyMap())), vars).run();
        assertThat(((Number) o).intValue(), equalTo(3));
    }

    @SuppressWarnings("unchecked")
    public void testMapAccess() {
        Map<String, Object> vars = new HashMap<String, Object>();

        Map<String, Object> obj2 = MapBuilder.<String, Object>newMapBuilder().put("prop2", "value2").map();
        Map<String, Object> obj1 = MapBuilder.<String, Object>newMapBuilder().put("prop1", "value1").put("obj2", obj2).put("l", Arrays.asList("2", "1")).map();
        vars.put("obj1", obj1);
        Object o = se.executable(new CompiledScript(ScriptService.ScriptType.INLINE, "testMapAccess", "js", se.compile(null, "obj1", Collections.emptyMap())), vars).run();
        assertThat(o, instanceOf(Map.class));
        obj1 = (Map<String, Object>) o;
        assertThat((String) obj1.get("prop1"), equalTo("value1"));
        assertThat((String) ((Map<String, Object>) obj1.get("obj2")).get("prop2"), equalTo("value2"));

        o = se.executable(new CompiledScript(ScriptService.ScriptType.INLINE, "testMapAccess", "js", se.compile(null, "obj1.l[0]", Collections.emptyMap())), vars).run();
        assertThat(((String) o), equalTo("2"));
    }

    @SuppressWarnings("unchecked")
    public void testJavaScriptObjectToMap() {
        Map<String, Object> vars = new HashMap<String, Object>();
        Object o = se.executable(new CompiledScript(ScriptService.ScriptType.INLINE, "testJavaScriptObjectToMap", "js",
                se.compile(null, "var obj1 = {}; obj1.prop1 = 'value1'; obj1.obj2 = {}; obj1.obj2.prop2 = 'value2'; obj1", Collections.emptyMap())), vars).run();
        Map<String, Object> obj1 = (Map<String, Object>) o;
        assertThat((String) obj1.get("prop1"), equalTo("value1"));
        assertThat((String) ((Map<String, Object>) obj1.get("obj2")).get("prop2"), equalTo("value2"));
    }

    @SuppressWarnings("unchecked")
    public void testJavaScriptObjectMapInter() {
        Map<String, Object> vars = new HashMap<String, Object>();
        Map<String, Object> ctx = new HashMap<String, Object>();
        Map<String, Object> obj1 = new HashMap<String, Object>();
        obj1.put("prop1", "value1");
        ctx.put("obj1", obj1);
        vars.put("ctx", ctx);

        ExecutableScript executable = se.executable(new CompiledScript(ScriptService.ScriptType.INLINE, "testJavaScriptObjectMapInter", "js",
                se.compile(null, "ctx.obj2 = {}; ctx.obj2.prop2 = 'value2'; ctx.obj1.prop1 = 'uvalue1'", Collections.emptyMap())), vars);
        executable.run();
        ctx = (Map<String, Object>) executable.unwrap(vars.get("ctx"));
        assertThat(ctx.containsKey("obj1"), equalTo(true));
        assertThat((String) ((Map<String, Object>) ctx.get("obj1")).get("prop1"), equalTo("uvalue1"));
        assertThat(ctx.containsKey("obj2"), equalTo(true));
        assertThat((String) ((Map<String, Object>) ctx.get("obj2")).get("prop2"), equalTo("value2"));
    }

    @SuppressWarnings("unchecked")
    public void testJavaScriptInnerArrayCreation() {
        Map<String, Object> ctx = new HashMap<String, Object>();
        Map<String, Object> doc = new HashMap<String, Object>();
        ctx.put("doc", doc);

        Object compiled = se.compile(null, "ctx.doc.field1 = ['value1', 'value2']", Collections.emptyMap());
        ExecutableScript script = se.executable(new CompiledScript(ScriptService.ScriptType.INLINE, "testJavaScriptInnerArrayCreation", "js",
                compiled), new HashMap<String, Object>());
        script.setNextVar("ctx", ctx);
        script.run();

        Map<String, Object> unwrap = (Map<String, Object>) script.unwrap(ctx);

        assertThat(((Map<String, Object>) unwrap.get("doc")).get("field1"), instanceOf(List.class));
    }

    @SuppressWarnings("unchecked")
    public void testAccessListInScript() {
        Map<String, Object> vars = new HashMap<String, Object>();
        Map<String, Object> obj2 = MapBuilder.<String, Object>newMapBuilder().put("prop2", "value2").map();
        Map<String, Object> obj1 = MapBuilder.<String, Object>newMapBuilder().put("prop1", "value1").put("obj2", obj2).map();
        vars.put("l", Arrays.asList("1", "2", "3", obj1));

        Object o = se.executable(new CompiledScript(ScriptService.ScriptType.INLINE, "testAccessInScript", "js",
                se.compile(null, "l.length", Collections.emptyMap())), vars).run();
        assertThat(((Number) o).intValue(), equalTo(4));

        o = se.executable(new CompiledScript(ScriptService.ScriptType.INLINE, "testAccessInScript", "js",
                se.compile(null, "l[0]", Collections.emptyMap())), vars).run();
        assertThat(((String) o), equalTo("1"));

        o = se.executable(new CompiledScript(ScriptService.ScriptType.INLINE, "testAccessInScript", "js",
                se.compile(null, "l[3]", Collections.emptyMap())), vars).run();
        obj1 = (Map<String, Object>) o;
        assertThat((String) obj1.get("prop1"), equalTo("value1"));
        assertThat((String) ((Map<String, Object>) obj1.get("obj2")).get("prop2"), equalTo("value2"));

        o = se.executable(new CompiledScript(ScriptService.ScriptType.INLINE, "testAccessInScript", "js",
                se.compile(null, "l[3].prop1", Collections.emptyMap())), vars).run();
        assertThat(((String) o), equalTo("value1"));
    }

    public void testChangingVarsCrossExecution1() {
        Map<String, Object> vars = new HashMap<String, Object>();
        Map<String, Object> ctx = new HashMap<String, Object>();
        vars.put("ctx", ctx);
        Object compiledScript = se.compile(null, "ctx.value", Collections.emptyMap());

        ExecutableScript script = se.executable(new CompiledScript(ScriptService.ScriptType.INLINE, "testChangingVarsCrossExecution1", "js",
                compiledScript), vars);
        ctx.put("value", 1);
        Object o = script.run();
        assertThat(((Number) o).intValue(), equalTo(1));

        ctx.put("value", 2);
        o = script.run();
        assertThat(((Number) o).intValue(), equalTo(2));
    }

    public void testChangingVarsCrossExecution2() {
        Map<String, Object> vars = new HashMap<String, Object>();
        Object compiledScript = se.compile(null, "value", Collections.emptyMap());

        ExecutableScript script = se.executable(new CompiledScript(ScriptService.ScriptType.INLINE, "testChangingVarsCrossExecution2", "js",
                compiledScript), vars);
        script.setNextVar("value", 1);
        Object o = script.run();
        assertThat(((Number) o).intValue(), equalTo(1));

        script.setNextVar("value", 2);
        o = script.run();
        assertThat(((Number) o).intValue(), equalTo(2));
    }
}
