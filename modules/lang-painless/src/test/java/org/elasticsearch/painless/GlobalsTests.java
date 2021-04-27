/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.painless;

import org.elasticsearch.painless.spi.Whitelist;
import org.elasticsearch.script.ScriptContext;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;


public class GlobalsTests extends ScriptTestCase {

    /**
     * Script contexts used to build the script engine. Override to customize which script contexts are available.
     */
    @Override
    protected Map<ScriptContext<?>, List<Whitelist>> scriptContexts() {
        Map<ScriptContext<?>, List<Whitelist>> contexts = super.scriptContexts();
        contexts.put(MockGlobalTestScript.CONTEXT, new ArrayList<>(Whitelist.BASE_WHITELISTS));

        return contexts;
    }

    public abstract static class MockGlobalTestScript {

        private final Map<String, Object> params;
        private final int other;

        public MockGlobalTestScript(Map<String, Object> params, int other) {
            this.params = params;
            this.other = other;
        }

        /** Return the parameters for this script. */
        public Map<String, Object> getParams() {
            return params;
        }

        public int getOther() { return other; }

        public abstract Object execute(double foo, String bar, Map<String, Object> baz);

        public interface Factory {
            MockGlobalTestScript newInstance(Map<String, Object> params, int other);
        }

        public static final String[] PARAMETERS = {"foo", "bar", "baz"};
        public static final ScriptContext<MockGlobalTestScript.Factory> CONTEXT =
            new ScriptContext<>("painless_test_global", MockGlobalTestScript.Factory.class);
    }

    public Object exec(String script, Map<String, Object> vars, int other, double foo, String bar, Map<String, Object> baz) {
        // test actual script execution
        MockGlobalTestScript.Factory factory = scriptEngine.compile(null, script, MockGlobalTestScript.CONTEXT, Collections.emptyMap());
        MockGlobalTestScript testScript = factory.newInstance(vars == null ? Collections.emptyMap() : vars, other);
        return testScript.execute(foo, bar, baz);
    }

    public void testGlobals() {
        String script = "def bar(Map params) { params['foo'] } \n" +
            "int baz() { int params = 123; return params } \n" +
            "int i = 1; \n" +
            "bar(params) + baz() + params['bar']";
        Object result = exec(script, Map.of("foo", 124, "bar", 125), 99, 1.5, "", Collections.emptyMap());
        assertEquals(124 + 123 + 125, result);
    }

    public void testGlobalsOverrideProxyInMain() {
        String script = "Map params = ['foo': 1]; return params['foo']";
        IllegalArgumentException e = expectScriptThrows(IllegalArgumentException.class, () ->
            exec(script, Map.of("foo", 124), 99, 1.5, "", Collections.emptyMap())
        );
        assertEquals("variable [params] is already defined", e.getMessage());
    }

    public void testGlobalsOverrideArgumentInMain() {
        String script = "String bar = 'abc'; return bar;";
        IllegalArgumentException e = expectScriptThrows(IllegalArgumentException.class, () ->
            exec(script, Map.of("foo", 124), 99, 1.5, "bar", Collections.emptyMap())
        );
        assertEquals("variable [bar] is already defined", e.getMessage());
    }

    public void testNoLocalShadowing() {
        String script = "def five(int abc) { int abc = 5; return abc } five(6)";
        IllegalArgumentException e = expectScriptThrows(IllegalArgumentException.class, () ->
            exec(script, Map.of("foo", 124), 99, 1.5, "bar", Collections.emptyMap())
        );
        assertEquals("variable [abc] is already defined", e.getMessage());
    }

    public void testNoLocalShadowingOfGlobal() {
        // Other is a parameter
        String script = "other";
        assertEquals(99, exec(script, Map.of("foo", 124), 99, 1.5, "bar", Collections.emptyMap()));

        // Shadowing other once is fine
        script = "def five() { int other = 5; return other } five() + other";
        assertEquals(99 + 5, exec(script, Map.of("foo", 124), 99, 1.5, "bar", Collections.emptyMap()));

        // Shadowing other twice is not fine
        String s = "def five(int other) { int other = 5; return other } five(123) + other";
        IllegalArgumentException e = expectScriptThrows(IllegalArgumentException.class, () ->
            exec(s, Map.of("foo", 124), 99, 1.5, "bar", Collections.emptyMap())
        );
        assertEquals("variable [other] is already defined", e.getMessage());
    }
}

