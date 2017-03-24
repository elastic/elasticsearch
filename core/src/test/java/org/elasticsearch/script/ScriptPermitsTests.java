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

package org.elasticsearch.script;

import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.search.lookup.SearchLookup;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;

public class ScriptPermitsTests extends ESTestCase {
    private static final String PLUGIN_NAME = "testplugin";

    private ScriptPermits permits;

    @Before
    public void makePermist() throws Exception {
        Settings settings = Settings.builder()
            .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir())
            // no file watching, so we don't need a ResourceWatcherService
            .put(ScriptService.SCRIPT_AUTO_RELOAD_ENABLED_SETTING.getKey(), "false")
            .put("script." + PLUGIN_NAME + "_custom_globally_disabled_op", "false")
            .put("script.engine." + MockScriptEngine.NAME + ".inline."
                    + PLUGIN_NAME + "_custom_exp_disabled_op", "false")
            .put("script.engine.mock_template.inline."
                    + PLUGIN_NAME + "_custom_exp_disabled_op", "false")
            .build();

        TemplateService.Backend templateBackend = new MockTemplateBackend();
        MockScriptEngine scriptEngine = new MockScriptEngine(MockScriptEngine.NAME,
                singletonMap("1", script -> "1"));
        ScriptEngineRegistry scriptEngineRegistry = new ScriptEngineRegistry(
                singletonList(scriptEngine));
        List<ScriptContext.Plugin> customContexts = Arrays.asList(
            new ScriptContext.Plugin(PLUGIN_NAME, "custom_op"),
            new ScriptContext.Plugin(PLUGIN_NAME, "custom_exp_disabled_op"),
            new ScriptContext.Plugin(PLUGIN_NAME, "custom_globally_disabled_op"));
        ScriptContextRegistry scriptContextRegistry = new ScriptContextRegistry(customContexts);
        ScriptSettings scriptSettings = new ScriptSettings(scriptEngineRegistry,
                templateBackend, scriptContextRegistry);
        permits = new ScriptPermits(settings, scriptSettings, scriptContextRegistry);
    }

    public void testCompilationCircuitBreaking() throws Exception {
        permits.setMaxCompilationsPerMinute(1);
        permits.checkCompilationLimit(); // should pass
        expectThrows(CircuitBreakingException.class, () -> permits.checkCompilationLimit());
        permits.setMaxCompilationsPerMinute(2);
        permits.checkCompilationLimit(); // should pass
        permits.checkCompilationLimit(); // should pass
        expectThrows(CircuitBreakingException.class, () -> permits.checkCompilationLimit());
        int count = randomIntBetween(5, 50);
        permits.setMaxCompilationsPerMinute(count);
        for (int i = 0; i < count; i++) {
            permits.checkCompilationLimit(); // should pass
        }
        expectThrows(CircuitBreakingException.class, () -> permits.checkCompilationLimit());
        permits.setMaxCompilationsPerMinute(0);
        expectThrows(CircuitBreakingException.class, () -> permits.checkCompilationLimit());
        permits.setMaxCompilationsPerMinute(Integer.MAX_VALUE);
        int largeLimit = randomIntBetween(1000, 10000);
        for (int i = 0; i < largeLimit; i++) {
            permits.checkCompilationLimit();
        }
    }

    public void testCustomGlobalScriptContextSettings() throws Exception {
        for (ScriptType scriptType : ScriptType.values()) {
            assertFalse(permits.checkContextPermissions(MockScriptEngine.NAME, scriptType,
                    new ScriptContext.Plugin(PLUGIN_NAME, "custom_globally_disabled_op")));
            assertFalse(permits.checkContextPermissions("mock_template", scriptType,
                    new ScriptContext.Plugin(PLUGIN_NAME, "custom_globally_disabled_op")));
        }
    }

    public void testCustomScriptContextSettings() throws Exception {
        assertFalse(permits.checkContextPermissions(MockScriptEngine.NAME, ScriptType.INLINE,
                new ScriptContext.Plugin(PLUGIN_NAME, "custom_exp_disabled_op")));
        assertFalse(permits.checkContextPermissions("mock_template", ScriptType.INLINE,
                new ScriptContext.Plugin(PLUGIN_NAME, "custom_exp_disabled_op")));

        // still works for other script contexts
        assertTrue(permits.checkContextPermissions(MockScriptEngine.NAME, ScriptType.INLINE,
                ScriptContext.Standard.AGGS));
        assertTrue(permits.checkContextPermissions(MockScriptEngine.NAME, ScriptType.INLINE,
                ScriptContext.Standard.SEARCH));
        assertTrue(permits.checkContextPermissions(MockScriptEngine.NAME, ScriptType.INLINE,
                new ScriptContext.Plugin(PLUGIN_NAME, "custom_op")));
        assertTrue(permits.checkContextPermissions("mock_template", ScriptType.INLINE,
                ScriptContext.Standard.AGGS));
        assertTrue(permits.checkContextPermissions("mock_template", ScriptType.INLINE,
                ScriptContext.Standard.SEARCH));
        assertTrue(permits.checkContextPermissions("mock_template", ScriptType.INLINE,
                new ScriptContext.Plugin(PLUGIN_NAME, "custom_op")));
    }

    public void testUnknownPluginScriptContext() throws Exception {
        for (ScriptType scriptType : ScriptType.values()) {
            Exception e = expectThrows(IllegalArgumentException.class, () ->
                    permits.checkContextPermissions(MockScriptEngine.NAME, scriptType,
                            new ScriptContext.Plugin(PLUGIN_NAME, "unknown")));
            assertEquals("script context [testplugin_unknown] not supported", e.getMessage());
            e = expectThrows(IllegalArgumentException.class, () ->
                    permits.checkContextPermissions("mock_template", scriptType,
                            new ScriptContext.Plugin(PLUGIN_NAME, "unknown")));
            assertEquals("script context [testplugin_unknown] not supported", e.getMessage());
        }
    }

    public void testUnknownCustomScriptContext() throws Exception {
        ScriptContext context = new ScriptContext() {
            @Override
            public String getKey() {
                return "test";
            }
        };
        for (ScriptType scriptType : ScriptType.values()) {
            Exception e = expectThrows(IllegalArgumentException.class, () ->
                    permits.checkContextPermissions(MockScriptEngine.NAME, scriptType, context));
            assertEquals("script context [test] not supported", e.getMessage());
            e = expectThrows(IllegalArgumentException.class, () ->
                    permits.checkContextPermissions("mock_template", scriptType, context));
            assertEquals("script context [test] not supported", e.getMessage());
        }
    }

    static class MockTemplateBackend implements TemplateService.Backend {
        @Override
        public String getType() {
            return "mock_template";
        }

        @Override
        public String getExtension() {
            return "mock_template";
        }

        @Override
        public Object compile(String scriptName, String scriptSource, Map<String, String> params) {
            return scriptSource;
        }

        @Override
        public ExecutableScript executable(CompiledScript compiledScript,
                Map<String, Object> vars) {
            return new ExecutableScript() {
                @Override
                public void setNextVar(String name, Object value) {
                }

                @Override
                public Object run() {
                    return new BytesArray((String) compiledScript.compiled());
                }
            };
        }

        @Override
        public SearchScript search(CompiledScript compiledScript, SearchLookup lookup,
                Map<String, Object> vars) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void close() throws IOException {
        }
    }
}
