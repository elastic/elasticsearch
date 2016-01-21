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

import org.elasticsearch.common.ContextAndHeaderHolder;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.test.ESTestCase;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class ScriptContextTests extends ESTestCase {

    private static final String PLUGIN_NAME = "testplugin";

    ScriptService makeScriptService() throws Exception {
        Settings settings = Settings.builder()
            .put("path.home", createTempDir())
            // no file watching, so we don't need a ResourceWatcherService
            .put(ScriptService.SCRIPT_AUTO_RELOAD_ENABLED_SETTING, false)
            .put("script." + PLUGIN_NAME + "_custom_globally_disabled_op", false)
            .put("script.engine." + MockScriptEngine.NAME + ".inline." + PLUGIN_NAME + "_custom_exp_disabled_op", false)
            .build();
        Set<ScriptEngineService> engines = new HashSet<ScriptEngineService>(Collections.singletonList(new MockScriptEngine()));
        List<ScriptContext.Plugin> customContexts = Arrays.asList(
            new ScriptContext.Plugin(PLUGIN_NAME, "custom_op"),
            new ScriptContext.Plugin(PLUGIN_NAME, "custom_exp_disabled_op"),
            new ScriptContext.Plugin(PLUGIN_NAME, "custom_globally_disabled_op"));
        return new ScriptService(settings, new Environment(settings), engines, null, new ScriptContextRegistry(customContexts));
    }

    public void testCustomGlobalScriptContextSettings() throws Exception {
        ContextAndHeaderHolder contextAndHeaders = new ContextAndHeaderHolder();
        ScriptService scriptService = makeScriptService();
        for (ScriptService.ScriptType scriptType : ScriptService.ScriptType.values()) {
            try {
                Script script = new Script("1", scriptType, MockScriptEngine.NAME, null);
                scriptService.compile(script, new ScriptContext.Plugin(PLUGIN_NAME, "custom_globally_disabled_op"), contextAndHeaders, Collections.<String, String>emptyMap());
                fail("script compilation should have been rejected");
            } catch (ScriptException e) {
                assertTrue(e.getMessage(), e.getMessage().contains("scripts of type [" + scriptType + "], operation [" + PLUGIN_NAME + "_custom_globally_disabled_op] and lang [" + MockScriptEngine.NAME + "] are disabled"));
            }
        }
    }

    public void testCustomScriptContextSettings() throws Exception {
        ContextAndHeaderHolder contextAndHeaders = new ContextAndHeaderHolder();
        ScriptService scriptService = makeScriptService();
        Script script = new Script("1", ScriptService.ScriptType.INLINE, MockScriptEngine.NAME, null);
        try {
            scriptService.compile(script, new ScriptContext.Plugin(PLUGIN_NAME, "custom_exp_disabled_op"), contextAndHeaders, Collections.<String, String>emptyMap());
            fail("script compilation should have been rejected");
        } catch (ScriptException e) {
            assertTrue(e.getMessage(), e.getMessage().contains("scripts of type [inline], operation [" + PLUGIN_NAME + "_custom_exp_disabled_op] and lang [" + MockScriptEngine.NAME + "] are disabled"));
        }

        // still works for other script contexts
        assertNotNull(scriptService.compile(script, ScriptContext.Standard.AGGS, contextAndHeaders, Collections.<String, String>emptyMap()));
        assertNotNull(scriptService.compile(script, ScriptContext.Standard.SEARCH, contextAndHeaders, Collections.<String, String>emptyMap()));
        assertNotNull(scriptService.compile(script, new ScriptContext.Plugin(PLUGIN_NAME, "custom_op"), contextAndHeaders, Collections.<String, String>emptyMap()));
    }

    public void testUnknownPluginScriptContext() throws Exception {
        ContextAndHeaderHolder contextAndHeaders = new ContextAndHeaderHolder();
        ScriptService scriptService = makeScriptService();
        for (ScriptService.ScriptType scriptType : ScriptService.ScriptType.values()) {
            try {
                Script script = new Script("1", scriptType, MockScriptEngine.NAME, null);
                scriptService.compile(script, new ScriptContext.Plugin(PLUGIN_NAME, "unknown"), contextAndHeaders, Collections.<String, String>emptyMap());
                fail("script compilation should have been rejected");
            } catch (IllegalArgumentException e) {
                assertTrue(e.getMessage(), e.getMessage().contains("script context [" + PLUGIN_NAME + "_unknown] not supported"));
            }
        }
    }

    public void testUnknownCustomScriptContext() throws Exception {
        ContextAndHeaderHolder contextAndHeaders = new ContextAndHeaderHolder();
        ScriptContext context = new ScriptContext() {
            @Override
            public String getKey() {
                return "test";
            }
        };
        ScriptService scriptService = makeScriptService();
        for (ScriptService.ScriptType scriptType : ScriptService.ScriptType.values()) {
            try {
                Script script = new Script("1", scriptType, MockScriptEngine.NAME, null);
                scriptService.compile(script, context, contextAndHeaders, Collections.<String, String>emptyMap());
                fail("script compilation should have been rejected");
            } catch (IllegalArgumentException e) {
                assertTrue(e.getMessage(), e.getMessage().contains("script context [test] not supported"));
            }
        }
    }

}
