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

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.test.ESTestCase;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.hamcrest.Matchers.containsString;

public class ScriptContextTests extends ESTestCase {

    private static final String PLUGIN_NAME = "testplugin";

    ScriptService makeScriptService() throws Exception {
        Settings settings = Settings.builder()
            .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir())
            // no file watching, so we don't need a ResourceWatcherService
            .put(ScriptService.SCRIPT_AUTO_RELOAD_ENABLED_SETTING.getKey(), "off")
            .put("script." + PLUGIN_NAME + "_custom_globally_disabled_op", "false")
            .put("script.engine." + MockScriptEngine.NAME + ".inline." + PLUGIN_NAME + "_custom_exp_disabled_op", "false")
            .build();
        ScriptEngineRegistry scriptEngineRegistry = new ScriptEngineRegistry(Collections.singletonList(new MockScriptEngine()));
        List<ScriptContext.Plugin> customContexts = Arrays.asList(
            new ScriptContext.Plugin(PLUGIN_NAME, "custom_op"),
            new ScriptContext.Plugin(PLUGIN_NAME, "custom_exp_disabled_op"),
            new ScriptContext.Plugin(PLUGIN_NAME, "custom_globally_disabled_op"));
        ScriptContextRegistry scriptContextRegistry = new ScriptContextRegistry(customContexts);
        ScriptSettings scriptSettings = new ScriptSettings(scriptEngineRegistry, scriptContextRegistry);

        return new ScriptService(settings, new Environment(settings), null, scriptEngineRegistry, scriptContextRegistry, scriptSettings);
    }

    public void testCustomGlobalScriptContextSettings() throws Exception {
        ScriptService scriptService = makeScriptService();
        for (ScriptService.ScriptType scriptType : ScriptService.ScriptType.values()) {
            try {
                Script script = new Script("1", scriptType, MockScriptEngine.NAME, null);
                scriptService.compile(script, new ScriptContext.Plugin(PLUGIN_NAME, "custom_globally_disabled_op"), Collections.emptyMap(), null);
                fail("script compilation should have been rejected");
            } catch (IllegalStateException e) {
                assertThat(e.getMessage(), containsString("scripts of type [" + scriptType + "], operation [" + PLUGIN_NAME + "_custom_globally_disabled_op] and lang [" + MockScriptEngine.NAME + "] are disabled"));
            }
        }
    }

    public void testCustomScriptContextSettings() throws Exception {
        ScriptService scriptService = makeScriptService();
        Script script = new Script("1", ScriptService.ScriptType.INLINE, MockScriptEngine.NAME, null);
        try {
            scriptService.compile(script, new ScriptContext.Plugin(PLUGIN_NAME, "custom_exp_disabled_op"), Collections.emptyMap(), null);
            fail("script compilation should have been rejected");
        } catch (IllegalStateException e) {
            assertTrue(e.getMessage(), e.getMessage().contains("scripts of type [inline], operation [" + PLUGIN_NAME + "_custom_exp_disabled_op] and lang [" + MockScriptEngine.NAME + "] are disabled"));
        }

        // still works for other script contexts
        assertNotNull(scriptService.compile(script, ScriptContext.Standard.AGGS, Collections.emptyMap(), null));
        assertNotNull(scriptService.compile(script, ScriptContext.Standard.SEARCH, Collections.emptyMap(), null));
        assertNotNull(scriptService.compile(script, new ScriptContext.Plugin(PLUGIN_NAME, "custom_op"), Collections.emptyMap(), null));
    }

    public void testUnknownPluginScriptContext() throws Exception {
        ScriptService scriptService = makeScriptService();
        for (ScriptService.ScriptType scriptType : ScriptService.ScriptType.values()) {
            try {
                Script script = new Script("1", scriptType, MockScriptEngine.NAME, null);
                scriptService.compile(script, new ScriptContext.Plugin(PLUGIN_NAME, "unknown"), Collections.emptyMap(), null);
                fail("script compilation should have been rejected");
            } catch (IllegalArgumentException e) {
                assertTrue(e.getMessage(), e.getMessage().contains("script context [" + PLUGIN_NAME + "_unknown] not supported"));
            }
        }
    }

    public void testUnknownCustomScriptContext() throws Exception {
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
                scriptService.compile(script, context, Collections.emptyMap(), null);
                fail("script compilation should have been rejected");
            } catch (IllegalArgumentException e) {
                assertTrue(e.getMessage(), e.getMessage().contains("script context [test] not supported"));
            }
        }
    }

}
