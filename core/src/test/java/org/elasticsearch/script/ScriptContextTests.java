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

import org.elasticsearch.action.admin.cluster.storedscripts.GetStoredScriptRequest;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.script.Script.ExecutableScriptBinding;
import org.elasticsearch.script.Script.ScriptInput;
import org.elasticsearch.script.Script.ScriptType;
import org.elasticsearch.script.Script.StoredScriptSource;
import org.elasticsearch.test.ESTestCase;
import org.mockito.Mock;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

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

        MockScriptEngine scriptEngine = new MockScriptEngine(MockScriptEngine.NAME, Collections.singletonMap("1", script -> "1"));
        ScriptEngineRegistry scriptEngineRegistry = new ScriptEngineRegistry(Collections.singletonList(scriptEngine));
        List<ScriptContext.Plugin> customContexts = Arrays.asList(
            new ScriptContext.Plugin(PLUGIN_NAME, "custom_op"),
            new ScriptContext.Plugin(PLUGIN_NAME, "custom_exp_disabled_op"),
            new ScriptContext.Plugin(PLUGIN_NAME, "custom_globally_disabled_op"));
        ScriptContextRegistry scriptContextRegistry = new ScriptContextRegistry(customContexts);
        ScriptSettings scriptSettings = new ScriptSettings(scriptEngineRegistry, scriptContextRegistry);

        return new ScriptService(settings, new Environment(settings), null, scriptEngineRegistry, scriptContextRegistry, scriptSettings) {
            @Override
            public StoredScriptSource getStoredScript(ClusterState state, GetStoredScriptRequest request) {
                return new StoredScriptSource(false, null, MockScriptEngine.NAME, "1", Collections.emptyMap());
            }
        };
    }

    public void testCustomGlobalScriptContextSettings() throws Exception {
        ScriptService scriptService = makeScriptService();
        for (ScriptType scriptType : Script.ScriptType.values()) {
            try {
                ScriptInput input;

                if (scriptType == ScriptType.FILE) {
                    input = ScriptInput.file("1");
                } else if (scriptType == ScriptType.STORED) {
                    input = ScriptInput.stored("1");
                } else {
                    input = ScriptInput.inline(MockScriptEngine.NAME, "1", Collections.emptyMap());
                }

                input.lookup.getCompiled(scriptService,
                    new ScriptContext.Plugin(PLUGIN_NAME, "custom_globally_disabled_op"), ExecutableScriptBinding.BINDING);
                fail("script compilation should have been rejected");
            } catch (IllegalStateException e) {
                assertThat(e.getMessage(), containsString("scripts of type [" + scriptType + "], operation [" + PLUGIN_NAME + "_custom_globally_disabled_op] and lang [" + MockScriptEngine.NAME + "] are disabled"));
            }
        }
    }

    public void testCustomScriptContextSettings() throws Exception {
        ScriptService scriptService = makeScriptService();
        ScriptInput script = ScriptInput.inline(MockScriptEngine.NAME, "1", Collections.emptyMap());
        try {
            script.lookup.getCompiled(scriptService, new ScriptContext.Plugin(PLUGIN_NAME, "custom_exp_disabled_op"), ExecutableScriptBinding.BINDING);
            fail("script compilation should have been rejected");
        } catch (IllegalStateException e) {
            assertTrue(e.getMessage(), e.getMessage().contains("scripts of type [inline], operation [" + PLUGIN_NAME + "_custom_exp_disabled_op] and lang [" + MockScriptEngine.NAME + "] are disabled"));
        }

        // still works for other script contexts
        assertNotNull(script.lookup.getCompiled(scriptService, ScriptContext.Standard.AGGS, ExecutableScriptBinding.BINDING));
        assertNotNull(script.lookup.getCompiled(scriptService, ScriptContext.Standard.SEARCH, ExecutableScriptBinding.BINDING));
        assertNotNull(script.lookup.getCompiled(scriptService, new ScriptContext.Plugin(PLUGIN_NAME, "custom_op"), ExecutableScriptBinding.BINDING));
    }

    public void testUnknownPluginScriptContext() throws Exception {
        ScriptService scriptService = makeScriptService();
        for (ScriptType scriptType : Script.ScriptType.values()) {
            try {
                ScriptInput input;

                if (scriptType == ScriptType.FILE) {
                    input = ScriptInput.file("1");
                } else if (scriptType == ScriptType.STORED) {
                    input = ScriptInput.stored("1");
                } else {
                    input = ScriptInput.inline(MockScriptEngine.NAME, "1", Collections.emptyMap());
                }

                input.lookup.getCompiled(scriptService, new ScriptContext.Plugin(PLUGIN_NAME, "unknown"), ExecutableScriptBinding.BINDING);
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
        for (ScriptType scriptType : Script.ScriptType.values()) {
            try {
                ScriptInput input;

                if (scriptType == ScriptType.FILE) {
                    input = ScriptInput.file("1");
                } else if (scriptType == ScriptType.STORED) {
                    input = ScriptInput.stored("1");
                } else {
                    input = ScriptInput.inline(MockScriptEngine.NAME, "1", Collections.emptyMap());
                }

                input.lookup.getCompiled(scriptService, context, ExecutableScriptBinding.BINDING);
                fail("script compilation should have been rejected");
            } catch (IllegalArgumentException e) {
                assertTrue(e.getMessage(), e.getMessage().contains("script context [test] not supported"));
            }
        }
    }

}
