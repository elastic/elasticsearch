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

import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.script.Script.ScriptInput;
import org.elasticsearch.script.Script.ScriptType;
import org.elasticsearch.script.Script.StoredScriptSource;
import org.elasticsearch.test.ESTestCase;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.Matchers.containsString;

public class ScriptContextTests extends ESTestCase {

    private static final String PLUGIN_NAME = "testplugin";

    ScriptService makeScriptService() throws Exception {
        Path genericConfigFolder = createTempDir();
        Path scriptsFilePath = genericConfigFolder.resolve("scripts");
        Files.createDirectories(scriptsFilePath);

        Path fileScript = scriptsFilePath.resolve("1." + MockScriptEngine.NAME);
        Streams.copy("1".getBytes("UTF-8"), Files.newOutputStream(fileScript));

        Settings settings = Settings.builder()
            .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir())
            .put(Environment.PATH_CONF_SETTING.getKey(), genericConfigFolder)
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
        ScriptService scriptService = new ScriptService(settings, new Environment(settings), null, scriptEngineRegistry, scriptContextRegistry, scriptSettings);

        ClusterState empty = ClusterState.builder(new ClusterName("_name")).build();
        ClusterState state = ScriptMetaData.storeScript(empty, "1", new StoredScriptSource(false, MockScriptEngine.NAME, "1", Collections.emptyMap()));
        scriptService.clusterChanged(new ClusterChangedEvent("test", state, empty));

        return scriptService;
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
                    new ScriptContext.Plugin(PLUGIN_NAME, "custom_globally_disabled_op"));
                fail("script compilation should have been rejected");
            } catch (IllegalStateException e) {
                assertThat(e.getMessage(), containsString("[" + scriptType + "] scripts using lang [" + MockScriptEngine.NAME + "] under context [" + PLUGIN_NAME + "_custom_globally_disabled_op] are disabled"));
            }
        }
    }

    public void testCustomScriptContextSettings() throws Exception {
        ScriptService scriptService = makeScriptService();
        ScriptInput script = ScriptInput.inline(MockScriptEngine.NAME, "1", Collections.emptyMap());
        try {
            script.lookup.getCompiled(scriptService, new ScriptContext.Plugin(PLUGIN_NAME, "custom_exp_disabled_op"));
            fail("script compilation should have been rejected");
        } catch (IllegalStateException e) {
            assertTrue(e.getMessage(), e.getMessage().contains("[inline] scripts using lang [" + MockScriptEngine.NAME + "] under context [" + PLUGIN_NAME + "_custom_exp_disabled_op] are disabled"));
        }

        // still works for other script contexts
        assertNotNull(script.lookup.getCompiled(scriptService, ScriptContext.Standard.AGGS));
        assertNotNull(script.lookup.getCompiled(scriptService, ScriptContext.Standard.SEARCH));
        assertNotNull(script.lookup.getCompiled(scriptService, new ScriptContext.Plugin(PLUGIN_NAME, "custom_op")));
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

                input.lookup.getCompiled(scriptService, new ScriptContext.Plugin(PLUGIN_NAME, "unknown"));
                fail("script compilation should have been rejected");
            } catch (IllegalArgumentException e) {
                assertTrue(e.getMessage(), e.getMessage().contains("script context [" + PLUGIN_NAME + "_unknown] does not exist"));
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

                input.lookup.getCompiled(scriptService, context);
                fail("script compilation should have been rejected");
            } catch (IllegalArgumentException e) {
                assertTrue(e.getMessage(), e.getMessage().contains("script context [test] does not exist"));
            }
        }
    }

}
