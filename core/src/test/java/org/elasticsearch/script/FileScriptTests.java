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
import org.elasticsearch.script.MockScriptEngine.MockCompiledScript;
import org.elasticsearch.test.ESTestCase;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

// TODO: these really should just be part of ScriptService tests, there is nothing special about them
public class FileScriptTests extends ESTestCase {

    ScriptService makeScriptService(Settings settings) throws Exception {
        Path homeDir = createTempDir();
        Path scriptsDir = homeDir.resolve("config").resolve("scripts");
        Files.createDirectories(scriptsDir);
        Path mockscript = scriptsDir.resolve("script1.mockscript");
        Files.write(mockscript, "1".getBytes("UTF-8"));
        settings = Settings.builder()
            .put(Environment.PATH_HOME_SETTING.getKey(), homeDir)
                // no file watching, so we don't need a ResourceWatcherService
            .put(ScriptService.SCRIPT_AUTO_RELOAD_ENABLED_SETTING.getKey(), false)
            .put(settings)
            .build();
        ScriptEngineRegistry scriptEngineRegistry = new ScriptEngineRegistry(Collections.singleton(new MockScriptEngine()));
        ScriptContextRegistry scriptContextRegistry = new ScriptContextRegistry(Collections.emptyList());
        ScriptSettings scriptSettings = new ScriptSettings(scriptEngineRegistry, scriptContextRegistry);
        return new ScriptService(settings, new Environment(settings), null, scriptEngineRegistry, scriptContextRegistry, scriptSettings);
    }

    public void testFileScriptFound() throws Exception {
        Settings settings = Settings.builder()
            .put("script.engine." + MockScriptEngine.NAME + ".file.aggs", "false").build();
        ScriptService scriptService = makeScriptService(settings);
        Script script = new Script("script1", ScriptService.ScriptType.FILE, MockScriptEngine.NAME, null);
        CompiledScript compiledScript = scriptService.compile(script, ScriptContext.Standard.SEARCH, Collections.emptyMap(), null);
        assertNotNull(compiledScript);
        MockCompiledScript executable = (MockCompiledScript) compiledScript.compiled();
        assertEquals("script1.mockscript", executable.name);
    }

    public void testAllOpsDisabled() throws Exception {
        Settings settings = Settings.builder()
            .put("script.engine." + MockScriptEngine.NAME + ".file.aggs", "false")
            .put("script.engine." + MockScriptEngine.NAME + ".file.search", "false")
            .put("script.engine." + MockScriptEngine.NAME + ".file.mapping", "false")
            .put("script.engine." + MockScriptEngine.NAME + ".file.update", "false")
            .put("script.engine." + MockScriptEngine.NAME + ".file.ingest", "false").build();
        ScriptService scriptService = makeScriptService(settings);
        Script script = new Script("script1", ScriptService.ScriptType.FILE, MockScriptEngine.NAME, null);
        for (ScriptContext context : ScriptContext.Standard.values()) {
            try {
                scriptService.compile(script, context, Collections.emptyMap(), null);
                fail(context.getKey() + " script should have been rejected");
            } catch(Exception e) {
                assertTrue(e.getMessage(), e.getMessage().contains("scripts of type [file], operation [" + context.getKey() + "] and lang [" + MockScriptEngine.NAME + "] are disabled"));
            }
        }
    }
}
