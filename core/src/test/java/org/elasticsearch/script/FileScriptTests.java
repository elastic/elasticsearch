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

import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.script.MockScriptEngine.MockCompiledScript;
import org.elasticsearch.test.ESTestCase;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;

// TODO: these really should just be part of ScriptService tests, there is nothing special about them
public class FileScriptTests extends ESTestCase {

    ScriptService makeScriptService(Settings settings) throws Exception {
        Path homeDir = createTempDir();
        Path scriptsDir = homeDir.resolve("config").resolve("scripts");
        Files.createDirectories(scriptsDir);
        Path mockscript = scriptsDir.resolve("script1.mockscript");
        String scriptSource = "1";
        Files.write(mockscript, scriptSource.getBytes("UTF-8"));
        settings = Settings.builder()
            .put(Environment.PATH_HOME_SETTING.getKey(), homeDir)
            .put(settings)
            .build();
        MockScriptEngine scriptEngine = new MockScriptEngine(MockScriptEngine.NAME, Collections.singletonMap(scriptSource, script -> "1"));
        ScriptEngineRegistry scriptEngineRegistry = new ScriptEngineRegistry(Collections.singleton(scriptEngine));
        ScriptContextRegistry scriptContextRegistry = new ScriptContextRegistry(Collections.emptyList());
        return new ScriptService(settings, new Environment(settings), null, scriptEngineRegistry, scriptContextRegistry);
    }

    public void testFileScriptFound() throws Exception {
        ScriptService scriptService = makeScriptService(Settings.EMPTY);
        Script script = new Script(ScriptType.FILE, MockScriptEngine.NAME, "script1", Collections.emptyMap());
        CompiledScript compiledScript = scriptService.compile(script, ScriptContext.Standard.SEARCH);
        assertNotNull(compiledScript);
        MockCompiledScript executable = (MockCompiledScript) compiledScript.compiled();
        assertEquals("script1.mockscript", executable.getName());
        assertWarnings("File scripts are deprecated. Use stored or inline scripts instead.");
    }
}
