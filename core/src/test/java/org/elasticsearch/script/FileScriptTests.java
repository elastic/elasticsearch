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
            .put("path.home", homeDir)
                // no file watching, so we don't need a ResourceWatcherService
            .put(ScriptService.SCRIPT_AUTO_RELOAD_ENABLED_SETTING, false)
            .put(settings)
            .build();
        Set<ScriptEngineService> engines = new HashSet<>(Collections.singletonList(new MockScriptEngine()));
        return new ScriptService(settings, new Environment(settings), engines, null, new ScriptContextRegistry(Collections.emptyList()));
    }

    public void testFileScriptFound() throws Exception {
        ContextAndHeaderHolder contextAndHeaders = new ContextAndHeaderHolder();
        Settings settings = Settings.builder()
            .put("script.engine." + MockScriptEngine.NAME + ".file.aggs", false).build();
        ScriptService scriptService = makeScriptService(settings);
        Script script = new Script("script1", ScriptService.ScriptType.FILE, MockScriptEngine.NAME, null);
        assertNotNull(scriptService.compile(script, ScriptContext.Standard.SEARCH, contextAndHeaders));
    }

    public void testAllOpsDisabled() throws Exception {
        ContextAndHeaderHolder contextAndHeaders = new ContextAndHeaderHolder();
        Settings settings = Settings.builder()
            .put("script.engine." + MockScriptEngine.NAME + ".file.aggs", false)
            .put("script.engine." + MockScriptEngine.NAME + ".file.search", false)
            .put("script.engine." + MockScriptEngine.NAME + ".file.mapping", false)
            .put("script.engine." + MockScriptEngine.NAME + ".file.update", false).build();
        ScriptService scriptService = makeScriptService(settings);
        Script script = new Script("script1", ScriptService.ScriptType.FILE, MockScriptEngine.NAME, null);
        for (ScriptContext context : ScriptContext.Standard.values()) {
            try {
                scriptService.compile(script, context, contextAndHeaders);
                fail(context.getKey() + " script should have been rejected");
            } catch(Exception e) {
                assertTrue(e.getMessage(), e.getMessage().contains("scripts of type [file], operation [" + context.getKey() + "] and lang [" + MockScriptEngine.NAME + "] are disabled"));
            }
        }
    }
}
