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
import org.elasticsearch.watcher.ResourceWatcherService;
import org.junit.Before;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TemplateServiceTests extends ESTestCase {
    private ResourceWatcherService resourceWatcherService;
    private ScriptContextRegistry scriptContextRegistry;
    private ScriptSettings scriptSettings;
    private ScriptContext[] scriptContexts;
    private Path scriptsFilePath;
    private Settings settings;
    private TemplateService.Backend backend;
    private TemplateService templateService;

    private static final Map<ScriptType, Boolean> DEFAULT_SCRIPT_ENABLED = new HashMap<>();
    static {
        DEFAULT_SCRIPT_ENABLED.put(ScriptType.FILE, true);
        DEFAULT_SCRIPT_ENABLED.put(ScriptType.STORED, false);
        DEFAULT_SCRIPT_ENABLED.put(ScriptType.INLINE, false);
    }

    @Before
    public void setup() throws IOException {
        Path genericConfigFolder = createTempDir();
        settings = Settings.builder()
                .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString())
                .put(Environment.PATH_CONF_SETTING.getKey(), genericConfigFolder)
                .put(ScriptService.SCRIPT_MAX_COMPILATIONS_PER_MINUTE.getKey(), 10000)
                .build();
        resourceWatcherService = new ResourceWatcherService(settings, null);
        //randomly register custom script contexts
        int randomInt = randomIntBetween(0, 3);
        //prevent duplicates using map
        Map<String, ScriptContext.Plugin> contexts = new HashMap<>();
        for (int i = 0; i < randomInt; i++) {
            String plugin;
            do {
                plugin = randomAsciiOfLength(randomIntBetween(1, 10));
            } while (ScriptContextRegistry.RESERVED_SCRIPT_CONTEXTS.contains(plugin));
            String operation;
            do {
                operation = randomAsciiOfLength(randomIntBetween(1, 30));
            } while (ScriptContextRegistry.RESERVED_SCRIPT_CONTEXTS.contains(operation));
            String context = plugin + "_" + operation;
            contexts.put(context, new ScriptContext.Plugin(plugin, operation));
        }
        scriptContextRegistry = new ScriptContextRegistry(contexts.values());
        backend = mock(TemplateService.Backend.class);
        when(backend.getType()).thenReturn("test_template_backend");
        scriptSettings = new ScriptSettings(new ScriptEngineRegistry(emptyList()), backend, scriptContextRegistry);
        scriptContexts = scriptContextRegistry.scriptContexts().toArray(new ScriptContext[scriptContextRegistry.scriptContexts().size()]);
        logger.info("--> setup script service");
        scriptsFilePath = genericConfigFolder.resolve("scripts");
        Files.createDirectories(scriptsFilePath);
        Environment environment = new Environment(settings);
        templateService = new TemplateService(settings, environment, resourceWatcherService, backend, scriptContextRegistry,
                scriptSettings, new ScriptMetrics());
    }

    public void testFoo() {
        // NOCOMMIT something here
    }
}
