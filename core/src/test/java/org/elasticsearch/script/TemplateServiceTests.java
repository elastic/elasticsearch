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

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.search.lookup.SearchLookup;
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

public class TemplateServiceTests extends ESTestCase {
    private ResourceWatcherService resourceWatcherService;
    private ScriptContextRegistry scriptContextRegistry;
    private ScriptSettings scriptSettings;
    private Path fileTemplatesPath;
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
        backend = new DummyBackend();
        scriptSettings = new ScriptSettings(new ScriptEngineRegistry(emptyList()), backend,
                scriptContextRegistry);
        logger.info("--> setup script service");
        fileTemplatesPath = genericConfigFolder.resolve("scripts");
        Files.createDirectories(fileTemplatesPath);
        Environment environment = new Environment(settings);
        templateService = new TemplateService(settings, environment, resourceWatcherService,
                backend, scriptContextRegistry, scriptSettings, new ScriptMetrics());
    }

    public void testFileTemplates() throws IOException {
        String body = "{\"test\":\"test\"}";
        Path testFileWithExt = fileTemplatesPath.resolve("test.test_template_backend");
        Streams.copy(body.getBytes("UTF-8"), Files.newOutputStream(testFileWithExt));
        resourceWatcherService.notifyNow();

        assertEquals(new BytesArray(body), templateService.template("test", ScriptType.FILE,
                ScriptContext.Standard.SEARCH, null).apply(emptyMap()));

        Files.delete(testFileWithExt);
        resourceWatcherService.notifyNow();

        Exception e = expectThrows(IllegalArgumentException.class, () -> templateService
                .template("test", ScriptType.FILE, ScriptContext.Standard.SEARCH, null));
        assertEquals("unable to find file template [id=test, contentType=text/plain]",
                e.getMessage());
    }

    public void testStoredTemplates() throws IOException {
        String body = "{\"test\":\"test\"}";

        ClusterState newState = ClusterState.builder(new ClusterName("test"))
                .metaData(MetaData.builder()
                    .putCustom(ScriptMetaData.TYPE, new ScriptMetaData.Builder(null)
                            .storeScript("test_template_backend#test", new StoredScriptSource(
                                    "test_template_backend", body, emptyMap()))
                            .build())
                    .build())
                .build();
        templateService.clusterChanged(
                new ClusterChangedEvent("test", newState, ClusterState.EMPTY_STATE));

        assertEquals(new BytesArray(body), templateService
                .template("test", ScriptType.STORED, ScriptContext.Standard.SEARCH, null)
                .apply(emptyMap()));

        ClusterState oldState = newState;
        newState = ClusterState.builder(oldState)
                .metaData(MetaData.builder(oldState.metaData())
                    .putCustom(ScriptMetaData.TYPE, new ScriptMetaData.Builder(null).build())
                    .build())
                .build();
        templateService.clusterChanged(new ClusterChangedEvent("test", newState, oldState));

        Exception e = expectThrows(ResourceNotFoundException.class, () -> templateService
                .template("test", ScriptType.STORED, ScriptContext.Standard.SEARCH, null));
        assertEquals("unable to find template [id=test, contentType=text/plain] in cluster state",
                e.getMessage());
    }

    public void testInlineTemplates() throws IOException {
        String body = "{\"test\":\"test\"}";

        assertEquals(new BytesArray(body), templateService
                .template(body, ScriptType.INLINE, ScriptContext.Standard.SEARCH, null)
                .apply(emptyMap()));
    }

    /**
     * Dummy backend that just returns the script's source when run.
     */
    private static class DummyBackend implements TemplateService.Backend {
        @Override
        public String getType() {
            return "test_template_backend";
        }

        @Override
        public String getExtension() {
            return "test_template_backend";
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
