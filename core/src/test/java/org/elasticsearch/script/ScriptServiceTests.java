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
import org.elasticsearch.action.admin.cluster.storedscripts.DeleteStoredScriptRequest;
import org.elasticsearch.action.admin.cluster.storedscripts.GetStoredScriptRequest;
import org.elasticsearch.action.admin.cluster.storedscripts.PutStoredScriptRequest;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.env.Environment;
import org.elasticsearch.script.ScriptService.ScriptType;
import org.elasticsearch.search.lookup.SearchLookup;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.watcher.ResourceWatcherService;
import org.junit.Before;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.sameInstance;

//TODO: this needs to be a base test class, and all scripting engines extend it
public class ScriptServiceTests extends ESTestCase {

    private ResourceWatcherService resourceWatcherService;
    private ScriptEngineService scriptEngineService;
    private ScriptEngineService dangerousScriptEngineService;
    private Map<String, ScriptEngineService> scriptEnginesByLangMap;
    private ScriptEngineRegistry scriptEngineRegistry;
    private ScriptContextRegistry scriptContextRegistry;
    private ScriptSettings scriptSettings;
    private ScriptContext[] scriptContexts;
    private ScriptService scriptService;
    private Path scriptsFilePath;
    private Settings baseSettings;

    private static final Map<ScriptType, Boolean> DEFAULT_SCRIPT_ENABLED = new HashMap<>();

    static {
        DEFAULT_SCRIPT_ENABLED.put(ScriptType.FILE, true);
        DEFAULT_SCRIPT_ENABLED.put(ScriptType.STORED, false);
        DEFAULT_SCRIPT_ENABLED.put(ScriptType.INLINE, false);
    }

    @Before
    public void setup() throws IOException {
        Path genericConfigFolder = createTempDir();
        baseSettings = Settings.builder()
                .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString())
                .put(Environment.PATH_CONF_SETTING.getKey(), genericConfigFolder)
                .build();
        resourceWatcherService = new ResourceWatcherService(baseSettings, null);
        scriptEngineService = new TestEngineService();
        dangerousScriptEngineService = new TestDangerousEngineService();
        scriptEnginesByLangMap = ScriptModesTests.buildScriptEnginesByLangMap(Collections.singleton(scriptEngineService));
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
        scriptEngineRegistry = new ScriptEngineRegistry(Arrays.asList(scriptEngineService, dangerousScriptEngineService));
        scriptContextRegistry = new ScriptContextRegistry(contexts.values());
        scriptSettings = new ScriptSettings(scriptEngineRegistry, scriptContextRegistry);
        scriptContexts = scriptContextRegistry.scriptContexts().toArray(new ScriptContext[scriptContextRegistry.scriptContexts().size()]);
        logger.info("--> setup script service");
        scriptsFilePath = genericConfigFolder.resolve("scripts");
        Files.createDirectories(scriptsFilePath);
    }

    private void buildScriptService(Settings additionalSettings) throws IOException {
        Settings finalSettings = Settings.builder().put(baseSettings).put(additionalSettings).build();
        Environment environment = new Environment(finalSettings);
        scriptService = new ScriptService(finalSettings, environment, resourceWatcherService, scriptEngineRegistry, scriptContextRegistry, scriptSettings) {
            @Override
            String getScriptFromClusterState(ClusterState state, String scriptLang, String id) {
                //mock the script that gets retrieved from an index
                return "100";
            }
        };
    }

    public void testNotSupportedDisableDynamicSetting() throws IOException {
        try {
            buildScriptService(Settings.builder().put(ScriptService.DISABLE_DYNAMIC_SCRIPTING_SETTING, randomUnicodeOfLength(randomIntBetween(1, 10))).build());
            fail("script service should have thrown exception due to non supported script.disable_dynamic setting");
        } catch(IllegalArgumentException e) {
            assertThat(e.getMessage(), containsString(ScriptService.DISABLE_DYNAMIC_SCRIPTING_SETTING + " is not a supported setting, replace with fine-grained script settings"));
        }
    }

    public void testScriptsWithoutExtensions() throws IOException {
        buildScriptService(Settings.EMPTY);
        Path testFileNoExt = scriptsFilePath.resolve("test_no_ext");
        Path testFileWithExt = scriptsFilePath.resolve("test_script.test");
        Streams.copy("test_file_no_ext".getBytes("UTF-8"), Files.newOutputStream(testFileNoExt));
        Streams.copy("test_file".getBytes("UTF-8"), Files.newOutputStream(testFileWithExt));
        resourceWatcherService.notifyNow();

        CompiledScript compiledScript = scriptService.compile(new Script("test_script", ScriptType.FILE, "test", null),
                ScriptContext.Standard.SEARCH, Collections.emptyMap(), emptyClusterState());
        assertThat(compiledScript.compiled(), equalTo((Object) "compiled_test_file"));

        Files.delete(testFileNoExt);
        Files.delete(testFileWithExt);
        resourceWatcherService.notifyNow();

        try {
            scriptService.compile(new Script("test_script", ScriptType.FILE, "test", null), ScriptContext.Standard.SEARCH,
                    Collections.emptyMap(), emptyClusterState());
            fail("the script test_script should no longer exist");
        } catch (IllegalArgumentException ex) {
            assertThat(ex.getMessage(), containsString("Unable to find on disk file script [test_script] using lang [test]"));
        }
    }

    public void testScriptCompiledOnceHiddenFileDetected() throws IOException {
        buildScriptService(Settings.EMPTY);

        Path testHiddenFile = scriptsFilePath.resolve(".hidden_file");
        Streams.copy("test_hidden_file".getBytes("UTF-8"), Files.newOutputStream(testHiddenFile));

        Path testFileScript = scriptsFilePath.resolve("file_script.test");
        Streams.copy("test_file_script".getBytes("UTF-8"), Files.newOutputStream(testFileScript));
        resourceWatcherService.notifyNow();

        CompiledScript compiledScript = scriptService.compile(new Script("file_script", ScriptType.FILE, "test", null),
                ScriptContext.Standard.SEARCH, Collections.emptyMap(), emptyClusterState());
        assertThat(compiledScript.compiled(), equalTo((Object) "compiled_test_file_script"));

        Files.delete(testHiddenFile);
        Files.delete(testFileScript);
        resourceWatcherService.notifyNow();
    }

    public void testInlineScriptCompiledOnceCache() throws IOException {
        buildScriptService(Settings.EMPTY);
        CompiledScript compiledScript1 = scriptService.compile(new Script("1+1", ScriptType.INLINE, "test", null),
                randomFrom(scriptContexts), Collections.emptyMap(), emptyClusterState());
        CompiledScript compiledScript2 = scriptService.compile(new Script("1+1", ScriptType.INLINE, "test", null),
                randomFrom(scriptContexts), Collections.emptyMap(), emptyClusterState());
        assertThat(compiledScript1.compiled(), sameInstance(compiledScript2.compiled()));
    }

    public void testDefaultBehaviourFineGrainedSettings() throws IOException {
        Settings.Builder builder = Settings.builder();
        //rarely inject the default settings, which have no effect
        if (rarely()) {
            builder.put("script.file", "true");
        }
        buildScriptService(builder.build());
        createFileScripts("groovy", "mustache", "dtest");

        for (ScriptContext scriptContext : scriptContexts) {
            // only file scripts are accepted by default
            assertCompileRejected("dtest", "script", ScriptType.INLINE, scriptContext);
            assertCompileRejected("dtest", "script", ScriptType.STORED, scriptContext);
            assertCompileAccepted("dtest", "file_script", ScriptType.FILE, scriptContext);
        }
    }

    public void testFineGrainedSettings() throws IOException {
        //collect the fine-grained settings to set for this run
        int numScriptSettings = randomIntBetween(0, ScriptType.values().length);
        Map<ScriptType, Boolean> scriptSourceSettings = new HashMap<>();
        for (int i = 0; i < numScriptSettings; i++) {
            ScriptType scriptType;
            do {
                scriptType = randomFrom(ScriptType.values());
            } while (scriptSourceSettings.containsKey(scriptType));
            scriptSourceSettings.put(scriptType, randomBoolean());
        }
        int numScriptContextSettings = randomIntBetween(0, this.scriptContextRegistry.scriptContexts().size());
        Map<ScriptContext, Boolean> scriptContextSettings = new HashMap<>();
        for (int i = 0; i < numScriptContextSettings; i++) {
            ScriptContext scriptContext;
            do {
                scriptContext = randomFrom(this.scriptContexts);
            } while (scriptContextSettings.containsKey(scriptContext));
            scriptContextSettings.put(scriptContext, randomBoolean());
        }
        int numEngineSettings = randomIntBetween(0, ScriptType.values().length * scriptContexts.length);
        Map<String, Boolean> engineSettings = new HashMap<>();
        for (int i = 0; i < numEngineSettings; i++) {
            String settingKey;
            do {
                ScriptType scriptType = randomFrom(ScriptType.values());
                ScriptContext scriptContext = randomFrom(this.scriptContexts);
                settingKey = scriptEngineService.getType() + "." + scriptType + "." + scriptContext.getKey();
            } while (engineSettings.containsKey(settingKey));
            engineSettings.put(settingKey, randomBoolean());
        }
        //set the selected fine-grained settings
        Settings.Builder builder = Settings.builder();
        for (Map.Entry<ScriptType, Boolean> entry : scriptSourceSettings.entrySet()) {
            if (entry.getValue()) {
                builder.put("script" + "." + entry.getKey().getScriptType(), "true");
            } else {
                builder.put("script" + "." + entry.getKey().getScriptType(), "false");
            }
        }
        for (Map.Entry<ScriptContext, Boolean> entry : scriptContextSettings.entrySet()) {
            if (entry.getValue()) {
                builder.put("script" + "." + entry.getKey().getKey(), "true");
            } else {
                builder.put("script" + "." + entry.getKey().getKey(), "false");
            }
        }
        for (Map.Entry<String, Boolean> entry : engineSettings.entrySet()) {
            int delimiter = entry.getKey().indexOf('.');
            String part1 = entry.getKey().substring(0, delimiter);
            String part2 = entry.getKey().substring(delimiter + 1);

            String lang = randomFrom(scriptEnginesByLangMap.get(part1).getType());
            if (entry.getValue()) {
                builder.put("script.engine" + "." + lang + "." + part2, "true");
            } else {
                builder.put("script.engine" + "." + lang + "." + part2, "false");
            }
        }

        buildScriptService(builder.build());
        createFileScripts("groovy", "expression", "mustache", "dtest");

        for (ScriptType scriptType : ScriptType.values()) {
            //make sure file scripts have a different name than inline ones.
            //Otherwise they are always considered file ones as they can be found in the static cache.
            String script = scriptType == ScriptType.FILE ? "file_script" : "script";
            for (ScriptContext scriptContext : this.scriptContexts) {
                //fallback mechanism: 1) engine specific settings 2) op based settings 3) source based settings
                Boolean scriptEnabled = engineSettings.get(dangerousScriptEngineService.getType() + "." + scriptType + "." + scriptContext.getKey());
                if (scriptEnabled == null) {
                    scriptEnabled = scriptContextSettings.get(scriptContext);
                }
                if (scriptEnabled == null) {
                    scriptEnabled = scriptSourceSettings.get(scriptType);
                }
                if (scriptEnabled == null) {
                    scriptEnabled = DEFAULT_SCRIPT_ENABLED.get(scriptType);
                }

                String lang = dangerousScriptEngineService.getType();
                if (scriptEnabled) {
                    assertCompileAccepted(lang, script, scriptType, scriptContext);
                } else {
                    assertCompileRejected(lang, script, scriptType, scriptContext);
                }
            }
        }
    }

    public void testCompileNonRegisteredContext() throws IOException {
        buildScriptService(Settings.EMPTY);
        String pluginName;
        String unknownContext;
        do {
            pluginName = randomAsciiOfLength(randomIntBetween(1, 10));
            unknownContext = randomAsciiOfLength(randomIntBetween(1, 30));
        } while(scriptContextRegistry.isSupportedContext(new ScriptContext.Plugin(pluginName, unknownContext)));

        String type = scriptEngineService.getType();
        try {
            scriptService.compile(new Script("test", randomFrom(ScriptType.values()), type, null), new ScriptContext.Plugin(
                            pluginName, unknownContext), Collections.emptyMap(), emptyClusterState());
            fail("script compilation should have been rejected");
        } catch(IllegalArgumentException e) {
            assertThat(e.getMessage(), containsString("script context [" + pluginName + "_" + unknownContext + "] not supported"));
        }
    }

    public void testCompileCountedInCompilationStats() throws IOException {
        buildScriptService(Settings.EMPTY);
        scriptService.compile(new Script("1+1", ScriptType.INLINE, "test", null), randomFrom(scriptContexts),
                Collections.emptyMap(), emptyClusterState());
        assertEquals(1L, scriptService.stats().getCompilations());
    }

    public void testExecutableCountedInCompilationStats() throws IOException {
        buildScriptService(Settings.EMPTY);
        ClusterState state = ClusterState.builder(new ClusterName("_name")).build();
        scriptService.executable(new Script("1+1", ScriptType.INLINE, "test", null), randomFrom(scriptContexts), Collections.emptyMap(), state);
        assertEquals(1L, scriptService.stats().getCompilations());
    }

    public void testSearchCountedInCompilationStats() throws IOException {
        buildScriptService(Settings.EMPTY);
        ClusterState state = ClusterState.builder(new ClusterName("_name")).build();
        scriptService.search(null, new Script("1+1", ScriptType.INLINE, "test", null), randomFrom(scriptContexts),
                Collections.emptyMap(), state);
        assertEquals(1L, scriptService.stats().getCompilations());
    }

    public void testMultipleCompilationsCountedInCompilationStats() throws IOException {
        buildScriptService(Settings.EMPTY);
        int numberOfCompilations = randomIntBetween(1, 1024);
        for (int i = 0; i < numberOfCompilations; i++) {
            scriptService
                    .compile(new Script(i + " + " + i, ScriptType.INLINE, "test", null), randomFrom(scriptContexts),
                            Collections.emptyMap(), emptyClusterState());
        }
        assertEquals(numberOfCompilations, scriptService.stats().getCompilations());
    }

    public void testCompilationStatsOnCacheHit() throws IOException {
        Settings.Builder builder = Settings.builder();
        builder.put(ScriptService.SCRIPT_CACHE_SIZE_SETTING.getKey(), 1);
        builder.put("script.inline", "true");
        buildScriptService(builder.build());
        ClusterState state = ClusterState.builder(new ClusterName("_name")).build();
        scriptService.executable(new Script("1+1", ScriptType.INLINE, "test", null), randomFrom(scriptContexts), Collections.emptyMap(), state);
        scriptService.executable(new Script("1+1", ScriptType.INLINE, "test", null), randomFrom(scriptContexts), Collections.emptyMap(), state);
        assertEquals(1L, scriptService.stats().getCompilations());
    }

    public void testFileScriptCountedInCompilationStats() throws IOException {
        buildScriptService(Settings.EMPTY);
        createFileScripts("test");
        scriptService.compile(new Script("file_script", ScriptType.FILE, "test", null), randomFrom(scriptContexts),
                Collections.emptyMap(), emptyClusterState());
        assertEquals(1L, scriptService.stats().getCompilations());
    }

    public void testIndexedScriptCountedInCompilationStats() throws IOException {
        buildScriptService(Settings.EMPTY);
        scriptService.compile(new Script("script", ScriptType.STORED, "test", null), randomFrom(scriptContexts),
                Collections.emptyMap(), emptyClusterState());
        assertEquals(1L, scriptService.stats().getCompilations());
    }

    public void testCacheEvictionCountedInCacheEvictionsStats() throws IOException {
        Settings.Builder builder = Settings.builder();
        builder.put(ScriptService.SCRIPT_CACHE_SIZE_SETTING.getKey(), 1);
        builder.put("script.inline", "true");
        buildScriptService(builder.build());
        ClusterState state = ClusterState.builder(new ClusterName("_name")).build();
        scriptService.executable(new Script("1+1", ScriptType.INLINE, "test", null), randomFrom(scriptContexts), Collections.emptyMap(), state);
        scriptService.executable(new Script("2+2", ScriptType.INLINE, "test", null), randomFrom(scriptContexts), Collections.emptyMap(), state);
        assertEquals(2L, scriptService.stats().getCompilations());
        assertEquals(1L, scriptService.stats().getCacheEvictions());
    }

    public void testDefaultLanguage() throws IOException {
        Settings.Builder builder = Settings.builder();
        builder.put("script.default_lang", "test");
        builder.put("script.inline", "true");
        buildScriptService(builder.build());
        CompiledScript script = scriptService.compile(new Script("1 + 1", ScriptType.INLINE, null, null),
                randomFrom(scriptContexts), Collections.emptyMap(), emptyClusterState());
        assertEquals(script.lang(), "test");
    }

    public void testStoreScript() throws Exception {
        BytesReference script = XContentFactory.jsonBuilder().startObject()
                    .field("script", "abc")
                .endObject().bytes();

        ClusterState empty = ClusterState.builder(new ClusterName("_name")).build();
        PutStoredScriptRequest request = new PutStoredScriptRequest("_lang", "_id")
                .script(script);
        ClusterState result = ScriptService.innerStoreScript(empty, "_lang", request);
        ScriptMetaData scriptMetaData = result.getMetaData().custom(ScriptMetaData.TYPE);
        assertNotNull(scriptMetaData);
        assertEquals("abc", scriptMetaData.getScript("_lang", "_id"));
        assertEquals(script, scriptMetaData.getScriptAsBytes("_lang", "_id"));
    }

    public void testDeleteScript() throws Exception {
        ClusterState cs = ClusterState.builder(new ClusterName("_name"))
                .metaData(MetaData.builder()
                        .putCustom(ScriptMetaData.TYPE,
                                new ScriptMetaData.Builder(null).storeScript("_lang", "_id", new BytesArray("abc")).build()))
                .build();

        DeleteStoredScriptRequest request = new DeleteStoredScriptRequest("_lang", "_id");
        ClusterState result = ScriptService.innerDeleteScript(cs, "_lang", request);
        ScriptMetaData scriptMetaData = result.getMetaData().custom(ScriptMetaData.TYPE);
        assertNotNull(scriptMetaData);
        assertNull(scriptMetaData.getScript("_lang", "_id"));
        assertNull(scriptMetaData.getScriptAsBytes("_lang", "_id"));

        ResourceNotFoundException e = expectThrows(ResourceNotFoundException.class, () -> {
            ScriptService.innerDeleteScript(cs, "_lang", new DeleteStoredScriptRequest("_lang", "_non_existing_id"));
        });
        assertEquals("Stored script with id [_non_existing_id] for language [_lang] does not exist", e.getMessage());
    }

    public void testGetStoredScript() throws Exception {
        buildScriptService(Settings.EMPTY);
        ClusterState cs = ClusterState.builder(new ClusterName("_name"))
                .metaData(MetaData.builder()
                        .putCustom(ScriptMetaData.TYPE,
                                new ScriptMetaData.Builder(null).storeScript("_lang", "_id",
                                        new BytesArray("{\"script\":\"abc\"}")).build()))
                .build();

        assertEquals("abc", scriptService.getStoredScript(cs, new GetStoredScriptRequest("_lang", "_id")));
        assertNull(scriptService.getStoredScript(cs, new GetStoredScriptRequest("_lang", "_id2")));

        cs = ClusterState.builder(new ClusterName("_name")).build();
        assertNull(scriptService.getStoredScript(cs, new GetStoredScriptRequest("_lang", "_id")));
    }

    public void testValidateScriptSize() throws Exception {
        int maxSize = 0xFFFF;
        buildScriptService(Settings.EMPTY);
        // allowed
        scriptService.validate("_id", "test", new BytesArray("{\"script\":\"" + randomAsciiOfLength(maxSize - 13) + "\"}"));

        // disallowed
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
                () -> {
                    scriptService.validate("_id", "test", new BytesArray("{\"script\":\"" + randomAsciiOfLength(maxSize - 12) + "\"}"));
                });
        assertThat(e.getMessage(), equalTo(
                "Limit of script size in bytes [" + maxSize+ "] has been exceeded for script [_id] with size [" + (maxSize + 1) + "]"));
    }


    private void createFileScripts(String... langs) throws IOException {
        for (String lang : langs) {
            Path scriptPath = scriptsFilePath.resolve("file_script." + lang);
            Streams.copy("10".getBytes("UTF-8"), Files.newOutputStream(scriptPath));
        }
        resourceWatcherService.notifyNow();
    }

    private void assertCompileRejected(String lang, String script, ScriptType scriptType, ScriptContext scriptContext) {
        try {
            scriptService.compile(new Script(script, scriptType, lang, null), scriptContext, Collections.emptyMap(), emptyClusterState());
            fail("compile should have been rejected for lang [" + lang + "], script_type [" + scriptType + "], scripted_op [" + scriptContext + "]");
        } catch(IllegalStateException e) {
            //all good
        }
    }

    private void assertCompileAccepted(String lang, String script, ScriptType scriptType, ScriptContext scriptContext) {
        ClusterState state = emptyClusterState();
        assertThat(
                scriptService.compile(new Script(script, scriptType, lang, null), scriptContext, Collections.emptyMap(), state),
                notNullValue()
        );
    }

    public static class TestEngineService implements ScriptEngineService {

        public static final String NAME = "test";

        @Override
        public String getType() {
            return NAME;
        }

        @Override
        public String getExtension() {
            return NAME;
        }

        @Override
        public Object compile(String scriptName, String scriptText, Map<String, String> params) {
            return "compiled_" + scriptText;
        }

        @Override
        public ExecutableScript executable(final CompiledScript compiledScript, @Nullable Map<String, Object> vars) {
            return null;
        }

        @Override
        public SearchScript search(CompiledScript compiledScript, SearchLookup lookup, @Nullable Map<String, Object> vars) {
            return null;
        }

        @Override
        public void close() {

        }

        @Override
        public void scriptRemoved(CompiledScript script) {
            // Nothing to do here
        }

        @Override
        public boolean isInlineScriptEnabled() {
            return true;
        }
    }

    public static class TestDangerousEngineService implements ScriptEngineService {

        public static final String NAME = "dtest";

        public static final List<String> EXTENSIONS = Collections.unmodifiableList(Arrays.asList("dtest"));

        @Override
        public String getType() {
            return NAME;
        }

        @Override
        public String getExtension() {
            return NAME;
        }

        @Override
        public Object compile(String scriptName, String scriptSource, Map<String, String> params) {
            return "compiled_" + scriptSource;
        }

        @Override
        public ExecutableScript executable(final CompiledScript compiledScript, @Nullable Map<String, Object> vars) {
            return null;
        }

        @Override
        public SearchScript search(CompiledScript compiledScript, SearchLookup lookup, @Nullable Map<String, Object> vars) {
            return null;
        }

        @Override
        public void close() {

        }

        @Override
        public void scriptRemoved(CompiledScript script) {
            // Nothing to do here
        }
    }

    private static ClusterState emptyClusterState() {
        return ClusterState.builder(new ClusterName("_name")).build();
    }

}
