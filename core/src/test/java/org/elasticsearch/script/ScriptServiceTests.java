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
import org.elasticsearch.action.admin.cluster.storedscripts.GetStoredScriptRequest;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.env.Environment;
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
import java.util.HashSet;
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
                .put(ScriptService.SCRIPT_MAX_COMPILATIONS_PER_MINUTE.getKey(), 10000)
                .build();
        resourceWatcherService = new ResourceWatcherService(baseSettings, null);
        scriptEngineService = new TestEngineService();
        dangerousScriptEngineService = new TestDangerousEngineService();
        TestEngineService defaultScriptServiceEngine = new TestEngineService(Script.DEFAULT_SCRIPT_LANG) {};
        scriptEnginesByLangMap = ScriptModesTests.buildScriptEnginesByLangMap(
                new HashSet<>(Arrays.asList(scriptEngineService, defaultScriptServiceEngine)));
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
        scriptEngineRegistry = new ScriptEngineRegistry(Arrays.asList(scriptEngineService, dangerousScriptEngineService,
                defaultScriptServiceEngine));
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
        // TODO:
        scriptService = new ScriptService(finalSettings, environment, resourceWatcherService, scriptEngineRegistry, scriptContextRegistry, scriptSettings) {
            @Override
            StoredScriptSource getScriptFromClusterState(String id, String lang) {
                //mock the script that gets retrieved from an index
                return new StoredScriptSource(lang, "100", Collections.emptyMap());
            }
        };
    }

    public void testCompilationCircuitBreaking() throws Exception {
        buildScriptService(Settings.EMPTY);
        scriptService.setMaxCompilationsPerMinute(1);
        scriptService.checkCompilationLimit(); // should pass
        expectThrows(CircuitBreakingException.class, () -> scriptService.checkCompilationLimit());
        scriptService.setMaxCompilationsPerMinute(2);
        scriptService.checkCompilationLimit(); // should pass
        scriptService.checkCompilationLimit(); // should pass
        expectThrows(CircuitBreakingException.class, () -> scriptService.checkCompilationLimit());
        int count = randomIntBetween(5, 50);
        scriptService.setMaxCompilationsPerMinute(count);
        for (int i = 0; i < count; i++) {
            scriptService.checkCompilationLimit(); // should pass
        }
        expectThrows(CircuitBreakingException.class, () -> scriptService.checkCompilationLimit());
        scriptService.setMaxCompilationsPerMinute(0);
        expectThrows(CircuitBreakingException.class, () -> scriptService.checkCompilationLimit());
        scriptService.setMaxCompilationsPerMinute(Integer.MAX_VALUE);
        int largeLimit = randomIntBetween(1000, 10000);
        for (int i = 0; i < largeLimit; i++) {
            scriptService.checkCompilationLimit();
        }
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

        CompiledScript compiledScript = scriptService.compile(new Script(ScriptType.FILE, "test", "test_script", Collections.emptyMap()),
                ScriptContext.Standard.SEARCH);
        assertThat(compiledScript.compiled(), equalTo((Object) "compiled_test_file"));

        Files.delete(testFileNoExt);
        Files.delete(testFileWithExt);
        resourceWatcherService.notifyNow();

        try {
            scriptService.compile(new Script(ScriptType.FILE, "test", "test_script", Collections.emptyMap()), ScriptContext.Standard.SEARCH);
            fail("the script test_script should no longer exist");
        } catch (IllegalArgumentException ex) {
            assertThat(ex.getMessage(), containsString("unable to find file script [test_script] using lang [test]"));
        }
    }

    public void testScriptCompiledOnceHiddenFileDetected() throws IOException {
        buildScriptService(Settings.EMPTY);

        Path testHiddenFile = scriptsFilePath.resolve(".hidden_file");
        Streams.copy("test_hidden_file".getBytes("UTF-8"), Files.newOutputStream(testHiddenFile));

        Path testFileScript = scriptsFilePath.resolve("file_script.test");
        Streams.copy("test_file_script".getBytes("UTF-8"), Files.newOutputStream(testFileScript));
        resourceWatcherService.notifyNow();

        CompiledScript compiledScript = scriptService.compile(new Script(ScriptType.FILE, "test", "file_script", Collections.emptyMap()),
                ScriptContext.Standard.SEARCH);
        assertThat(compiledScript.compiled(), equalTo((Object) "compiled_test_file_script"));

        Files.delete(testHiddenFile);
        Files.delete(testFileScript);
        resourceWatcherService.notifyNow();
    }

    public void testInlineScriptCompiledOnceCache() throws IOException {
        buildScriptService(Settings.EMPTY);
        CompiledScript compiledScript1 = scriptService.compile(new Script(ScriptType.INLINE, "test", "1+1", Collections.emptyMap()),
                randomFrom(scriptContexts));
        CompiledScript compiledScript2 = scriptService.compile(new Script(ScriptType.INLINE, "test", "1+1", Collections.emptyMap()),
                randomFrom(scriptContexts));
        assertThat(compiledScript1.compiled(), sameInstance(compiledScript2.compiled()));
    }

    public void testDefaultBehaviourFineGrainedSettings() throws IOException {
        Settings.Builder builder = Settings.builder();
        //rarely inject the default settings, which have no effect
        if (rarely()) {
            builder.put("script.file", "true");
        }
        buildScriptService(builder.build());
        createFileScripts("mustache", "dtest");

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
                builder.put("script" + "." + entry.getKey().getName(), "true");
            } else {
                builder.put("script" + "." + entry.getKey().getName(), "false");
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
        createFileScripts("expression", "mustache", "dtest");

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
            scriptService.compile(new Script(randomFrom(ScriptType.values()), type, "test", Collections.emptyMap()),
                new ScriptContext.Plugin(pluginName, unknownContext));
            fail("script compilation should have been rejected");
        } catch(IllegalArgumentException e) {
            assertThat(e.getMessage(), containsString("script context [" + pluginName + "_" + unknownContext + "] not supported"));
        }
    }

    public void testCompileCountedInCompilationStats() throws IOException {
        buildScriptService(Settings.EMPTY);
        scriptService.compile(new Script(ScriptType.INLINE, "test", "1+1", Collections.emptyMap()), randomFrom(scriptContexts));
        assertEquals(1L, scriptService.stats().getCompilations());
    }

    public void testExecutableCountedInCompilationStats() throws IOException {
        buildScriptService(Settings.EMPTY);
        scriptService.executable(new Script(ScriptType.INLINE, "test", "1+1", Collections.emptyMap()), randomFrom(scriptContexts));
        assertEquals(1L, scriptService.stats().getCompilations());
    }

    public void testSearchCountedInCompilationStats() throws IOException {
        buildScriptService(Settings.EMPTY);
        scriptService.search(null, new Script(ScriptType.INLINE, "test", "1+1", Collections.emptyMap()), randomFrom(scriptContexts));
        assertEquals(1L, scriptService.stats().getCompilations());
    }

    public void testMultipleCompilationsCountedInCompilationStats() throws IOException {
        buildScriptService(Settings.EMPTY);
        int numberOfCompilations = randomIntBetween(1, 1024);
        for (int i = 0; i < numberOfCompilations; i++) {
            scriptService
                    .compile(new Script(ScriptType.INLINE, "test", i + " + " + i, Collections.emptyMap()), randomFrom(scriptContexts));
        }
        assertEquals(numberOfCompilations, scriptService.stats().getCompilations());
    }

    public void testCompilationStatsOnCacheHit() throws IOException {
        Settings.Builder builder = Settings.builder();
        builder.put(ScriptService.SCRIPT_CACHE_SIZE_SETTING.getKey(), 1);
        builder.put("script.inline", "true");
        buildScriptService(builder.build());
        scriptService.executable(new Script(ScriptType.INLINE, "test", "1+1", Collections.emptyMap()), randomFrom(scriptContexts));
        scriptService.executable(new Script(ScriptType.INLINE, "test", "1+1", Collections.emptyMap()), randomFrom(scriptContexts));
        assertEquals(1L, scriptService.stats().getCompilations());
    }

    public void testFileScriptCountedInCompilationStats() throws IOException {
        buildScriptService(Settings.EMPTY);
        createFileScripts("test");
        scriptService.compile(new Script(ScriptType.FILE, "test", "file_script", Collections.emptyMap()), randomFrom(scriptContexts));
        assertEquals(1L, scriptService.stats().getCompilations());
    }

    public void testIndexedScriptCountedInCompilationStats() throws IOException {
        buildScriptService(Settings.EMPTY);
        scriptService.compile(new Script(ScriptType.STORED, "test", "script", Collections.emptyMap()), randomFrom(scriptContexts));
        assertEquals(1L, scriptService.stats().getCompilations());
    }

    public void testCacheEvictionCountedInCacheEvictionsStats() throws IOException {
        Settings.Builder builder = Settings.builder();
        builder.put(ScriptService.SCRIPT_CACHE_SIZE_SETTING.getKey(), 1);
        builder.put("script.inline", "true");
        buildScriptService(builder.build());
        scriptService.executable(new Script(ScriptType.INLINE, "test", "1+1", Collections.emptyMap()), randomFrom(scriptContexts));
        scriptService.executable(new Script(ScriptType.INLINE, "test", "2+2", Collections.emptyMap()), randomFrom(scriptContexts));
        assertEquals(2L, scriptService.stats().getCompilations());
        assertEquals(1L, scriptService.stats().getCacheEvictions());
    }

    public void testDefaultLanguage() throws IOException {
        Settings.Builder builder = Settings.builder();
        builder.put("script.inline", "true");
        buildScriptService(builder.build());
        CompiledScript script = scriptService.compile(
            new Script(ScriptType.INLINE, Script.DEFAULT_SCRIPT_LANG, "1 + 1", Collections.emptyMap()), randomFrom(scriptContexts));
        assertEquals(script.lang(), Script.DEFAULT_SCRIPT_LANG);
    }

    public void testStoreScript() throws Exception {
        BytesReference script = XContentFactory.jsonBuilder().startObject()
                    .field("script", "abc")
                .endObject().bytes();

        ScriptMetaData scriptMetaData = ScriptMetaData.putStoredScript(null, "_id",
            StoredScriptSource.parse("_lang", script, XContentType.JSON));
        assertNotNull(scriptMetaData);
        assertEquals("abc", scriptMetaData.getStoredScript("_id", "_lang").getCode());
    }

    public void testDeleteScript() throws Exception {
        ScriptMetaData scriptMetaData = ScriptMetaData.putStoredScript(null, "_id",
            StoredScriptSource.parse("_lang", new BytesArray("{\"script\":\"abc\"}"), XContentType.JSON));
        scriptMetaData = ScriptMetaData.deleteStoredScript(scriptMetaData, "_id", "_lang");
        assertNotNull(scriptMetaData);
        assertNull(scriptMetaData.getStoredScript("_id", "_lang"));

        ScriptMetaData errorMetaData = scriptMetaData;
        ResourceNotFoundException e = expectThrows(ResourceNotFoundException.class, () -> {
            ScriptMetaData.deleteStoredScript(errorMetaData, "_id", "_lang");
        });
        assertEquals("stored script [_id] using lang [_lang] does not exist and cannot be deleted", e.getMessage());
    }

    public void testGetStoredScript() throws Exception {
        buildScriptService(Settings.EMPTY);
        ClusterState cs = ClusterState.builder(new ClusterName("_name"))
            .metaData(MetaData.builder()
                .putCustom(ScriptMetaData.TYPE,
                    new ScriptMetaData.Builder(null).storeScript("_id",
                        StoredScriptSource.parse("_lang", new BytesArray("{\"script\":\"abc\"}"), XContentType.JSON)).build()))
            .build();

        assertEquals("abc", scriptService.getStoredScript(cs, new GetStoredScriptRequest("_id", "_lang")).getCode());
        assertNull(scriptService.getStoredScript(cs, new GetStoredScriptRequest("_id2", "_lang")));

        cs = ClusterState.builder(new ClusterName("_name")).build();
        assertNull(scriptService.getStoredScript(cs, new GetStoredScriptRequest("_id", "_lang")));
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
            scriptService.compile(new Script(scriptType, lang, script, Collections.emptyMap()), scriptContext);
            fail("compile should have been rejected for lang [" + lang + "], script_type [" + scriptType + "], scripted_op [" + scriptContext + "]");
        } catch(IllegalStateException e) {
            //all good
        }
    }

    private void assertCompileAccepted(String lang, String script, ScriptType scriptType, ScriptContext scriptContext) {
        assertThat(
                scriptService.compile(new Script(scriptType, lang, script, Collections.emptyMap()), scriptContext),
                notNullValue()
        );
    }

    public static class TestEngineService implements ScriptEngineService {

        public static final String NAME = "test";

        private final String name;

        public TestEngineService() {
            this(NAME);
        }

        public TestEngineService(String name) {
            this.name = name;
        }

        @Override
        public String getType() {
            return name;
        }

        @Override
        public String getExtension() {
            return name;
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
        public boolean isInlineScriptEnabled() {
            return true;
        }
    }

    public static class TestDangerousEngineService implements ScriptEngineService {

        public static final String NAME = "dtest";

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
    }
}
