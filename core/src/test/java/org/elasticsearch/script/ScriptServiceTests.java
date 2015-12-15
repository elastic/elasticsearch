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
import org.elasticsearch.common.HasContextAndHeaders;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.script.ScriptService.ScriptType;
import org.elasticsearch.search.lookup.SearchLookup;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.watcher.ResourceWatcherService;
import org.junit.Before;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.common.settings.Settings.settingsBuilder;
import static org.elasticsearch.common.util.set.Sets.newHashSet;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.sameInstance;

//TODO: this needs to be a base test class, and all scripting engines extend it
public class ScriptServiceTests extends ESTestCase {

    private ResourceWatcherService resourceWatcherService;
    private Set<ScriptEngineService> scriptEngineServices;
    private Map<String, ScriptEngineService> scriptEnginesByLangMap;
    private ScriptContextRegistry scriptContextRegistry;
    private ScriptContext[] scriptContexts;
    private ScriptService scriptService;
    private Path scriptsFilePath;
    private Settings baseSettings;

    private static final Map<ScriptType, ScriptMode> DEFAULT_SCRIPT_MODES = new HashMap<>();

    static {
        DEFAULT_SCRIPT_MODES.put(ScriptType.FILE, ScriptMode.ON);
        DEFAULT_SCRIPT_MODES.put(ScriptType.INDEXED, ScriptMode.SANDBOX);
        DEFAULT_SCRIPT_MODES.put(ScriptType.INLINE, ScriptMode.SANDBOX);
    }

    @Before
    public void setup() throws IOException {
        Path genericConfigFolder = createTempDir();
        baseSettings = settingsBuilder()
                .put("path.home", createTempDir().toString())
                .put("path.conf", genericConfigFolder)
                .build();
        resourceWatcherService = new ResourceWatcherService(baseSettings, null);
        scriptEngineServices = newHashSet(new TestEngineService());
        scriptEnginesByLangMap = ScriptModesTests.buildScriptEnginesByLangMap(scriptEngineServices);
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
        scriptContexts = scriptContextRegistry.scriptContexts().toArray(new ScriptContext[scriptContextRegistry.scriptContexts().size()]);
        logger.info("--> setup script service");
        scriptsFilePath = genericConfigFolder.resolve("scripts");
        Files.createDirectories(scriptsFilePath);
    }

    private void buildScriptService(Settings additionalSettings) throws IOException {
        Settings finalSettings = Settings.builder().put(baseSettings).put(additionalSettings).build();
        Environment environment = new Environment(finalSettings);
        scriptService = new ScriptService(finalSettings, environment, scriptEngineServices, resourceWatcherService, scriptContextRegistry) {
            @Override
            String getScriptFromIndex(String scriptLang, String id, HasContextAndHeaders headersContext) {
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

        ContextAndHeaderHolder contextAndHeaders = new ContextAndHeaderHolder();
        buildScriptService(Settings.EMPTY);
        logger.info("--> setup two test files one with extension and another without");
        Path testFileNoExt = scriptsFilePath.resolve("test_no_ext");
        Path testFileWithExt = scriptsFilePath.resolve("test_script.tst");
        Streams.copy("test_file_no_ext".getBytes("UTF-8"), Files.newOutputStream(testFileNoExt));
        Streams.copy("test_file".getBytes("UTF-8"), Files.newOutputStream(testFileWithExt));
        resourceWatcherService.notifyNow();

        logger.info("--> verify that file with extension was correctly processed");
        CompiledScript compiledScript = scriptService.compile(new Script("test_script", ScriptType.FILE, "test", null),
                ScriptContext.Standard.SEARCH, contextAndHeaders);
        assertThat(compiledScript.compiled(), equalTo((Object) "compiled_test_file"));

        logger.info("--> delete both files");
        Files.delete(testFileNoExt);
        Files.delete(testFileWithExt);
        resourceWatcherService.notifyNow();

        logger.info("--> verify that file with extension was correctly removed");
        try {
            scriptService.compile(new Script("test_script", ScriptType.FILE, "test", null), ScriptContext.Standard.SEARCH,
                    contextAndHeaders);
            fail("the script test_script should no longer exist");
        } catch (IllegalArgumentException ex) {
            assertThat(ex.getMessage(), containsString("Unable to find on disk file script [test_script] using lang [test]"));
        }
    }

    public void testInlineScriptCompiledOnceCache() throws IOException {
        ContextAndHeaderHolder contextAndHeaders = new ContextAndHeaderHolder();
        buildScriptService(Settings.EMPTY);
        CompiledScript compiledScript1 = scriptService.compile(new Script("1+1", ScriptType.INLINE, "test", null),
                randomFrom(scriptContexts), contextAndHeaders);
        CompiledScript compiledScript2 = scriptService.compile(new Script("1+1", ScriptType.INLINE, "test", null),
                randomFrom(scriptContexts), contextAndHeaders);
        assertThat(compiledScript1.compiled(), sameInstance(compiledScript2.compiled()));
    }

    public void testInlineScriptCompiledOnceMultipleLangAcronyms() throws IOException {
        ContextAndHeaderHolder contextAndHeaders = new ContextAndHeaderHolder();
        buildScriptService(Settings.EMPTY);
        CompiledScript compiledScript1 = scriptService.compile(new Script("script", ScriptType.INLINE, "test", null),
                randomFrom(scriptContexts), contextAndHeaders);
        CompiledScript compiledScript2 = scriptService.compile(new Script("script", ScriptType.INLINE, "test2", null),
                randomFrom(scriptContexts), contextAndHeaders);
        assertThat(compiledScript1.compiled(), sameInstance(compiledScript2.compiled()));
    }

    public void testFileScriptCompiledOnceMultipleLangAcronyms() throws IOException {
        ContextAndHeaderHolder contextAndHeaders = new ContextAndHeaderHolder();
        buildScriptService(Settings.EMPTY);
        createFileScripts("test");
        CompiledScript compiledScript1 = scriptService.compile(new Script("file_script", ScriptType.FILE, "test", null),
                randomFrom(scriptContexts), contextAndHeaders);
        CompiledScript compiledScript2 = scriptService.compile(new Script("file_script", ScriptType.FILE, "test2", null),
                randomFrom(scriptContexts), contextAndHeaders);
        assertThat(compiledScript1.compiled(), sameInstance(compiledScript2.compiled()));
    }

    public void testDefaultBehaviourFineGrainedSettings() throws IOException {
        ContextAndHeaderHolder contextAndHeaders = new ContextAndHeaderHolder();
        Settings.Builder builder = Settings.builder();
        //rarely inject the default settings, which have no effect
        if (rarely()) {
            builder.put("script.file", randomFrom(ScriptModesTests.ENABLE_VALUES));
        }
        if (rarely()) {
            builder.put("script.indexed", ScriptMode.SANDBOX);
        }
        if (rarely()) {
            builder.put("script.inline", ScriptMode.SANDBOX);
        }
        buildScriptService(builder.build());
        createFileScripts("groovy", "mustache", "test");

        for (ScriptContext scriptContext : scriptContexts) {
            //custom engine is sandboxed, all scripts are enabled by default
            assertCompileAccepted("test", "script", ScriptType.INLINE, scriptContext, contextAndHeaders);
            assertCompileAccepted("test", "script", ScriptType.INDEXED, scriptContext, contextAndHeaders);
            assertCompileAccepted("test", "file_script", ScriptType.FILE, scriptContext, contextAndHeaders);
        }
    }

    public void testFineGrainedSettings() throws IOException {
        ContextAndHeaderHolder contextAndHeaders = new ContextAndHeaderHolder();
        //collect the fine-grained settings to set for this run
        int numScriptSettings = randomIntBetween(0, ScriptType.values().length);
        Map<ScriptType, ScriptMode> scriptSourceSettings = new HashMap<>();
        for (int i = 0; i < numScriptSettings; i++) {
            ScriptType scriptType;
            do {
                scriptType = randomFrom(ScriptType.values());
            } while (scriptSourceSettings.containsKey(scriptType));
            scriptSourceSettings.put(scriptType, randomFrom(ScriptMode.values()));
        }
        int numScriptContextSettings = randomIntBetween(0, this.scriptContextRegistry.scriptContexts().size());
        Map<String, ScriptMode> scriptContextSettings = new HashMap<>();
        for (int i = 0; i < numScriptContextSettings; i++) {
            String scriptContext;
            do {
                scriptContext = randomFrom(this.scriptContexts).getKey();
            } while (scriptContextSettings.containsKey(scriptContext));
            scriptContextSettings.put(scriptContext, randomFrom(ScriptMode.values()));
        }
        int numEngineSettings = randomIntBetween(0, 10);
        Map<String, ScriptMode> engineSettings = new HashMap<>();
        for (int i = 0; i < numEngineSettings; i++) {
            String settingKey;
            do {
                ScriptEngineService[] scriptEngineServices = this.scriptEngineServices.toArray(new ScriptEngineService[this.scriptEngineServices.size()]);
                ScriptEngineService scriptEngineService = randomFrom(scriptEngineServices);
                ScriptType scriptType = randomFrom(ScriptType.values());
                ScriptContext scriptContext = randomFrom(this.scriptContexts);
                settingKey = scriptEngineService.types()[0] + "." + scriptType + "." + scriptContext.getKey();
            } while (engineSettings.containsKey(settingKey));
            engineSettings.put(settingKey, randomFrom(ScriptMode.values()));
        }
        //set the selected fine-grained settings
        Settings.Builder builder = Settings.builder();
        for (Map.Entry<ScriptType, ScriptMode> entry : scriptSourceSettings.entrySet()) {
            switch (entry.getValue()) {
                case ON:
                    builder.put(ScriptModes.SCRIPT_SETTINGS_PREFIX + entry.getKey(), randomFrom(ScriptModesTests.ENABLE_VALUES));
                    break;
                case OFF:
                    builder.put(ScriptModes.SCRIPT_SETTINGS_PREFIX + entry.getKey(), randomFrom(ScriptModesTests.DISABLE_VALUES));
                    break;
                case SANDBOX:
                    builder.put(ScriptModes.SCRIPT_SETTINGS_PREFIX + entry.getKey(), ScriptMode.SANDBOX);
                    break;
            }
        }
        for (Map.Entry<String, ScriptMode> entry : scriptContextSettings.entrySet()) {
            switch (entry.getValue()) {
                case ON:
                    builder.put(ScriptModes.SCRIPT_SETTINGS_PREFIX + entry.getKey(), randomFrom(ScriptModesTests.ENABLE_VALUES));
                    break;
                case OFF:
                    builder.put(ScriptModes.SCRIPT_SETTINGS_PREFIX + entry.getKey(), randomFrom(ScriptModesTests.DISABLE_VALUES));
                    break;
                case SANDBOX:
                    builder.put(ScriptModes.SCRIPT_SETTINGS_PREFIX + entry.getKey(), ScriptMode.SANDBOX);
                    break;
            }
        }
        for (Map.Entry<String, ScriptMode> entry : engineSettings.entrySet()) {
            int delimiter = entry.getKey().indexOf('.');
            String part1 = entry.getKey().substring(0, delimiter);
            String part2 = entry.getKey().substring(delimiter + 1);

            String lang = randomFrom(scriptEnginesByLangMap.get(part1).types());
            switch (entry.getValue()) {
                case ON:
                    builder.put(ScriptModes.ENGINE_SETTINGS_PREFIX + "." + lang + "." + part2, randomFrom(ScriptModesTests.ENABLE_VALUES));
                    break;
                case OFF:
                    builder.put(ScriptModes.ENGINE_SETTINGS_PREFIX + "." + lang + "." + part2, randomFrom(ScriptModesTests.DISABLE_VALUES));
                    break;
                case SANDBOX:
                    builder.put(ScriptModes.ENGINE_SETTINGS_PREFIX + "." + lang + "." + part2, ScriptMode.SANDBOX);
                    break;
            }
        }

        buildScriptService(builder.build());
        createFileScripts("groovy", "expression", "mustache", "test");

        for (ScriptEngineService scriptEngineService : scriptEngineServices) {
            for (ScriptType scriptType : ScriptType.values()) {
                //make sure file scripts have a different name than inline ones.
                //Otherwise they are always considered file ones as they can be found in the static cache.
                String script = scriptType == ScriptType.FILE ? "file_script" : "script";
                for (ScriptContext scriptContext : this.scriptContexts) {
                    //fallback mechanism: 1) engine specific settings 2) op based settings 3) source based settings
                    ScriptMode scriptMode = engineSettings.get(scriptEngineService.types()[0] + "." + scriptType + "." + scriptContext.getKey());
                    if (scriptMode == null) {
                        scriptMode = scriptContextSettings.get(scriptContext.getKey());
                    }
                    if (scriptMode == null) {
                        scriptMode = scriptSourceSettings.get(scriptType);
                    }
                    if (scriptMode == null) {
                        scriptMode = DEFAULT_SCRIPT_MODES.get(scriptType);
                    }

                    for (String lang : scriptEngineService.types()) {
                        switch (scriptMode) {
                            case ON:
                            assertCompileAccepted(lang, script, scriptType, scriptContext, contextAndHeaders);
                                break;
                            case OFF:
                            assertCompileRejected(lang, script, scriptType, scriptContext, contextAndHeaders);
                                break;
                            case SANDBOX:
                                if (scriptEngineService.sandboxed()) {
                                assertCompileAccepted(lang, script, scriptType, scriptContext, contextAndHeaders);
                                } else {
                                assertCompileRejected(lang, script, scriptType, scriptContext, contextAndHeaders);
                                }
                                break;
                        }
                    }
                }
            }
        }
    }

    public void testCompileNonRegisteredContext() throws IOException {
        ContextAndHeaderHolder contextAndHeaders = new ContextAndHeaderHolder();
        buildScriptService(Settings.EMPTY);
        String pluginName;
        String unknownContext;
        do {
            pluginName = randomAsciiOfLength(randomIntBetween(1, 10));
            unknownContext = randomAsciiOfLength(randomIntBetween(1, 30));
        } while(scriptContextRegistry.isSupportedContext(new ScriptContext.Plugin(pluginName, unknownContext)));

        for (ScriptEngineService scriptEngineService : scriptEngineServices) {
            for (String type : scriptEngineService.types()) {
                try {
                    scriptService.compile(new Script("test", randomFrom(ScriptType.values()), type, null), new ScriptContext.Plugin(
                            pluginName, unknownContext), contextAndHeaders);
                    fail("script compilation should have been rejected");
                } catch(IllegalArgumentException e) {
                    assertThat(e.getMessage(), containsString("script context [" + pluginName + "_" + unknownContext + "] not supported"));
                }
            }
        }
    }

    public void testCompileCountedInCompilationStats() throws IOException {
        ContextAndHeaderHolder contextAndHeaders = new ContextAndHeaderHolder();
        buildScriptService(Settings.EMPTY);
        scriptService.compile(new Script("1+1", ScriptType.INLINE, "test", null), randomFrom(scriptContexts), contextAndHeaders);
        assertEquals(1L, scriptService.stats().getCompilations());
    }

    public void testExecutableCountedInCompilationStats() throws IOException {
        ContextAndHeaderHolder contextAndHeaders = new ContextAndHeaderHolder();
        buildScriptService(Settings.EMPTY);
        scriptService.executable(new Script("1+1", ScriptType.INLINE, "test", null), randomFrom(scriptContexts), contextAndHeaders);
        assertEquals(1L, scriptService.stats().getCompilations());
    }

    public void testSearchCountedInCompilationStats() throws IOException {
        buildScriptService(Settings.EMPTY);
        scriptService.search(null, new Script("1+1", ScriptType.INLINE, "test", null), randomFrom(scriptContexts));
        assertEquals(1L, scriptService.stats().getCompilations());
    }

    public void testMultipleCompilationsCountedInCompilationStats() throws IOException {
        ContextAndHeaderHolder contextAndHeaders = new ContextAndHeaderHolder();
        buildScriptService(Settings.EMPTY);
        int numberOfCompilations = randomIntBetween(1, 1024);
        for (int i = 0; i < numberOfCompilations; i++) {
            scriptService
                    .compile(new Script(i + " + " + i, ScriptType.INLINE, "test", null), randomFrom(scriptContexts), contextAndHeaders);
        }
        assertEquals(numberOfCompilations, scriptService.stats().getCompilations());
    }

    public void testCompilationStatsOnCacheHit() throws IOException {
        ContextAndHeaderHolder contextAndHeaders = new ContextAndHeaderHolder();
        Settings.Builder builder = Settings.builder();
        builder.put(ScriptService.SCRIPT_CACHE_SIZE_SETTING, 1);
        buildScriptService(builder.build());
        scriptService.executable(new Script("1+1", ScriptType.INLINE, "test", null), randomFrom(scriptContexts), contextAndHeaders);
        scriptService.executable(new Script("1+1", ScriptType.INLINE, "test", null), randomFrom(scriptContexts), contextAndHeaders);
        assertEquals(1L, scriptService.stats().getCompilations());
    }

    public void testFileScriptCountedInCompilationStats() throws IOException {
        ContextAndHeaderHolder contextAndHeaders = new ContextAndHeaderHolder();
        buildScriptService(Settings.EMPTY);
        createFileScripts("test");
        scriptService.compile(new Script("file_script", ScriptType.FILE, "test", null), randomFrom(scriptContexts), contextAndHeaders);
        assertEquals(1L, scriptService.stats().getCompilations());
    }

    public void testIndexedScriptCountedInCompilationStats() throws IOException {
        ContextAndHeaderHolder contextAndHeaders = new ContextAndHeaderHolder();
        buildScriptService(Settings.EMPTY);
        scriptService.compile(new Script("script", ScriptType.INDEXED, "test", null), randomFrom(scriptContexts), contextAndHeaders);
        assertEquals(1L, scriptService.stats().getCompilations());
    }

    public void testCacheEvictionCountedInCacheEvictionsStats() throws IOException {
        ContextAndHeaderHolder contextAndHeaders = new ContextAndHeaderHolder();
        Settings.Builder builder = Settings.builder();
        builder.put(ScriptService.SCRIPT_CACHE_SIZE_SETTING, 1);
        buildScriptService(builder.build());
        scriptService.executable(new Script("1+1", ScriptType.INLINE, "test", null), randomFrom(scriptContexts), contextAndHeaders);
        scriptService.executable(new Script("2+2", ScriptType.INLINE, "test", null), randomFrom(scriptContexts), contextAndHeaders);
        assertEquals(2L, scriptService.stats().getCompilations());
        assertEquals(1L, scriptService.stats().getCacheEvictions());
    }

    private void createFileScripts(String... langs) throws IOException {
        for (String lang : langs) {
            Path scriptPath = scriptsFilePath.resolve("file_script." + lang);
            Streams.copy("10".getBytes("UTF-8"), Files.newOutputStream(scriptPath));
        }
        resourceWatcherService.notifyNow();
    }

    private void assertCompileRejected(String lang, String script, ScriptType scriptType, ScriptContext scriptContext,
            HasContextAndHeaders contextAndHeaders) {
        try {
            scriptService.compile(new Script(script, scriptType, lang, null), scriptContext, contextAndHeaders);
            fail("compile should have been rejected for lang [" + lang + "], script_type [" + scriptType + "], scripted_op [" + scriptContext + "]");
        } catch(ScriptException e) {
            //all good
        }
    }

    private void assertCompileAccepted(String lang, String script, ScriptType scriptType, ScriptContext scriptContext,
            HasContextAndHeaders contextAndHeaders) {
        assertThat(scriptService.compile(new Script(script, scriptType, lang, null), scriptContext, contextAndHeaders), notNullValue());
    }

    public static class TestEngineService implements ScriptEngineService {

        @Override
        public String[] types() {
            return new String[] {"test", "test2"};
        }

        @Override
        public String[] extensions() {
            return new String[] {"test", "tst"};
        }

        @Override
        public boolean sandboxed() {
            return true;
        }

        @Override
        public Object compile(String script) {
            return "compiled_" + script;
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

}
