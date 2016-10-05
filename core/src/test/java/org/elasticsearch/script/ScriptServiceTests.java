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
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.script.Script.ExecutableScriptBinding;
import org.elasticsearch.script.Script.ScriptInput;
import org.elasticsearch.script.Script.ScriptType;
import org.elasticsearch.script.Script.SearchScriptBinding;
import org.elasticsearch.script.Script.StoredScriptSource;
import org.elasticsearch.script.Script.UnknownScriptBinding;
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
        DEFAULT_SCRIPT_ENABLED.put(Script.ScriptType.FILE, true);
        DEFAULT_SCRIPT_ENABLED.put(Script.ScriptType.STORED, false);
        DEFAULT_SCRIPT_ENABLED.put(Script.ScriptType.INLINE, false);
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
        scriptService = new ScriptService(finalSettings, environment, resourceWatcherService, scriptEngineRegistry, scriptContextRegistry, scriptSettings);
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

    public void testScriptsWithoutExtensions() throws IOException {
        buildScriptService(Settings.EMPTY);
        Path testFileNoExt = scriptsFilePath.resolve("test_no_ext");
        Path testFileWithExt = scriptsFilePath.resolve("test_script.test");
        Streams.copy("test_file_no_ext".getBytes("UTF-8"), Files.newOutputStream(testFileNoExt));
        Streams.copy("test_file".getBytes("UTF-8"), Files.newOutputStream(testFileWithExt));
        resourceWatcherService.notifyNow();

        CompiledScript compiledScript = ScriptInput.file("test_script").lookup
            .getCompiled(scriptService, ScriptContext.Standard.SEARCH, ExecutableScriptBinding.BINDING);
        assertThat(compiledScript.compiled(), equalTo((Object) "compiled_test_file"));

        Files.delete(testFileNoExt);
        Files.delete(testFileWithExt);
        resourceWatcherService.notifyNow();

        try {
            ScriptInput.file("test_script").lookup
                .getCompiled(scriptService, ScriptContext.Standard.SEARCH, ExecutableScriptBinding.BINDING);
            fail("the script test_script should no longer exist");
        } catch (ResourceNotFoundException ex) {
            assertThat(ex.getMessage(), containsString("file script [test_script] does not exist"));
        }
    }

    public void testScriptCompiledOnceHiddenFileDetected() throws IOException {
        buildScriptService(Settings.EMPTY);

        Path testHiddenFile = scriptsFilePath.resolve(".hidden_file");
        Streams.copy("test_hidden_file".getBytes("UTF-8"), Files.newOutputStream(testHiddenFile));

        Path testFileScript = scriptsFilePath.resolve("file_script.test");
        Streams.copy("test_file_script".getBytes("UTF-8"), Files.newOutputStream(testFileScript));
        resourceWatcherService.notifyNow();

        CompiledScript compiledScript = ScriptInput.file("file_script").lookup
            .getCompiled(scriptService, ScriptContext.Standard.SEARCH, ExecutableScriptBinding.BINDING);
        assertThat(compiledScript.compiled(), equalTo((Object) "compiled_test_file_script"));

        Files.delete(testHiddenFile);
        Files.delete(testFileScript);
        resourceWatcherService.notifyNow();
    }

    public void testInlineScriptCompiledOnceCache() throws IOException {
        buildScriptService(Settings.EMPTY);
        CompiledScript compiledScript1 = ScriptInput.inline("1+1").lookup
            .getCompiled(scriptService, ScriptContext.Standard.SEARCH, ExecutableScriptBinding.BINDING);
        CompiledScript compiledScript2 = ScriptInput.inline("1+1").lookup
            .getCompiled(scriptService, ScriptContext.Standard.SEARCH, ExecutableScriptBinding.BINDING);
        assertThat(compiledScript1.compiled(), sameInstance(compiledScript2.compiled()));
    }

    public void testDefaultBehaviourFineGrainedSettings() throws IOException {
        Settings.Builder builder = Settings.builder();
        //rarely inject the default settings, which have no effect
        if (rarely()) {
            builder.put("script.file", "true");
        }
        buildScriptService(builder.build());
        createFileScripts("groovy.groovy", "mustache.mustache", "dtest.dtest");
        createStoredScript(null, "script", "dtest", "script");

        for (ScriptContext scriptContext : scriptContexts) {
            // only file scripts are accepted by default
            assertCompileRejected("dtest", "script", Script.ScriptType.INLINE, scriptContext);
            assertCompileRejected("dtest", "script", Script.ScriptType.STORED, scriptContext);
            assertCompileAccepted("dtest", "dtest", Script.ScriptType.FILE, scriptContext);
        }
    }

    public void testFineGrainedSettings() throws IOException {
        String[] langs = new String[] {"test", "painless", "dtest"};
        String[] names = new String[] {"tscript", "pscript", "dscript"};

        Map<ScriptType, Boolean> scriptSourceSettings = new HashMap<ScriptType, Boolean>() {
            @Override
            public String toString() {
                StringBuilder builder = new StringBuilder();

                for (Entry<ScriptType, Boolean> entry : entrySet()) {
                    builder.append(entry.getKey());
                    builder.append(':');
                    builder.append(entry.getValue());
                    builder.append("\n");
                }

                return builder.toString();
            }
        };

        for (ScriptType type : ScriptType.values()) {
            if (randomBoolean()) {
                scriptSourceSettings.put(type, randomBoolean());
            }
        }

        System.out.println(scriptSourceSettings);

        Map<ScriptContext, Boolean> scriptContextSettings = new HashMap<ScriptContext, Boolean>() {
            @Override
            public String toString() {
                StringBuilder builder = new StringBuilder();

                for (Entry<ScriptContext, Boolean> entry : entrySet()) {
                    builder.append(entry.getKey());
                    builder.append(':');
                    builder.append(entry.getValue());
                    builder.append("\n");
                }

                return builder.toString();
            }
        };

        for (ScriptContext context : scriptContextRegistry.scriptContexts()) {
            if (randomBoolean()) {
                scriptContextSettings.put(randomFrom(scriptContexts), randomBoolean());
            }
        }

        System.out.println(scriptContextSettings);

        int numEngineSettings = randomIntBetween(0, ScriptType.values().length * scriptContexts.length);
        Map<String, Boolean> engineSettings = new HashMap<String, Boolean>() {
            @Override
            public String toString() {
                StringBuilder builder = new StringBuilder();

                for (Entry<String, Boolean> entry : entrySet()) {
                    builder.append(entry.getKey());
                    builder.append(':');
                    builder.append(entry.getValue());
                    builder.append("\n");
                }

                return builder.toString();
            }
        };

        for (int setting = 0; setting < numEngineSettings; ++setting) {
            ScriptType scriptType = randomFrom(ScriptType.values());
            ScriptContext scriptContext = randomFrom(this.scriptContexts);
            String settingKey = randomFrom(langs) + "." + scriptType + "." + scriptContext.getKey();

            engineSettings.put(settingKey, randomBoolean());
        }

        System.out.println(engineSettings);

        Settings.Builder builder = Settings.builder();

        for (Map.Entry<ScriptType, Boolean> entry : scriptSourceSettings.entrySet()) {
            if (entry.getValue()) {
                builder.put("script" + "." + entry.getKey().name, "true");
            } else {
                builder.put("script" + "." + entry.getKey().name, "false");
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

            if (entry.getValue()) {
                builder.put("script.engine" + "." + part1 + "." + part2, "true");
            } else {
                builder.put("script.engine" + "." + part1 + "." + part2, "false");
            }
        }

        buildScriptService(builder.build());
        createFileScripts("tscript.test", "pscript.painless", "dscript.dtest");
        ClusterState state = createStoredScript(null, "dscript", "dtest", "script");
        state = createStoredScript(state, "pscript", "painless", "1+1");
        createStoredScript(state, "tscript", "test", "script");

        for (ScriptType scriptType : Script.ScriptType.values()) {
            for (ScriptContext scriptContext : this.scriptContexts) {
                for (int lang = 0; lang < langs.length; ++lang) {
                    //fallback mechanism: 1) engine specific settings 2) op based settings 3) source based settings
                    Boolean scriptEnabled = engineSettings.get("dtest"/*langs[lang]*/ + "." + scriptType + "." + scriptContext.getKey());

                    if (scriptEnabled == null) {
                        System.out.println("context: " + scriptContext);
                        scriptEnabled = scriptContextSettings.get(scriptContext);
                    }

                    if (scriptEnabled == null) {
                        System.out.println("type: " + scriptType);
                        scriptEnabled = scriptSourceSettings.get(scriptType);
                    }

                    if (scriptEnabled == null) {
                        System.out.println("default: " + scriptType);
                        scriptEnabled = DEFAULT_SCRIPT_ENABLED.get(scriptType);
                    }

                    if (scriptEnabled) {
                        assertCompileAccepted("dtest", "dscript" /*langs[lang], names[lang]*/, scriptType, scriptContext);
                    } else {
                        assertCompileRejected("dtest", "dscript" /*langs[lang], names[lang]*/, scriptType, scriptContext);
                    }
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

        ScriptType type = randomFrom(ScriptType.values());
        ScriptInput input;

        if (type == ScriptType.FILE) {
            createFileScripts("script.painless");
            input = ScriptInput.file("script");
        } else if (type == ScriptType.STORED) {
            createStoredScript(null, "script", "painless", "1+1");
            input = ScriptInput.stored("script");
        } else {
            input = ScriptInput.inline("test", "1+1", Collections.emptyMap());
        }

        try {
            input.lookup.getCompiled(scriptService, new ScriptContext.Plugin(pluginName, unknownContext), ExecutableScriptBinding.BINDING);
            fail("script compilation should have been rejected");
        } catch(IllegalArgumentException e) {
            assertThat(e.getMessage(), containsString("script context [" + pluginName + "_" + unknownContext + "] does not exist"));
        }
    }

    public void testCompileCountedInCompilationStats() throws IOException {
        buildScriptService(Settings.EMPTY);
        ScriptInput.inline("test", "1+1", Collections.emptyMap())
            .lookup.getCompiled(scriptService, randomFrom(scriptContexts), ExecutableScriptBinding.BINDING);
        assertEquals(1L, scriptService.stats().getCompilations());
    }

    public void testExecutableCountedInCompilationStats() throws IOException {
        buildScriptService(Settings.EMPTY);
        ScriptInput scriptInput = ScriptInput.inline("test", "1+1", Collections.emptyMap());
        ExecutableScriptBinding.bind(scriptService, randomFrom(scriptContexts), scriptInput.lookup, scriptInput.params);
        assertEquals(1L, scriptService.stats().getCompilations());
    }

    public void testSearchCountedInCompilationStats() throws IOException {
        buildScriptService(Settings.EMPTY);
        ScriptInput scriptInput = ScriptInput.inline("test", "1+1", Collections.emptyMap());
        SearchScriptBinding.bind(scriptService, randomFrom(scriptContexts), null, scriptInput.lookup, scriptInput.params);
        assertEquals(1L, scriptService.stats().getCompilations());
    }

    public void testMultipleCompilationsCountedInCompilationStats() throws IOException {
        buildScriptService(Settings.EMPTY);
        int numberOfCompilations = randomIntBetween(1, 1024);
        for (int i = 0; i < numberOfCompilations; i++) {
            ScriptInput.inline("test", i + " + " + i, Collections.emptyMap())
                .lookup.getCompiled(scriptService, randomFrom(scriptContexts), ExecutableScriptBinding.BINDING);
        }
        assertEquals(numberOfCompilations, scriptService.stats().getCompilations());
    }

    public void testCompilationStatsOnCacheHit() throws IOException {
        Settings.Builder builder = Settings.builder();
        builder.put(ScriptService.SCRIPT_CACHE_SIZE_SETTING.getKey(), 1);
        builder.put("script.inline", "true");
        buildScriptService(builder.build());
        ScriptInput scriptInput = ScriptInput.inline("test", "1+1", Collections.emptyMap());
        ExecutableScriptBinding.bind(scriptService, randomFrom(scriptContexts), scriptInput.lookup, scriptInput.params);
        ExecutableScriptBinding.bind(scriptService, randomFrom(scriptContexts), scriptInput.lookup, scriptInput.params);
        assertEquals(1L, scriptService.stats().getCompilations());
    }

    public void testFileScriptCountedInCompilationStats() throws IOException {
        buildScriptService(Settings.EMPTY);
        createFileScripts("file_script.test");
        ScriptInput.file("file_script").lookup.getCompiled(scriptService, randomFrom(scriptContexts), ExecutableScriptBinding.BINDING);
        assertEquals(1L, scriptService.stats().getCompilations());
    }

    public void testStoredScriptCountedInCompilationStats() throws IOException {
        buildScriptService(Settings.EMPTY);
        createStoredScript(null, "stored_script", "test", "script");
        ScriptInput.stored("stored_script").lookup.getCompiled(scriptService, randomFrom(scriptContexts), ExecutableScriptBinding.BINDING);
        assertEquals(1L, scriptService.stats().getCompilations());
    }

    public void testCacheEvictionCountedInCacheEvictionsStats() throws IOException {
        Settings.Builder builder = Settings.builder();
        builder.put(ScriptService.SCRIPT_CACHE_SIZE_SETTING.getKey(), 1);
        builder.put("script.inline", "true");
        buildScriptService(builder.build());
        ScriptInput scriptInput1 = ScriptInput.inline("test", "1+1", Collections.emptyMap());
        ExecutableScriptBinding.bind(scriptService, randomFrom(scriptContexts), scriptInput1.lookup, scriptInput1.params);
        ScriptInput scriptInput2 = ScriptInput.inline("test", "2+2", Collections.emptyMap());
        ExecutableScriptBinding.bind(scriptService, randomFrom(scriptContexts), scriptInput2.lookup, scriptInput2.params);
        assertEquals(2L, scriptService.stats().getCompilations());
        assertEquals(1L, scriptService.stats().getCacheEvictions());
    }

    public void testDefaultLanguage() throws IOException {
        Settings.Builder builder = Settings.builder();
        builder.put("script.inline", "true");
        buildScriptService(builder.build());
        CompiledScript compiled = ScriptInput.inline("test")
            .lookup.getCompiled(scriptService, randomFrom(scriptContexts), ExecutableScriptBinding.BINDING);
        assertEquals(compiled.lang(), Script.DEFAULT_SCRIPT_LANG);
    }


    private void createFileScripts(String... names) throws IOException {
        for (String name : names) {
            Path scriptPath = scriptsFilePath.resolve(name);
            Streams.copy("10".getBytes("UTF-8"), Files.newOutputStream(scriptPath));
        }
        resourceWatcherService.notifyNow();
    }

    private ClusterState createStoredScript(ClusterState state, String name, String lang, String code) {
        if (state == null) {
            state = ClusterState.builder(new ClusterName("_name")).build();
        }

        ClusterState stored = ScriptMetaData.storeScript(state, name,
            new StoredScriptSource(false, UnknownScriptBinding.NAME, lang, code, Collections.emptyMap()));
        scriptService.clusterChanged(new ClusterChangedEvent("test", stored, state));

        return stored;
    }

    private void assertCompileRejected(String lang, String script, ScriptType scriptType, ScriptContext scriptContext) {
        try {
            ScriptInput input;

            if (scriptType == ScriptType.FILE) {
                input = ScriptInput.file(script);
            } else if (scriptType == ScriptType.STORED) {
                input = ScriptInput.stored(script);
            } else {
                input = ScriptInput.inline(lang, script, Collections.emptyMap());
            }

            input.lookup.getCompiled(scriptService, scriptContext, ExecutableScriptBinding.BINDING);
            System.out.println(scriptService.scriptModes);
            System.out.println(scriptService.scriptModes.scriptEnabled);
            fail("compile should have been rejected for lang [" + lang + "], script_type [" + scriptType + "], scripted_op [" + scriptContext + "]");
        } catch(IllegalArgumentException | ResourceNotFoundException e) {
            //all good
        }
    }

    private void assertCompileAccepted(String lang, String script, ScriptType scriptType, ScriptContext scriptContext) {
        ScriptInput input;

        if (scriptType == ScriptType.FILE) {
            input = ScriptInput.file(script);
        } else if (scriptType == ScriptType.STORED) {
            input = ScriptInput.stored(script);
        } else {
            input = ScriptInput.inline(lang, script, Collections.emptyMap());
        }

        assertThat(
            input.lookup.getCompiled(scriptService, scriptContext, ExecutableScriptBinding.BINDING),
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
