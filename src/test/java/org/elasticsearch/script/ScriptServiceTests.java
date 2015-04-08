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

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.script.ScriptService.ScriptType;
import org.elasticsearch.script.expression.ExpressionScriptEngineService;
import org.elasticsearch.script.groovy.GroovyScriptEngineService;
import org.elasticsearch.script.mustache.MustacheScriptEngineService;
import org.elasticsearch.search.lookup.SearchLookup;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.elasticsearch.watcher.ResourceWatcherService;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.Matchers.*;

public class ScriptServiceTests extends ElasticsearchTestCase {

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
                .put("path.conf", genericConfigFolder)
                .build();
        resourceWatcherService = new ResourceWatcherService(baseSettings, null);
        scriptEngineServices = ImmutableSet.of(new TestEngineService(), new GroovyScriptEngineService(baseSettings),
                new ExpressionScriptEngineService(baseSettings), new MustacheScriptEngineService(baseSettings));
        scriptEnginesByLangMap = ScriptModesTests.buildScriptEnginesByLangMap(scriptEngineServices);
        //randomly register custom script contexts
        int randomInt = randomIntBetween(0, 3);
        //prevent duplicates using map
        Map<String, ScriptContext.Plugin> contexts = Maps.newHashMap();
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
        Settings finalSettings = ImmutableSettings.builder().put(baseSettings).put(additionalSettings).build();
        Environment environment = new Environment(finalSettings);
        scriptService = new ScriptService(finalSettings, environment, scriptEngineServices, resourceWatcherService, scriptContextRegistry) {
            @Override
            String getScriptFromIndex(String scriptLang, String id) {
                //mock the script that gets retrieved from an index
                return "100";
            }
        };
    }

    @Test
    public void testNotSupportedDisableDynamicSetting() throws IOException {
        try {
            buildScriptService(ImmutableSettings.builder().put(ScriptService.DISABLE_DYNAMIC_SCRIPTING_SETTING, randomUnicodeOfLength(randomIntBetween(1, 10))).build());
            fail("script service should have thrown exception due to non supported script.disable_dynamic setting");
        } catch(ElasticsearchIllegalArgumentException e) {
            assertThat(e.getMessage(), containsString(ScriptService.DISABLE_DYNAMIC_SCRIPTING_SETTING + " is not a supported setting, replace with fine-grained script settings"));
        }
    }

    @Test
    public void testScriptsWithoutExtensions() throws IOException {
        buildScriptService(ImmutableSettings.EMPTY);
        logger.info("--> setup two test files one with extension and another without");
        Path testFileNoExt = scriptsFilePath.resolve("test_no_ext");
        Path testFileWithExt = scriptsFilePath.resolve("test_script.tst");
        Streams.copy("test_file_no_ext".getBytes("UTF-8"), Files.newOutputStream(testFileNoExt));
        Streams.copy("test_file".getBytes("UTF-8"), Files.newOutputStream(testFileWithExt));
        resourceWatcherService.notifyNow();

        logger.info("--> verify that file with extension was correctly processed");
        CompiledScript compiledScript = scriptService.compile(new Script("test", "test_script", ScriptType.FILE, null), ScriptContext.Standard.SEARCH);
        assertThat(compiledScript.compiled(), equalTo((Object) "compiled_test_file"));

        logger.info("--> delete both files");
        Files.delete(testFileNoExt);
        Files.delete(testFileWithExt);
        resourceWatcherService.notifyNow();

        logger.info("--> verify that file with extension was correctly removed");
        try {
            scriptService.compile(new Script("test", "test_script", ScriptType.FILE, null), ScriptContext.Standard.SEARCH);
            fail("the script test_script should no longer exist");
        } catch (ElasticsearchIllegalArgumentException ex) {
            assertThat(ex.getMessage(), containsString("Unable to find on disk script test_script"));
        }
    }

    @Test
    public void testScriptsSameNameDifferentLanguage() throws IOException {
        buildScriptService(ImmutableSettings.EMPTY);
        createFileScripts("groovy", "expression");
        CompiledScript groovyScript = scriptService.compile(new Script(GroovyScriptEngineService.NAME, "file_script", ScriptType.FILE, null), randomFrom(scriptContexts));
        assertThat(groovyScript.lang(), equalTo(GroovyScriptEngineService.NAME));
        CompiledScript expressionScript = scriptService.compile(new Script(ExpressionScriptEngineService.NAME, "file_script", ScriptType.FILE, null), randomFrom(scriptContexts));
        assertThat(expressionScript.lang(), equalTo(ExpressionScriptEngineService.NAME));
    }

    @Test
    public void testInlineScriptCompiledOnceMultipleLangAcronyms() throws IOException {
        buildScriptService(ImmutableSettings.EMPTY);
        CompiledScript compiledScript1 = scriptService.compile(new Script("test", "script", ScriptType.INLINE, null), randomFrom(scriptContexts));
        CompiledScript compiledScript2 = scriptService.compile(new Script("test2", "script", ScriptType.INLINE, null), randomFrom(scriptContexts));
        assertThat(compiledScript1, sameInstance(compiledScript2));
    }

    @Test
    public void testFileScriptCompiledOnceMultipleLangAcronyms() throws IOException {
        buildScriptService(ImmutableSettings.EMPTY);
        createFileScripts("test");
        CompiledScript compiledScript1 = scriptService.compile(new Script("test", "file_script", ScriptType.FILE, null), randomFrom(scriptContexts));
        CompiledScript compiledScript2 = scriptService.compile(new Script("test2", "file_script", ScriptType.FILE, null), randomFrom(scriptContexts));
        assertThat(compiledScript1, sameInstance(compiledScript2));
    }

    @Test
    public void testDefaultBehaviourFineGrainedSettings() throws IOException {
        ImmutableSettings.Builder builder = ImmutableSettings.builder();
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
        createFileScripts("groovy", "expression", "mustache", "test");

        for (ScriptContext scriptContext : scriptContexts) {
            //groovy is not sandboxed, only file scripts are enabled by default
            assertCompileRejected(GroovyScriptEngineService.NAME, "script", ScriptType.INLINE, scriptContext);
            assertCompileRejected(GroovyScriptEngineService.NAME, "script", ScriptType.INDEXED, scriptContext);
            assertCompileAccepted(GroovyScriptEngineService.NAME, "file_script", ScriptType.FILE, scriptContext);
            //expression engine is sandboxed, all scripts are enabled by default
            assertCompileAccepted(ExpressionScriptEngineService.NAME, "script", ScriptType.INLINE, scriptContext);
            assertCompileAccepted(ExpressionScriptEngineService.NAME, "script", ScriptType.INDEXED, scriptContext);
            assertCompileAccepted(ExpressionScriptEngineService.NAME, "file_script", ScriptType.FILE, scriptContext);
            //mustache engine is sandboxed, all scripts are enabled by default
            assertCompileAccepted(MustacheScriptEngineService.NAME, "script", ScriptType.INLINE, scriptContext);
            assertCompileAccepted(MustacheScriptEngineService.NAME, "script", ScriptType.INDEXED, scriptContext);
            assertCompileAccepted(MustacheScriptEngineService.NAME, "file_script", ScriptType.FILE, scriptContext);
            //custom engine is sandboxed, all scripts are enabled by default
            assertCompileAccepted("test", "script", ScriptType.INLINE, scriptContext);
            assertCompileAccepted("test", "script", ScriptType.INDEXED, scriptContext);
            assertCompileAccepted("test", "file_script", ScriptType.FILE, scriptContext);
        }
    }

    @Test
    public void testFineGrainedSettings() throws IOException {
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
        ImmutableSettings.Builder builder = ImmutableSettings.builder();
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
                                assertCompileAccepted(lang, script, scriptType, scriptContext);
                                break;
                            case OFF:
                                assertCompileRejected(lang, script, scriptType, scriptContext);
                                break;
                            case SANDBOX:
                                if (scriptEngineService.sandboxed()) {
                                    assertCompileAccepted(lang, script, scriptType, scriptContext);
                                } else {
                                    assertCompileRejected(lang, script, scriptType, scriptContext);
                                }
                                break;
                        }
                    }
                }
            }
        }
    }

    @Test
    public void testCompileNonRegisteredContext() throws IOException {
        buildScriptService(ImmutableSettings.EMPTY);
        String pluginName;
        String unknownContext;
        do {
            pluginName = randomAsciiOfLength(randomIntBetween(1, 10));
            unknownContext = randomAsciiOfLength(randomIntBetween(1, 30));
        } while(scriptContextRegistry.isSupportedContext(new ScriptContext.Plugin(pluginName, unknownContext)));

        for (ScriptEngineService scriptEngineService : scriptEngineServices) {
            for (String type : scriptEngineService.types()) {
                try {
                    scriptService.compile(new Script(type, "test", randomFrom(ScriptType.values()), null), new ScriptContext.Plugin(pluginName, unknownContext));
                    fail("script compilation should have been rejected");
                } catch(ElasticsearchIllegalArgumentException e) {
                    assertThat(e.getMessage(), containsString("script context [" + pluginName + "_" + unknownContext + "] not supported"));
                }
            }
        }
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
            scriptService.compile(new Script(lang, script, scriptType, null), scriptContext);
            fail("compile should have been rejected for lang [" + lang + "], script_type [" + scriptType + "], scripted_op [" + scriptContext + "]");
        } catch(ScriptException e) {
            //all good
        }
    }

    private void assertCompileAccepted(String lang, String script, ScriptType scriptType, ScriptContext scriptContext) {
        assertThat(scriptService.compile(new Script(lang, script, scriptType, null), scriptContext), notNullValue());
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
        public ExecutableScript executable(final Object compiledScript, @Nullable Map<String, Object> vars) {
            return null;
        }

        @Override
        public SearchScript search(Object compiledScript, SearchLookup lookup, @Nullable Map<String, Object> vars) {
            return null;
        }

        @Override
        public Object execute(Object compiledScript, Map<String, Object> vars) {
            return null;
        }

        @Override
        public Object unwrap(Object value) {
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
