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
import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.node.settings.NodeSettingsService;
import org.elasticsearch.script.ScriptService.ScriptType;
import org.elasticsearch.script.expression.ExpressionScriptEngineService;
import org.elasticsearch.script.groovy.GroovyScriptEngineService;
import org.elasticsearch.script.mustache.MustacheScriptEngineService;
import org.elasticsearch.search.lookup.SearchLookup;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.elasticsearch.watcher.ResourceWatcherService;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.hamcrest.Matchers.*;

/**
 *
 */
public class ScriptServiceTests extends ElasticsearchTestCase {

    private ResourceWatcherService resourceWatcherService;
    private Set<ScriptEngineService> scriptEngineServices;
    private Map<String, ScriptEngineService> scriptEnginesByLangMap;
    private ScriptService scriptService;
    private File scriptsFile;
    private Settings baseSettings;

    private static final Map<ScriptType, ScriptMode> DEFAULT_SCRIPT_MODES = new HashMap<>();

    static {
        DEFAULT_SCRIPT_MODES.put(ScriptType.FILE, ScriptMode.ENABLE);
        DEFAULT_SCRIPT_MODES.put(ScriptType.INDEXED, ScriptMode.SANDBOX);
        DEFAULT_SCRIPT_MODES.put(ScriptType.INLINE, ScriptMode.SANDBOX);
    }

    @Before
    public void setup() throws IOException {
        File configDir = newTempDir();
        baseSettings = settingsBuilder()
                .put("path.conf", configDir)
                .build();
        resourceWatcherService = new ResourceWatcherService(baseSettings, null);
        scriptEngineServices = ImmutableSet.of(new TestEngineService(), new GroovyScriptEngineService(baseSettings),
                new ExpressionScriptEngineService(baseSettings), new MustacheScriptEngineService(baseSettings));
        scriptEnginesByLangMap = ScriptModesTests.buildScriptEnginesByLangMap(scriptEngineServices);
        logger.info("--> setup script service");
        scriptsFile = new File(configDir, "scripts");
        assertThat(scriptsFile.mkdir(), equalTo(true));
    }

    private void buildScriptService(Settings additionalSettings) throws IOException {
        Settings finalSettings = ImmutableSettings.builder().put(baseSettings).put(additionalSettings).build();
        Environment environment = new Environment(finalSettings);
        scriptService = new ScriptService(finalSettings, environment, scriptEngineServices, resourceWatcherService, new NodeSettingsService(finalSettings)) {
            @Override
            String getScriptFromIndex(String scriptLang, String id) {
                //mock the script that gets retrieved from an index
                return "100";
            }
        };
    }

    @Test
    public void testScriptsWithoutExtensions() throws IOException {
        buildScriptService(ImmutableSettings.EMPTY);
        logger.info("--> setup two test files one with extension and another without");
        File testFileNoExt = new File(scriptsFile, "test_no_ext");
        File testFileWithExt = new File(scriptsFile, "test_script.tst");
        Streams.copy("test_file_no_ext".getBytes("UTF-8"), testFileNoExt);
        Streams.copy("test_file".getBytes("UTF-8"), testFileWithExt);
        resourceWatcherService.notifyNow();

        logger.info("--> verify that file with extension was correctly processed");
        CompiledScript compiledScript = scriptService.compile("test", "test_script", ScriptType.FILE, ScriptedOp.SEARCH);
        assertThat(compiledScript.compiled(), equalTo((Object) "compiled_test_file"));

        logger.info("--> delete both files");
        assertThat(testFileNoExt.delete(), equalTo(true));
        assertThat(testFileWithExt.delete(), equalTo(true));
        resourceWatcherService.notifyNow();

        logger.info("--> verify that file with extension was correctly removed");
        try {
            scriptService.compile("test", "test_script", ScriptType.FILE, ScriptedOp.SEARCH);
            fail("the script test_script should no longer exist");
        } catch (ElasticsearchIllegalArgumentException ex) {
            assertThat(ex.getMessage(), containsString("Unable to find on disk script test_script"));
        }
    }

    @Test
    public void testScriptsSameNameDifferentLanguage() throws IOException {
        buildScriptService(ImmutableSettings.EMPTY);
        createFileScripts("groovy", "expression");
        CompiledScript groovyScript = scriptService.compile(GroovyScriptEngineService.NAME, "file_script", ScriptType.FILE, randomFrom(ScriptedOp.values()));
        assertThat(groovyScript.lang(), equalTo(GroovyScriptEngineService.NAME));
        CompiledScript expressionScript = scriptService.compile(ExpressionScriptEngineService.NAME, "file_script", ScriptType.FILE, randomFrom(ScriptedOp.values()));
        assertThat(expressionScript.lang(), equalTo(ExpressionScriptEngineService.NAME));
    }

    @Test
    public void testInlineScriptCompiledOnceMultipleLangAcronyms() throws IOException {
        buildScriptService(ImmutableSettings.EMPTY);
        CompiledScript compiledScript1 = scriptService.compile("test", "script", ScriptType.INLINE, randomFrom(ScriptedOp.values()));
        CompiledScript compiledScript2 = scriptService.compile("test2", "script", ScriptType.INLINE, randomFrom(ScriptedOp.values()));
        assertThat(compiledScript1, sameInstance(compiledScript2));
    }

    @Test
    public void testFileScriptCompiledOnceMultipleLangAcronyms() throws IOException {
        buildScriptService(ImmutableSettings.EMPTY);
        createFileScripts("test");
        CompiledScript compiledScript1 = scriptService.compile("test", "file_script", ScriptType.FILE, randomFrom(ScriptedOp.values()));
        CompiledScript compiledScript2 = scriptService.compile("test2", "file_script", ScriptType.FILE, randomFrom(ScriptedOp.values()));
        assertThat(compiledScript1, sameInstance(compiledScript2));
    }

    @Test
    public void testDefaultBehaviourFineGrainedSettings() throws IOException {
        ImmutableSettings.Builder builder = ImmutableSettings.builder();
        //rarely inject the default settings, which have no effect
        if (rarely()) {
            builder.put("script.file", randomFrom(ScriptModesTests.ENABLE.values()));
        }
        if (rarely()) {
            builder.put("script.indexed", randomFrom(ScriptModesTests.SANDBOX.values()));
        }
        if (rarely()) {
            builder.put("script.dynamic", randomFrom(ScriptModesTests.SANDBOX.values()));
        }
        buildScriptService(builder.build());
        createFileScripts("groovy", "expression", "mustache", "test");

        for (ScriptedOp scriptedOp : ScriptedOp.values()) {
            //groovy is not sandboxed, only file scripts are enabled by default
            assertCompileRejected(GroovyScriptEngineService.NAME, "script", ScriptType.INLINE, scriptedOp);
            assertCompileRejected(GroovyScriptEngineService.NAME, "script", ScriptType.INDEXED, scriptedOp);
            assertCompileAccepted(GroovyScriptEngineService.NAME, "file_script", ScriptType.FILE, scriptedOp);
            //expression engine is sandboxed, all scripts are enabled by default
            assertCompileAccepted(ExpressionScriptEngineService.NAME, "script", ScriptType.INLINE, scriptedOp);
            assertCompileAccepted(ExpressionScriptEngineService.NAME, "script", ScriptType.INDEXED, scriptedOp);
            assertCompileAccepted(ExpressionScriptEngineService.NAME, "file_script", ScriptType.FILE, scriptedOp);
            //mustache engine is sandboxed, all scripts are enabled by default
            assertCompileAccepted(MustacheScriptEngineService.NAME, "script", ScriptType.INLINE, scriptedOp);
            assertCompileAccepted(MustacheScriptEngineService.NAME, "script", ScriptType.INDEXED, scriptedOp);
            assertCompileAccepted(MustacheScriptEngineService.NAME, "file_script", ScriptType.FILE, scriptedOp);
            //custom engine is sandboxed, all scripts are enabled by default
            assertCompileAccepted("test", "script", ScriptType.INLINE, scriptedOp);
            assertCompileAccepted("test", "script", ScriptType.INDEXED, scriptedOp);
            assertCompileAccepted("test", "file_script", ScriptType.FILE, scriptedOp);
        }
    }

    @Test
    public void testDisableDynamicDeprecatedSetting() throws IOException {
        ImmutableSettings.Builder builder = ImmutableSettings.builder();
        ScriptService.DynamicScriptDisabling dynamicScriptDisabling = randomFrom(ScriptService.DynamicScriptDisabling.values());
        switch(dynamicScriptDisabling) {
            case EVERYTHING_ALLOWED:
                builder.put("script.disable_dynamic", randomFrom("false", "none"));
                break;
            case ONLY_DISK_ALLOWED:
                builder.put("script.disable_dynamic", randomFrom("true", "all"));
                break;
            case SANDBOXED_ONLY:
                builder.put("script.disable_dynamic", randomFrom("sandbox", "sandboxed"));
                break;
        }

        buildScriptService(builder.build());
        createFileScripts("groovy", "expression", "mustache", "test");

        for (ScriptedOp scriptedOp : ScriptedOp.values()) {
            for (ScriptEngineService scriptEngineService : scriptEngineServices) {
                for (String lang : scriptEngineService.types()) {
                    assertCompileAccepted(lang, "file_script", ScriptType.FILE, scriptedOp);

                    switch (dynamicScriptDisabling) {
                        case EVERYTHING_ALLOWED:
                            assertCompileAccepted(lang, "script", ScriptType.INDEXED, scriptedOp);
                            assertCompileAccepted(lang, "script", ScriptType.INLINE, scriptedOp);
                            break;
                        case ONLY_DISK_ALLOWED:
                            assertCompileRejected(lang, "script", ScriptType.INDEXED, scriptedOp);
                            assertCompileRejected(lang, "script", ScriptType.INLINE, scriptedOp);
                            break;
                        case SANDBOXED_ONLY:
                            if (scriptEngineService.sandboxed()) {
                                assertCompileAccepted(lang, "script", ScriptType.INDEXED, scriptedOp);
                                assertCompileAccepted(lang, "script", ScriptType.INLINE, scriptedOp);
                            } else {
                                assertCompileRejected(lang, "script", ScriptType.INDEXED, scriptedOp);
                                assertCompileRejected(lang, "script", ScriptType.INLINE, scriptedOp);
                            }
                            break;
                    }
                }
            }
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
        int numScriptedOpSettings = randomIntBetween(0, ScriptedOp.values().length);
        Map<ScriptedOp, ScriptMode> scriptedOpSettings = new HashMap<>();
        for (int i = 0; i < numScriptedOpSettings; i++) {
            ScriptedOp scriptedOp;
            do {
                scriptedOp = randomFrom(ScriptedOp.values());
            } while (scriptedOpSettings.containsKey(scriptedOp));
            scriptedOpSettings.put(scriptedOp, randomFrom(ScriptMode.values()));
        }
        int numEngineSettings = randomIntBetween(0, 10);
        Map<String, ScriptMode> engineSettings = new HashMap<>();
        for (int i = 0; i < numEngineSettings; i++) {
            String settingKey;
            do {
                ScriptEngineService[] scriptEngineServices = this.scriptEngineServices.toArray(new ScriptEngineService[this.scriptEngineServices.size()]);
                ScriptEngineService scriptEngineService = randomFrom(scriptEngineServices);
                ScriptType scriptType = randomFrom(ScriptType.values());
                ScriptedOp scriptedOp = randomFrom(ScriptedOp.values());
                settingKey = scriptEngineService.types()[0] + "." + scriptType + "." + scriptedOp;
            } while(engineSettings.containsKey(settingKey));
            engineSettings.put(settingKey, randomFrom(ScriptMode.values()));
        }
        //set the selected fine-grained settings
        ImmutableSettings.Builder builder = ImmutableSettings.builder();
        for (Map.Entry<ScriptType, ScriptMode> entry : scriptSourceSettings.entrySet()) {
            switch (entry.getValue()) {
                case ENABLE:
                    builder.put(ScriptModes.SCRIPT_SETTINGS_PREFIX + entry.getKey(), randomFrom(ScriptModesTests.ENABLE.values()));
                    break;
                case DISABLE:
                    builder.put(ScriptModes.SCRIPT_SETTINGS_PREFIX + entry.getKey(), randomFrom(ScriptModesTests.DISABLE.values()));
                    break;
                case SANDBOX:
                    builder.put(ScriptModes.SCRIPT_SETTINGS_PREFIX + entry.getKey(), randomFrom(ScriptModesTests.SANDBOX.values()));
                    break;
            }
        }
        for (Map.Entry<ScriptedOp, ScriptMode> entry : scriptedOpSettings.entrySet()) {
            switch (entry.getValue()) {
                case ENABLE:
                    builder.put(ScriptModes.SCRIPT_SETTINGS_PREFIX + entry.getKey(), randomFrom(ScriptModesTests.ENABLE.values()));
                    break;
                case DISABLE:
                    builder.put(ScriptModes.SCRIPT_SETTINGS_PREFIX + entry.getKey(), randomFrom(ScriptModesTests.DISABLE.values()));
                    break;
                case SANDBOX:
                    builder.put(ScriptModes.SCRIPT_SETTINGS_PREFIX + entry.getKey(), randomFrom(ScriptModesTests.SANDBOX.values()));
                    break;
            }
        }
        for (Map.Entry<String, ScriptMode> entry : engineSettings.entrySet()) {
            int delimiter = entry.getKey().indexOf('.');
            String part1 = entry.getKey().substring(0, delimiter);
            String part2 = entry.getKey().substring(delimiter + 1);

            String lang = randomFrom(scriptEnginesByLangMap.get(part1).types());
            switch (entry.getValue()) {
                case ENABLE:
                    builder.put(ScriptModes.ENGINE_SETTINGS_PREFIX + "." + lang + "." + part2, randomFrom(ScriptModesTests.ENABLE.values()));
                    break;
                case DISABLE:
                    builder.put(ScriptModes.ENGINE_SETTINGS_PREFIX + "." + lang + "." + part2, randomFrom(ScriptModesTests.DISABLE.values()));
                    break;
                case SANDBOX:
                    builder.put(ScriptModes.ENGINE_SETTINGS_PREFIX + "." + lang + "." + part2, randomFrom(ScriptModesTests.SANDBOX.values()));
                    break;
            }
        }

        buildScriptService(builder.build());
        createFileScripts("groovy", "expression", "mustache", "test");

        for (ScriptEngineService scriptEngineService : scriptEngineServices) {
            for (ScriptType scriptType : ScriptType.values()) {
                //make sure file scripts have a different name than dynamic ones.
                //Otherwise they are always considered file ones as they can be found in the static cache.
                String script = scriptType == ScriptType.FILE ? "file_script" : "script";
                for (ScriptedOp scriptedOp : ScriptedOp.values()) {
                    //fallback mechanism: 1) engine specific settings 2) op based settings 3) source based settings
                    ScriptMode scriptMode = engineSettings.get(scriptEngineService.types()[0] + "." + scriptType + "." + scriptedOp);
                    ;
                    if (scriptMode == null) {
                        scriptMode = scriptedOpSettings.get(scriptedOp);
                    }
                    if (scriptMode == null) {
                        scriptMode = scriptSourceSettings.get(scriptType);
                    }
                    if (scriptMode == null) {
                        scriptMode = DEFAULT_SCRIPT_MODES.get(scriptType);
                    }

                    for (String lang : scriptEngineService.types()) {
                        switch (scriptMode) {
                            case ENABLE:
                                assertCompileAccepted(lang, script, scriptType, scriptedOp);
                                break;
                            case DISABLE:
                                assertCompileRejected(lang, script, scriptType, scriptedOp);
                                break;
                            case SANDBOX:
                                if (scriptEngineService.sandboxed()) {
                                    assertCompileAccepted(lang, script, scriptType, scriptedOp);
                                } else {
                                    assertCompileRejected(lang, script, scriptType, scriptedOp);
                                }
                                break;
                        }
                    }
                }
            }
        }
    }

    private void createFileScripts(String... langs) throws IOException {
        for (String lang : langs) {
            File script = new File(scriptsFile, "file_script." + lang);
            Streams.copy("10".getBytes("UTF-8"), script);
        }
        resourceWatcherService.notifyNow();
    }

    private void assertCompileRejected(String lang, String script, ScriptType scriptType, ScriptedOp scriptedOp) {
        try {
            scriptService.compile(lang, script, scriptType, scriptedOp);
            fail("compile should have been rejected for lang [" + lang + "], script_type [" + scriptType + "], scripted_op [" + scriptedOp + "]");
        } catch(ScriptException e) {
            //all good
        }
    }

    private void assertCompileAccepted(String lang, String script, ScriptType scriptType, ScriptedOp scriptedOp) {
        assertThat(scriptService.compile(lang, script, scriptType, scriptedOp), notNullValue());
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
