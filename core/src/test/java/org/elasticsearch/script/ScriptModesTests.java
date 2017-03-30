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

import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.script.ScriptPermitsTests.MockTemplateBackend;
import org.elasticsearch.search.lookup.SearchLookup;
import org.elasticsearch.test.ESTestCase;
import org.junit.After;
import org.junit.Before;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static java.util.Collections.emptyMap;
import static java.util.Collections.unmodifiableMap;
import static org.elasticsearch.common.util.set.Sets.newHashSet;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.Matchers.containsString;

public class ScriptModesTests extends ESTestCase {
    ScriptSettings scriptSettings;
    ScriptContextRegistry scriptContextRegistry;
    private ScriptContext[] scriptContexts;
    private Map<String, ScriptEngineService> scriptEngines;
    private ScriptModes scriptModes;
    private Set<String> checkedSettings;
    private boolean assertAllSettingsWereChecked;
    private boolean assertScriptModesNonNull;

    @Before
    public void setupScriptEngines() {
        //randomly register custom script contexts
        int randomInt = randomIntBetween(0, 3);
        //prevent duplicates using map
        Map<String, ScriptContext.Plugin> contexts = new HashMap<>();
        for (int i = 0; i < randomInt; i++) {
            String plugin = randomAsciiOfLength(randomIntBetween(1, 10));
            String operation = randomAsciiOfLength(randomIntBetween(1, 30));
            String context = plugin + "-" + operation;
            contexts.put(context, new ScriptContext.Plugin(plugin, operation));
        }
        scriptContextRegistry = new ScriptContextRegistry(contexts.values());
        scriptContexts = scriptContextRegistry.scriptContexts().toArray(new ScriptContext[scriptContextRegistry.scriptContexts().size()]);
        scriptEngines = buildScriptEnginesByLangMap(newHashSet(
                //add the native engine just to make sure it gets filtered out
                new NativeScriptEngineService(Settings.EMPTY, emptyMap()),
                new CustomScriptEngineService()));
        ScriptEngineRegistry scriptEngineRegistry = new ScriptEngineRegistry(scriptEngines.values());
        scriptSettings = new ScriptSettings(scriptEngineRegistry, new MockTemplateBackend(),
                scriptContextRegistry);
        checkedSettings = new HashSet<>();
        assertAllSettingsWereChecked = true;
        assertScriptModesNonNull = true;
    }

    @After
    public void assertNativeScriptsAreAlwaysAllowed() {
        if (assertScriptModesNonNull) {
            assertThat(scriptModes.getScriptEnabled(NativeScriptEngineService.NAME, randomFrom(ScriptType.values()), randomFrom(scriptContexts)), equalTo(true));
        }
    }

    @After
    public void assertAllSettingsWereChecked() {
        if (assertScriptModesNonNull) {
            assertThat(scriptModes, notNullValue());
            int numberOfSettings = ScriptType.values().length * scriptContextRegistry.scriptContexts().size();
            numberOfSettings += 3; // for top-level inline/store/file settings
            numberOfSettings *= 2; // Once for the script engine, once for the template engine
            assertThat(scriptModes.scriptEnabled.size(), equalTo(numberOfSettings));
            if (assertAllSettingsWereChecked) {
                assertThat(checkedSettings.size(), equalTo(numberOfSettings));
            }
        }
    }

    public void testDefaultSettings() {
        this.scriptModes = new ScriptModes(scriptSettings, Settings.EMPTY);
        assertScriptModesAllOps("custom", true, ScriptType.FILE);
        assertScriptModesAllOps("custom", false, ScriptType.STORED, ScriptType.INLINE);
        assertScriptModesAllOps("mock_template", true, ScriptType.FILE, ScriptType.STORED,
                ScriptType.INLINE);
    }

    public void testMissingSetting() {
        assertAllSettingsWereChecked = false;
        this.scriptModes = new ScriptModes(scriptSettings, Settings.EMPTY);
        try {
            scriptModes.getScriptEnabled("non_existing", randomFrom(ScriptType.values()), randomFrom(scriptContexts));
            fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), containsString("not found for lang [non_existing]"));
        }
    }

    public void testScriptTypeGenericSettings() {
        int randomInt = randomIntBetween(1, ScriptType.values().length - 1);
        Set<ScriptType> randomScriptTypesSet = new HashSet<>();
        boolean[] randomScriptModes = new boolean[randomInt];
        for (int i = 0; i < randomInt; i++) {
            boolean added = false;
            while (added == false) {
                added = randomScriptTypesSet.add(randomFrom(ScriptType.values()));
            }
            randomScriptModes[i] = randomBoolean();
        }
        ScriptType[] randomScriptTypes = randomScriptTypesSet.toArray(new ScriptType[randomScriptTypesSet.size()]);
        Settings.Builder builder = Settings.builder();
        for (int i = 0; i < randomInt; i++) {
            builder.put("script" + "." + randomScriptTypes[i].getName(), randomScriptModes[i]);
        }
        this.scriptModes = new ScriptModes(scriptSettings, builder.build());

        for (int i = 0; i < randomInt; i++) {
            assertScriptModesAllOps("custom", randomScriptModes[i], randomScriptTypes[i]);
            assertScriptModesAllOps("mock_template", randomScriptModes[i], randomScriptTypes[i]);
        }
        if (randomScriptTypesSet.contains(ScriptType.FILE) == false) {
            assertScriptModesAllOps("custom", true, ScriptType.FILE);
            assertScriptModesAllOps("mock_template", true, ScriptType.FILE);
        }
        if (randomScriptTypesSet.contains(ScriptType.STORED) == false) {
            assertScriptModesAllOps("custom", false, ScriptType.STORED);
            assertScriptModesAllOps("mock_template", true, ScriptType.STORED);
        }
        if (randomScriptTypesSet.contains(ScriptType.INLINE) == false) {
            assertScriptModesAllOps("custom", false, ScriptType.INLINE);
            assertScriptModesAllOps("mock_template", true, ScriptType.INLINE);
        }
    }

    public void testScriptContextGenericSettings() {
        int randomInt = randomIntBetween(1, scriptContexts.length - 1);
        Set<ScriptContext> randomScriptContextsSet = new HashSet<>();
        boolean[] randomScriptModes = new boolean[randomInt];
        for (int i = 0; i < randomInt; i++) {
            boolean added = false;
            while (added == false) {
                added = randomScriptContextsSet.add(randomFrom(scriptContexts));
            }
            randomScriptModes[i] = randomBoolean();
        }
        ScriptContext[] randomScriptContexts = randomScriptContextsSet.toArray(new ScriptContext[randomScriptContextsSet.size()]);
        Settings.Builder builder = Settings.builder();
        for (int i = 0; i < randomInt; i++) {
            builder.put("script" + "." + randomScriptContexts[i].getKey(), randomScriptModes[i]);
        }
        this.scriptModes = new ScriptModes(scriptSettings, builder.build());

        for (int i = 0; i < randomInt; i++) {
            assertScriptModesAllTypes("custom", randomScriptModes[i], randomScriptContexts[i]);
            assertScriptModesAllTypes("mock_template", randomScriptModes[i], randomScriptContexts[i]);
        }

        ScriptContext[] complementOf = complementOf(randomScriptContexts);
        assertScriptModes("custom", true, new ScriptType[] {ScriptType.FILE}, complementOf);
        assertScriptModes("custom", false, new ScriptType[] {ScriptType.STORED, ScriptType.INLINE}, complementOf);
        assertScriptModes("mock_template", true, ScriptType.values(), complementOf);
    }

    public void testConflictingScriptTypeAndOpGenericSettings() {
        ScriptContext scriptContext = randomFrom(scriptContexts);
        Settings.Builder builder = Settings.builder()
                .put("script." + scriptContext.getKey(), "false")
                .put("script.stored", "true")
                .put("script.inline", "true");
        //operations generic settings have precedence over script type generic settings
        this.scriptModes = new ScriptModes(scriptSettings, builder.build());
        assertScriptModesAllTypes("custom", false, scriptContext);
        assertScriptModesAllTypes("mock_template", false, scriptContext);
        ScriptContext[] complementOf = complementOf(scriptContext);
        assertScriptModes("custom", true, new ScriptType[] {ScriptType.FILE, ScriptType.STORED}, complementOf);
        assertScriptModes("custom", true, new ScriptType[] {ScriptType.INLINE}, complementOf);
        assertScriptModes("mock_template", true, ScriptType.values(), complementOf);
    }

    private void assertScriptModesAllOps(String lang, boolean expectedScriptEnabled,
            ScriptType... scriptTypes) {
        assertScriptModes(lang, expectedScriptEnabled, scriptTypes, scriptContexts);
    }

    private void assertScriptModesAllTypes(String lang, boolean expectedScriptEnabled,
            ScriptContext... scriptContexts) {
        assertScriptModes(lang, expectedScriptEnabled, ScriptType.values(), scriptContexts);
    }

    private void assertScriptModes(String lang, boolean expectedScriptEnabled,
            ScriptType[] scriptTypes, ScriptContext... scriptContexts) {
        assert scriptTypes.length > 0;
        assert scriptContexts.length > 0;
        for (ScriptType scriptType : scriptTypes) {
            checkedSettings.add("script.engine." + lang + "." + scriptType);
            for (ScriptContext scriptContext : scriptContexts) {
                assertEquals(lang + "." + scriptType + "." + scriptContext.getKey()
                                + " doesn't have the expected value",
                        expectedScriptEnabled,
                        scriptModes.getScriptEnabled(lang, scriptType, scriptContext));
                checkedSettings.add(lang + "." + scriptType + "." + scriptContext);
            }
        }
    }

    private ScriptContext[] complementOf(ScriptContext... scriptContexts) {
        Map<String, ScriptContext> copy = new HashMap<>();
        for (ScriptContext scriptContext : scriptContextRegistry.scriptContexts()) {
            copy.put(scriptContext.getKey(), scriptContext);
        }
        for (ScriptContext scriptContext : scriptContexts) {
            copy.remove(scriptContext.getKey());
        }
        return copy.values().toArray(new ScriptContext[copy.size()]);
    }

    static Map<String, ScriptEngineService> buildScriptEnginesByLangMap(Set<ScriptEngineService> scriptEngines) {
        Map<String, ScriptEngineService> builder = new HashMap<>();
        for (ScriptEngineService scriptEngine : scriptEngines) {
            String type = scriptEngine.getType();
            builder.put(type, scriptEngine);
        }
        return unmodifiableMap(builder);
    }

    private static class CustomScriptEngineService implements ScriptEngineService {

        public static final String NAME = "custom";

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
            return null;
        }

        @Override
        public ExecutableScript executable(CompiledScript compiledScript, @Nullable Map<String, Object> vars) {
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
