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
import org.elasticsearch.script.ScriptService.ScriptType;
import org.elasticsearch.search.lookup.SearchLookup;
import org.elasticsearch.test.ESTestCase;
import org.junit.After;
import org.junit.Before;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static java.util.Collections.unmodifiableMap;
import static org.elasticsearch.common.util.set.Sets.newHashSet;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.Matchers.containsString;

// TODO: this needs to be a base test class, and all scripting engines extend it
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
                new NativeScriptEngineService(Settings.EMPTY, Collections.<String, NativeScriptFactory>emptyMap()),
                new CustomScriptEngineService()));
        ScriptEngineRegistry scriptEngineRegistry = new ScriptEngineRegistry(scriptEngines.values());
        scriptSettings = new ScriptSettings(scriptEngineRegistry, scriptContextRegistry);
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
            assertThat(scriptModes.scriptEnabled.size(), equalTo(numberOfSettings));
            if (assertAllSettingsWereChecked) {
                assertThat(checkedSettings.size(), equalTo(numberOfSettings));
            }
        }
    }

    public void testDefaultSettings() {
        this.scriptModes = new ScriptModes(scriptSettings, Settings.EMPTY);
        assertScriptModesAllOps(true, ScriptType.FILE);
        assertScriptModesAllOps(false, ScriptType.STORED, ScriptType.INLINE);
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
            builder.put("script" + "." + randomScriptTypes[i].getScriptType(), randomScriptModes[i]);
        }
        this.scriptModes = new ScriptModes(scriptSettings, builder.build());

        for (int i = 0; i < randomInt; i++) {
            assertScriptModesAllOps(randomScriptModes[i], randomScriptTypes[i]);
        }
        if (randomScriptTypesSet.contains(ScriptType.FILE) == false) {
            assertScriptModesAllOps(true, ScriptType.FILE);
        }
        if (randomScriptTypesSet.contains(ScriptType.STORED) == false) {
            assertScriptModesAllOps(false, ScriptType.STORED);
        }
        if (randomScriptTypesSet.contains(ScriptType.INLINE) == false) {
            assertScriptModesAllOps(false, ScriptType.INLINE);
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
            assertScriptModesAllTypes(randomScriptModes[i], randomScriptContexts[i]);
        }

        ScriptContext[] complementOf = complementOf(randomScriptContexts);
        assertScriptModes(true, new ScriptType[]{ScriptType.FILE}, complementOf);
        assertScriptModes(false, new ScriptType[]{ScriptType.STORED, ScriptType.INLINE}, complementOf);
    }

    public void testConflictingScriptTypeAndOpGenericSettings() {
        ScriptContext scriptContext = randomFrom(scriptContexts);
        Settings.Builder builder = Settings.builder()
                .put("script." + scriptContext.getKey(), "false")
                .put("script.stored", "true")
                .put("script.inline", "true");
        //operations generic settings have precedence over script type generic settings
        this.scriptModes = new ScriptModes(scriptSettings, builder.build());
        assertScriptModesAllTypes(false, scriptContext);
        ScriptContext[] complementOf = complementOf(scriptContext);
        assertScriptModes(true, new ScriptType[]{ScriptType.FILE, ScriptType.STORED}, complementOf);
        assertScriptModes(true, new ScriptType[]{ScriptType.INLINE}, complementOf);
    }

    private void assertScriptModesAllOps(boolean expectedScriptEnabled, ScriptType... scriptTypes) {
        assertScriptModes(expectedScriptEnabled, scriptTypes, scriptContexts);
    }

    private void assertScriptModesAllTypes(boolean expectedScriptEnabled, ScriptContext... scriptContexts) {
        assertScriptModes(expectedScriptEnabled, ScriptType.values(), scriptContexts);
    }

    private void assertScriptModes(boolean expectedScriptEnabled, ScriptType[] scriptTypes, ScriptContext... scriptContexts) {
        assert scriptTypes.length > 0;
        assert scriptContexts.length > 0;
        for (ScriptType scriptType : scriptTypes) {
            checkedSettings.add("script.engine.custom." + scriptType);
            for (ScriptContext scriptContext : scriptContexts) {
                assertThat("custom." + scriptType + "." + scriptContext.getKey() + " doesn't have the expected value",
                        scriptModes.getScriptEnabled("custom", scriptType, scriptContext), equalTo(expectedScriptEnabled));
                checkedSettings.add("custom." + scriptType + "." + scriptContext);
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

        @Override
        public void scriptRemoved(@Nullable CompiledScript script) {

        }
    }
}
