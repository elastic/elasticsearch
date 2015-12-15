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

import static java.util.Collections.singleton;
import static java.util.Collections.unmodifiableMap;
import static java.util.Collections.unmodifiableSet;
import static org.elasticsearch.common.util.set.Sets.newHashSet;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.Matchers.containsString;

// TODO: this needs to be a base test class, and all scripting engines extend it
public class ScriptModesTests extends ESTestCase {
    private static final Set<String> ALL_LANGS = unmodifiableSet(
            newHashSet("custom", "test"));

    static final String[] ENABLE_VALUES = new String[]{"on", "true", "yes", "1"};
    static final String[] DISABLE_VALUES = new String[]{"off", "false", "no", "0"};

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
        checkedSettings = new HashSet<>();
        assertAllSettingsWereChecked = true;
        assertScriptModesNonNull = true;
    }

    @After
    public void assertNativeScriptsAreAlwaysAllowed() {
        if (assertScriptModesNonNull) {
            assertThat(scriptModes.getScriptMode(NativeScriptEngineService.NAME, randomFrom(ScriptType.values()), randomFrom(scriptContexts)), equalTo(ScriptMode.ON));
        }
    }

    @After
    public void assertAllSettingsWereChecked() {
        if (assertScriptModesNonNull) {
            assertThat(scriptModes, notNullValue());
            //2 is the number of engines (native excluded), custom is counted twice though as it's associated with two different names
            int numberOfSettings = 2 * ScriptType.values().length * scriptContextRegistry.scriptContexts().size();
            assertThat(scriptModes.scriptModes.size(), equalTo(numberOfSettings));
            if (assertAllSettingsWereChecked) {
                assertThat(checkedSettings.size(), equalTo(numberOfSettings));
            }
        }
    }

    public void testDefaultSettings() {
        this.scriptModes = new ScriptModes(scriptEngines, scriptContextRegistry, Settings.EMPTY);
        assertScriptModesAllOps(ScriptMode.ON, ALL_LANGS, ScriptType.FILE);
        assertScriptModesAllOps(ScriptMode.SANDBOX, ALL_LANGS, ScriptType.INDEXED, ScriptType.INLINE);
    }

    public void testMissingSetting() {
        assertAllSettingsWereChecked = false;
        this.scriptModes = new ScriptModes(scriptEngines, scriptContextRegistry, Settings.EMPTY);
        try {
            scriptModes.getScriptMode("non_existing", randomFrom(ScriptType.values()), randomFrom(scriptContexts));
            fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), containsString("not found for lang [non_existing]"));
        }
    }

    public void testScriptTypeGenericSettings() {
        int randomInt = randomIntBetween(1, ScriptType.values().length - 1);
        Set<ScriptType> randomScriptTypesSet = new HashSet<>();
        ScriptMode[] randomScriptModes = new ScriptMode[randomInt];
        for (int i = 0; i < randomInt; i++) {
            boolean added = false;
            while (added == false) {
                added = randomScriptTypesSet.add(randomFrom(ScriptType.values()));
            }
            randomScriptModes[i] = randomFrom(ScriptMode.values());
        }
        ScriptType[] randomScriptTypes = randomScriptTypesSet.toArray(new ScriptType[randomScriptTypesSet.size()]);
        Settings.Builder builder = Settings.builder();
        for (int i = 0; i < randomInt; i++) {
            builder.put(ScriptModes.SCRIPT_SETTINGS_PREFIX + randomScriptTypes[i], randomScriptModes[i]);
        }
        this.scriptModes = new ScriptModes(scriptEngines, scriptContextRegistry, builder.build());

        for (int i = 0; i < randomInt; i++) {
            assertScriptModesAllOps(randomScriptModes[i], ALL_LANGS, randomScriptTypes[i]);
        }
        if (randomScriptTypesSet.contains(ScriptType.FILE) == false) {
            assertScriptModesAllOps(ScriptMode.ON, ALL_LANGS, ScriptType.FILE);
        }
        if (randomScriptTypesSet.contains(ScriptType.INDEXED) == false) {
            assertScriptModesAllOps(ScriptMode.SANDBOX, ALL_LANGS, ScriptType.INDEXED);
        }
        if (randomScriptTypesSet.contains(ScriptType.INLINE) == false) {
            assertScriptModesAllOps(ScriptMode.SANDBOX, ALL_LANGS, ScriptType.INLINE);
        }
    }

    public void testScriptContextGenericSettings() {
        int randomInt = randomIntBetween(1, scriptContexts.length - 1);
        Set<ScriptContext> randomScriptContextsSet = new HashSet<>();
        ScriptMode[] randomScriptModes = new ScriptMode[randomInt];
        for (int i = 0; i < randomInt; i++) {
            boolean added = false;
            while (added == false) {
                added = randomScriptContextsSet.add(randomFrom(scriptContexts));
            }
            randomScriptModes[i] = randomFrom(ScriptMode.values());
        }
        ScriptContext[] randomScriptContexts = randomScriptContextsSet.toArray(new ScriptContext[randomScriptContextsSet.size()]);
        Settings.Builder builder = Settings.builder();
        for (int i = 0; i < randomInt; i++) {
            builder.put(ScriptModes.SCRIPT_SETTINGS_PREFIX + randomScriptContexts[i].getKey(), randomScriptModes[i]);
        }
        this.scriptModes = new ScriptModes(scriptEngines, scriptContextRegistry, builder.build());

        for (int i = 0; i < randomInt; i++) {
            assertScriptModesAllTypes(randomScriptModes[i], ALL_LANGS, randomScriptContexts[i]);
        }

        ScriptContext[] complementOf = complementOf(randomScriptContexts);
        assertScriptModes(ScriptMode.ON, ALL_LANGS, new ScriptType[]{ScriptType.FILE}, complementOf);
        assertScriptModes(ScriptMode.SANDBOX, ALL_LANGS, new ScriptType[]{ScriptType.INDEXED, ScriptType.INLINE}, complementOf);
    }

    public void testConflictingScriptTypeAndOpGenericSettings() {
        ScriptContext scriptContext = randomFrom(scriptContexts);
        Settings.Builder builder = Settings.builder().put(ScriptModes.SCRIPT_SETTINGS_PREFIX + scriptContext.getKey(), randomFrom(DISABLE_VALUES))
                .put("script.indexed", randomFrom(ENABLE_VALUES)).put("script.inline", ScriptMode.SANDBOX);
        //operations generic settings have precedence over script type generic settings
        this.scriptModes = new ScriptModes(scriptEngines, scriptContextRegistry, builder.build());
        assertScriptModesAllTypes(ScriptMode.OFF, ALL_LANGS, scriptContext);
        ScriptContext[] complementOf = complementOf(scriptContext);
        assertScriptModes(ScriptMode.ON, ALL_LANGS, new ScriptType[]{ScriptType.FILE, ScriptType.INDEXED}, complementOf);
        assertScriptModes(ScriptMode.SANDBOX, ALL_LANGS, new ScriptType[]{ScriptType.INLINE}, complementOf);
    }

    private void assertScriptModesAllOps(ScriptMode expectedScriptMode, Set<String> langs, ScriptType... scriptTypes) {
        assertScriptModes(expectedScriptMode, langs, scriptTypes, scriptContexts);
    }

    private void assertScriptModesAllTypes(ScriptMode expectedScriptMode, Set<String> langs, ScriptContext... scriptContexts) {
        assertScriptModes(expectedScriptMode, langs, ScriptType.values(), scriptContexts);
    }

    private void assertScriptModes(ScriptMode expectedScriptMode, Set<String> langs, ScriptType[] scriptTypes, ScriptContext... scriptContexts) {
        assert langs.size() > 0;
        assert scriptTypes.length > 0;
        assert scriptContexts.length > 0;
        for (String lang : langs) {
            for (ScriptType scriptType : scriptTypes) {
                for (ScriptContext scriptContext : scriptContexts) {
                    assertThat(lang + "." + scriptType + "." + scriptContext.getKey() + " doesn't have the expected value", scriptModes.getScriptMode(lang, scriptType, scriptContext), equalTo(expectedScriptMode));
                    checkedSettings.add(lang + "." + scriptType + "." + scriptContext);
                }
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

    private static String specificEngineOpSettings(String lang, ScriptType scriptType, ScriptContext scriptContext) {
        return ScriptModes.ENGINE_SETTINGS_PREFIX + "." + lang + "." + scriptType + "." + scriptContext.getKey();
    }

    static Map<String, ScriptEngineService> buildScriptEnginesByLangMap(Set<ScriptEngineService> scriptEngines) {
        Map<String, ScriptEngineService> builder = new HashMap<>();
        for (ScriptEngineService scriptEngine : scriptEngines) {
            for (String type : scriptEngine.types()) {
                builder.put(type, scriptEngine);
            }
        }
        return unmodifiableMap(builder);
    }

    private static class CustomScriptEngineService implements ScriptEngineService {
        @Override
        public String[] types() {
            return new String[]{"custom", "test"};
        }

        @Override
        public String[] extensions() {
            return new String[0];
        }

        @Override
        public boolean sandboxed() {
            return false;
        }

        @Override
        public Object compile(String script) {
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
