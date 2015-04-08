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

import com.google.common.collect.*;
import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.script.ScriptService.ScriptType;
import org.elasticsearch.script.expression.ExpressionScriptEngineService;
import org.elasticsearch.script.groovy.GroovyScriptEngineService;
import org.elasticsearch.script.mustache.MustacheScriptEngineService;
import org.elasticsearch.search.lookup.SearchLookup;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.*;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;

public class ScriptModesTests extends ElasticsearchTestCase {

    private static final Set<String> ALL_LANGS = ImmutableSet.of(GroovyScriptEngineService.NAME, MustacheScriptEngineService.NAME, ExpressionScriptEngineService.NAME, "custom", "test");

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
        Map<String, ScriptContext.Plugin> contexts = Maps.newHashMap();
        for (int i = 0; i < randomInt; i++) {
            String plugin = randomAsciiOfLength(randomIntBetween(1, 10));
            String operation = randomAsciiOfLength(randomIntBetween(1, 30));
            String context = plugin + "-" + operation;
            contexts.put(context, new ScriptContext.Plugin(plugin, operation));
        }
        scriptContextRegistry = new ScriptContextRegistry(contexts.values());
        scriptContexts = scriptContextRegistry.scriptContexts().toArray(new ScriptContext[scriptContextRegistry.scriptContexts().size()]);
        scriptEngines = buildScriptEnginesByLangMap(ImmutableSet.of(
                new GroovyScriptEngineService(ImmutableSettings.EMPTY),
                new MustacheScriptEngineService(ImmutableSettings.EMPTY),
                new ExpressionScriptEngineService(ImmutableSettings.EMPTY),
                //add the native engine just to make sure it gets filtered out
                new NativeScriptEngineService(ImmutableSettings.EMPTY, Collections.<String, NativeScriptFactory>emptyMap()),
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
            //4 is the number of engines (native excluded), custom is counted twice though as it's associated with two different names
            int numberOfSettings = 5 * ScriptType.values().length * scriptContextRegistry.scriptContexts().size();
            assertThat(scriptModes.scriptModes.size(), equalTo(numberOfSettings));
            if (assertAllSettingsWereChecked) {
                assertThat(checkedSettings.size(), equalTo(numberOfSettings));
            }
        }
    }

    @Test
    public void testDefaultSettings() {
        this.scriptModes = new ScriptModes(scriptEngines, scriptContextRegistry, ImmutableSettings.EMPTY);
        assertScriptModesAllOps(ScriptMode.ON, ALL_LANGS, ScriptType.FILE);
        assertScriptModesAllOps(ScriptMode.SANDBOX, ALL_LANGS, ScriptType.INDEXED, ScriptType.INLINE);
    }

    @Test(expected = ElasticsearchIllegalArgumentException.class)
    public void testMissingSetting() {
        assertAllSettingsWereChecked = false;
        this.scriptModes = new ScriptModes(scriptEngines, scriptContextRegistry, ImmutableSettings.EMPTY);
        scriptModes.getScriptMode("non_existing", randomFrom(ScriptType.values()), randomFrom(scriptContexts));
    }

    @Test
    public void testScriptTypeGenericSettings() {
        int randomInt = randomIntBetween(1, ScriptType.values().length - 1);
        Set<ScriptType> randomScriptTypesSet = Sets.newHashSet();
        ScriptMode[] randomScriptModes = new ScriptMode[randomInt];
        for (int i = 0; i < randomInt; i++) {
            boolean added = false;
            while (added == false) {
                added = randomScriptTypesSet.add(randomFrom(ScriptType.values()));
            }
            randomScriptModes[i] = randomFrom(ScriptMode.values());
        }
        ScriptType[] randomScriptTypes = randomScriptTypesSet.toArray(new ScriptType[randomScriptTypesSet.size()]);
        ImmutableSettings.Builder builder = ImmutableSettings.builder();
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

    @Test
    public void testScriptContextGenericSettings() {
        int randomInt = randomIntBetween(1, scriptContexts.length - 1);
        Set<ScriptContext> randomScriptContextsSet = Sets.newHashSet();
        ScriptMode[] randomScriptModes = new ScriptMode[randomInt];
        for (int i = 0; i < randomInt; i++) {
            boolean added = false;
            while (added == false) {
                added = randomScriptContextsSet.add(randomFrom(scriptContexts));
            }
            randomScriptModes[i] = randomFrom(ScriptMode.values());
        }
        ScriptContext[] randomScriptContexts = randomScriptContextsSet.toArray(new ScriptContext[randomScriptContextsSet.size()]);
        ImmutableSettings.Builder builder = ImmutableSettings.builder();
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

    @Test
    public void testConflictingScriptTypeAndOpGenericSettings() {
        ScriptContext scriptContext = randomFrom(scriptContexts);
        ImmutableSettings.Builder builder = ImmutableSettings.builder().put(ScriptModes.SCRIPT_SETTINGS_PREFIX + scriptContext.getKey(), randomFrom(DISABLE_VALUES))
                .put("script.indexed", randomFrom(ENABLE_VALUES)).put("script.inline", ScriptMode.SANDBOX);
        //operations generic settings have precedence over script type generic settings
        this.scriptModes = new ScriptModes(scriptEngines, scriptContextRegistry, builder.build());
        assertScriptModesAllTypes(ScriptMode.OFF, ALL_LANGS, scriptContext);
        ScriptContext[] complementOf = complementOf(scriptContext);
        assertScriptModes(ScriptMode.ON, ALL_LANGS, new ScriptType[]{ScriptType.FILE, ScriptType.INDEXED}, complementOf);
        assertScriptModes(ScriptMode.SANDBOX, ALL_LANGS, new ScriptType[]{ScriptType.INLINE}, complementOf);
    }

    @Test
    public void testEngineSpecificSettings() {
        ImmutableSettings.Builder builder = ImmutableSettings.builder()
                .put(specificEngineOpSettings(GroovyScriptEngineService.NAME, ScriptType.INLINE, ScriptContext.Standard.MAPPING), randomFrom(DISABLE_VALUES))
                .put(specificEngineOpSettings(GroovyScriptEngineService.NAME, ScriptType.INLINE, ScriptContext.Standard.UPDATE), randomFrom(DISABLE_VALUES));
        ImmutableSet<String> groovyLangSet = ImmutableSet.of(GroovyScriptEngineService.NAME);
        Set<String> allButGroovyLangSet = new HashSet<>(ALL_LANGS);
        allButGroovyLangSet.remove(GroovyScriptEngineService.NAME);
        this.scriptModes = new ScriptModes(scriptEngines, scriptContextRegistry, builder.build());
        assertScriptModes(ScriptMode.OFF, groovyLangSet, new ScriptType[]{ScriptType.INLINE}, ScriptContext.Standard.MAPPING, ScriptContext.Standard.UPDATE);
        assertScriptModes(ScriptMode.SANDBOX, groovyLangSet, new ScriptType[]{ScriptType.INLINE}, complementOf(ScriptContext.Standard.MAPPING, ScriptContext.Standard.UPDATE));
        assertScriptModesAllOps(ScriptMode.SANDBOX, allButGroovyLangSet, ScriptType.INLINE);
        assertScriptModesAllOps(ScriptMode.SANDBOX, ALL_LANGS, ScriptType.INDEXED);
        assertScriptModesAllOps(ScriptMode.ON, ALL_LANGS, ScriptType.FILE);
    }

    @Test
    public void testInteractionBetweenGenericAndEngineSpecificSettings() {
        ImmutableSettings.Builder builder = ImmutableSettings.builder().put("script.inline", randomFrom(DISABLE_VALUES))
                .put(specificEngineOpSettings(MustacheScriptEngineService.NAME, ScriptType.INLINE, ScriptContext.Standard.AGGS), randomFrom(ENABLE_VALUES))
                .put(specificEngineOpSettings(MustacheScriptEngineService.NAME, ScriptType.INLINE, ScriptContext.Standard.SEARCH), randomFrom(ENABLE_VALUES));
        ImmutableSet<String> mustacheLangSet = ImmutableSet.of(MustacheScriptEngineService.NAME);
        Set<String> allButMustacheLangSet = new HashSet<>(ALL_LANGS);
        allButMustacheLangSet.remove(MustacheScriptEngineService.NAME);
        this.scriptModes = new ScriptModes(scriptEngines, scriptContextRegistry, builder.build());
        assertScriptModes(ScriptMode.ON, mustacheLangSet, new ScriptType[]{ScriptType.INLINE}, ScriptContext.Standard.AGGS, ScriptContext.Standard.SEARCH);
        assertScriptModes(ScriptMode.OFF, mustacheLangSet, new ScriptType[]{ScriptType.INLINE}, complementOf(ScriptContext.Standard.AGGS, ScriptContext.Standard.SEARCH));
        assertScriptModesAllOps(ScriptMode.OFF, allButMustacheLangSet, ScriptType.INLINE);
        assertScriptModesAllOps(ScriptMode.SANDBOX, ALL_LANGS, ScriptType.INDEXED);
        assertScriptModesAllOps(ScriptMode.ON, ALL_LANGS, ScriptType.FILE);
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
        Map<String, ScriptContext> copy = Maps.newHashMap();
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

    static ImmutableMap<String, ScriptEngineService> buildScriptEnginesByLangMap(Set<ScriptEngineService> scriptEngines) {
        ImmutableMap.Builder<String, ScriptEngineService> builder = ImmutableMap.builder();
        for (ScriptEngineService scriptEngine : scriptEngines) {
            for (String type : scriptEngine.types()) {
                builder.put(type, scriptEngine);
            }
        }
        return builder.build();
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
        public ExecutableScript executable(Object compiledScript, @Nullable Map<String, Object> vars) {
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
        public void scriptRemoved(@Nullable CompiledScript script) {

        }
    }
}
