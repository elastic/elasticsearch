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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
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

public class ScriptModesTests extends ElasticsearchTestCase {

    private static final Set<String> ALL_LANGS = ImmutableSet.of(GroovyScriptEngineService.NAME, MustacheScriptEngineService.NAME, ExpressionScriptEngineService.NAME, "custom", "test");
    
    private Map<String, ScriptEngineService> scriptEngines;
    private ScriptModes scriptModes;
    private Set<String> checkedSettings;
    private boolean assertAllSettingsWereChecked;

    @Before
    public void setupScriptEngines() {
        scriptEngines = buildScriptEnginesByLangMap(ImmutableSet.of(
                new GroovyScriptEngineService(ImmutableSettings.EMPTY),
                new MustacheScriptEngineService(ImmutableSettings.EMPTY),
                new ExpressionScriptEngineService(ImmutableSettings.EMPTY),
                //add the native engine just to make sure it gets filtered out
                new NativeScriptEngineService(ImmutableSettings.EMPTY, Collections.<String, NativeScriptFactory>emptyMap()),
                new CustomScriptEngineService()));
        checkedSettings = new HashSet<>();
        assertAllSettingsWereChecked = true;
    }

    @After
    public void assertNativeScriptsAreAlwaysAllowed() {
        assertThat(scriptModes.getScriptMode(NativeScriptEngineService.NAME, randomFrom(ScriptType.values()), randomFrom(ScriptedOp.values())), equalTo(ScriptMode.ENABLE));
    }

    @After
    public void assertAllSettingsWereChecked() {
        //4 is the number of engines (native excluded), custom is counted twice though as it's associated with two different names
        int numberOfSettings = 5 * ScriptType.values().length * ScriptedOp.values().length;
        assertThat(scriptModes.scriptModes.size(), equalTo(numberOfSettings));
        if (assertAllSettingsWereChecked) {
            assertThat(checkedSettings.size(), equalTo(numberOfSettings));
        }
    }

    @Test
    public void testDefaultSettings() {
        this.scriptModes = new ScriptModes(scriptEngines, ImmutableSettings.EMPTY);
        assertScriptModesAllOps(ScriptMode.ENABLE, ALL_LANGS, ScriptType.FILE);
        assertScriptModesAllOps(ScriptMode.SANDBOX, ALL_LANGS, ScriptType.INDEXED, ScriptType.INLINE);
    }

    @Test
    public void testDefaultSettingsDisableDynamicTrue() {
        //verify that disable_dynamic setting gets still read and applied, iff new settings are not present
        this.scriptModes = new ScriptModes(scriptEngines, ImmutableSettings.builder().put(ScriptService.DISABLE_DYNAMIC_SCRIPTING_SETTING, randomFrom("true", "all")).build());
        assertScriptModesAllOps(ScriptMode.ENABLE, ALL_LANGS, ScriptType.FILE);
        assertScriptModesAllOps(ScriptMode.DISABLE, ALL_LANGS, ScriptType.INDEXED, ScriptType.INLINE);
    }

    @Test
    public void testDefaultSettingsEnableDynamicFalse() {
        //verify that disable_dynamic setting gets still read and applied, iff new settings are not present
        this.scriptModes = new ScriptModes(scriptEngines, ImmutableSettings.builder().put(ScriptService.DISABLE_DYNAMIC_SCRIPTING_SETTING, randomFrom("false", "none")).build());
        assertScriptModesAllOps(ScriptMode.ENABLE, ALL_LANGS, ScriptType.FILE, ScriptType.INDEXED, ScriptType.INLINE);
    }

    @Test
    public void testDefaultSettingsDisableDynamicSandbox() {
        //verify that disable_dynamic setting gets still read and applied, iff new settings are not present
        this.scriptModes = new ScriptModes(scriptEngines, ImmutableSettings.builder().put(ScriptService.DISABLE_DYNAMIC_SCRIPTING_SETTING, randomFrom(SANDBOX.values())).build());
        assertScriptModesAllOps(ScriptMode.ENABLE, ALL_LANGS, ScriptType.FILE);
        assertScriptModesAllOps(ScriptMode.SANDBOX, ALL_LANGS, ScriptType.INDEXED, ScriptType.INLINE);
    }

    @Test(expected = ElasticsearchIllegalArgumentException.class)
    public void testMissingSetting() {
        assertAllSettingsWereChecked = false;
        this.scriptModes = new ScriptModes(scriptEngines, ImmutableSettings.EMPTY);
        scriptModes.getScriptMode("non_existing", randomFrom(ScriptType.values()), randomFrom(ScriptedOp.values()));
    }

    @Test
    public void testEnableDynamicGenericSettings() {
        ImmutableSettings.Builder builder = ImmutableSettings.builder().put("script.dynamic", randomFrom(ENABLE.values()));
        rarelyAddDisableDynamicSetting(builder);
        this.scriptModes = new ScriptModes(scriptEngines, builder.build());
        assertScriptModesAllOps(ScriptMode.ENABLE, ALL_LANGS, ScriptType.FILE, ScriptType.INLINE);
        assertScriptModesAllOps(ScriptMode.SANDBOX, ALL_LANGS, ScriptType.INDEXED);
    }

    @Test
    public void testDisableDynamicGenericSettings() {
        ImmutableSettings.Builder builder = ImmutableSettings.builder().put("script.dynamic", randomFrom(DISABLE.values()));
        rarelyAddDisableDynamicSetting(builder);
        this.scriptModes = new ScriptModes(scriptEngines, builder.build());
        assertScriptModesAllOps(ScriptMode.ENABLE, ALL_LANGS, ScriptType.FILE);
        assertScriptModesAllOps(ScriptMode.SANDBOX, ALL_LANGS, ScriptType.INDEXED);
        assertScriptModesAllOps(ScriptMode.DISABLE, ALL_LANGS, ScriptType.INLINE);
    }

    @Test
    public void testSandboxDynamicGenericSettings() {
        ImmutableSettings.Builder builder = ImmutableSettings.builder().put("script.dynamic", randomFrom(SANDBOX.values()));
        rarelyAddDisableDynamicSetting(builder);
        //nothing changes if setting set is same as default
        this.scriptModes = new ScriptModes(scriptEngines, builder.build());
        assertScriptModesAllOps(ScriptMode.ENABLE, ALL_LANGS, ScriptType.FILE);
        assertScriptModesAllOps(ScriptMode.SANDBOX, ALL_LANGS, ScriptType.INDEXED, ScriptType.INLINE);
    }

    @Test
    public void testEnableIndexedGenericSettings() {
        ImmutableSettings.Builder builder = ImmutableSettings.builder().put("script.indexed", randomFrom(ENABLE.values()));
        rarelyAddDisableDynamicSetting(builder);
        this.scriptModes = new ScriptModes(scriptEngines, builder.build());
        assertScriptModesAllOps(ScriptMode.ENABLE, ALL_LANGS, ScriptType.FILE, ScriptType.INDEXED);
        assertScriptModesAllOps(ScriptMode.SANDBOX, ALL_LANGS, ScriptType.INLINE);
    }

    @Test
    public void testDisableIndexedGenericSettings() {
        ImmutableSettings.Builder builder = ImmutableSettings.builder().put("script.indexed", randomFrom(DISABLE.values()));
        rarelyAddDisableDynamicSetting(builder);
        this.scriptModes = new ScriptModes(scriptEngines, builder.build());
        assertScriptModesAllOps(ScriptMode.ENABLE, ALL_LANGS, ScriptType.FILE);
        assertScriptModesAllOps(ScriptMode.DISABLE, ALL_LANGS, ScriptType.INDEXED);
        assertScriptModesAllOps(ScriptMode.SANDBOX, ALL_LANGS, ScriptType.INLINE);
    }

    @Test
    public void testSandboxIndexedGenericSettings() {
        ImmutableSettings.Builder builder = ImmutableSettings.builder().put("script.indexed", randomFrom(SANDBOX.values()));
        rarelyAddDisableDynamicSetting(builder);
        //nothing changes if setting set is same as default
        this.scriptModes = new ScriptModes(scriptEngines, builder.build());
        assertScriptModesAllOps(ScriptMode.ENABLE, ALL_LANGS, ScriptType.FILE);
        assertScriptModesAllOps(ScriptMode.SANDBOX, ALL_LANGS, ScriptType.INDEXED, ScriptType.INLINE);
    }

    @Test
    public void testEnableFileGenericSettings() {
        ImmutableSettings.Builder builder = ImmutableSettings.builder().put("script.file", randomFrom(ENABLE.values()));
        rarelyAddDisableDynamicSetting(builder);
        //nothing changes if setting set is same as default
        this.scriptModes = new ScriptModes(scriptEngines, builder.build());
        assertScriptModesAllOps(ScriptMode.ENABLE, ALL_LANGS, ScriptType.FILE);
        assertScriptModesAllOps(ScriptMode.SANDBOX, ALL_LANGS, ScriptType.INDEXED, ScriptType.INLINE);
    }

    @Test
    public void testDisableFileGenericSettings() {
        ImmutableSettings.Builder builder = ImmutableSettings.builder().put("script.file", randomFrom(DISABLE.values()));
        rarelyAddDisableDynamicSetting(builder);
        this.scriptModes = new ScriptModes(scriptEngines, builder.build());
        assertScriptModesAllOps(ScriptMode.DISABLE, ALL_LANGS, ScriptType.FILE);
        assertScriptModesAllOps(ScriptMode.SANDBOX, ALL_LANGS, ScriptType.INDEXED, ScriptType.INLINE);
    }

    @Test
    public void testSandboxFileGenericSettings() {
        ImmutableSettings.Builder builder = ImmutableSettings.builder().put("script.file", randomFrom(SANDBOX.values()));
        rarelyAddDisableDynamicSetting(builder);
        //nothing changes if setting set is same as default
        this.scriptModes = new ScriptModes(scriptEngines, builder.build());
        assertScriptModesAllOps(ScriptMode.SANDBOX, ALL_LANGS, ScriptType.FILE, ScriptType.INDEXED, ScriptType.INLINE);
    }

    @Test
    public void testMultipleScriptTypeGenericSettings() {
        ImmutableSettings.Builder builder = ImmutableSettings.builder().put("script.file", randomFrom(SANDBOX.values())).put("script.dynamic", randomFrom(DISABLE.values()));
        rarelyAddDisableDynamicSetting(builder);
        this.scriptModes = new ScriptModes(scriptEngines, builder.build());
        assertScriptModesAllOps(ScriptMode.SANDBOX, ALL_LANGS, ScriptType.FILE);
        assertScriptModesAllOps(ScriptMode.SANDBOX, ALL_LANGS, ScriptType.INDEXED);
        assertScriptModesAllOps(ScriptMode.DISABLE, ALL_LANGS, ScriptType.INLINE);
    }

    @Test
    public void testEnableMappingGenericSettings() {
        ImmutableSettings.Builder builder = ImmutableSettings.builder().put(randomGenericOpSettings(ScriptedOp.MAPPING), randomFrom(ENABLE.values()));
        rarelyAddDisableDynamicSetting(builder);
        this.scriptModes = new ScriptModes(scriptEngines, builder.build());
        assertScriptModesAllTypes(ScriptMode.ENABLE, ALL_LANGS, ScriptedOp.MAPPING);
        assertScriptModes(ScriptMode.ENABLE, ALL_LANGS, new ScriptType[]{ScriptType.FILE}, ScriptedOp.AGGS, ScriptedOp.SEARCH, ScriptedOp.UPDATE);
        assertScriptModes(ScriptMode.SANDBOX, ALL_LANGS, new ScriptType[]{ScriptType.INDEXED, ScriptType.INLINE}, ScriptedOp.AGGS, ScriptedOp.SEARCH, ScriptedOp.UPDATE);
    }

    @Test
    public void testDisableMappingGenericSettings() {
        ImmutableSettings.Builder builder = ImmutableSettings.builder().put(randomGenericOpSettings(ScriptedOp.MAPPING), randomFrom(DISABLE.values()));
        rarelyAddDisableDynamicSetting(builder);
        this.scriptModes = new ScriptModes(scriptEngines, builder.build());
        assertScriptModesAllTypes(ScriptMode.DISABLE, ALL_LANGS, ScriptedOp.MAPPING);
        assertScriptModes(ScriptMode.ENABLE, ALL_LANGS, new ScriptType[]{ScriptType.FILE}, ScriptedOp.AGGS, ScriptedOp.SEARCH, ScriptedOp.UPDATE);
        assertScriptModes(ScriptMode.SANDBOX, ALL_LANGS, new ScriptType[]{ScriptType.INDEXED, ScriptType.INLINE}, ScriptedOp.AGGS, ScriptedOp.SEARCH, ScriptedOp.UPDATE);
    }

    @Test
    public void testSandboxMappingGenericSettings() {
        ImmutableSettings.Builder builder = ImmutableSettings.builder().put(randomGenericOpSettings(ScriptedOp.MAPPING), randomFrom(SANDBOX.values()));
        this.scriptModes = new ScriptModes(scriptEngines, builder.build());
        assertScriptModesAllTypes(ScriptMode.SANDBOX, ALL_LANGS, ScriptedOp.MAPPING);
        assertScriptModes(ScriptMode.ENABLE, ALL_LANGS, new ScriptType[]{ScriptType.FILE}, ScriptedOp.AGGS, ScriptedOp.SEARCH, ScriptedOp.UPDATE);
        assertScriptModes(ScriptMode.SANDBOX, ALL_LANGS, new ScriptType[]{ScriptType.INDEXED, ScriptType.INLINE}, ScriptedOp.AGGS, ScriptedOp.SEARCH, ScriptedOp.UPDATE);
    }

    @Test
    public void testEnableSearchGenericSettings() {
        ImmutableSettings.Builder builder = ImmutableSettings.builder().put(randomGenericOpSettings(ScriptedOp.SEARCH), randomFrom(ENABLE.values()));
        rarelyAddDisableDynamicSetting(builder);
        this.scriptModes = new ScriptModes(scriptEngines, builder.build());
        assertScriptModesAllTypes(ScriptMode.ENABLE, ALL_LANGS, ScriptedOp.SEARCH);
        assertScriptModes(ScriptMode.ENABLE, ALL_LANGS, new ScriptType[]{ScriptType.FILE}, ScriptedOp.AGGS, ScriptedOp.MAPPING, ScriptedOp.UPDATE);
        assertScriptModes(ScriptMode.SANDBOX, ALL_LANGS, new ScriptType[]{ScriptType.INDEXED, ScriptType.INLINE}, ScriptedOp.AGGS, ScriptedOp.MAPPING, ScriptedOp.UPDATE);
    }

    @Test
    public void testDisableSearchGenericSettings() {
        ImmutableSettings.Builder builder = ImmutableSettings.builder().put(randomGenericOpSettings(ScriptedOp.SEARCH), randomFrom(DISABLE.values()));
        rarelyAddDisableDynamicSetting(builder);
        this.scriptModes = new ScriptModes(scriptEngines, builder.build());
        assertScriptModesAllTypes(ScriptMode.DISABLE, ALL_LANGS, ScriptedOp.SEARCH);
        assertScriptModes(ScriptMode.ENABLE, ALL_LANGS, new ScriptType[]{ScriptType.FILE}, ScriptedOp.AGGS, ScriptedOp.MAPPING, ScriptedOp.UPDATE);
        assertScriptModes(ScriptMode.SANDBOX, ALL_LANGS, new ScriptType[]{ScriptType.INDEXED, ScriptType.INLINE}, ScriptedOp.AGGS, ScriptedOp.MAPPING, ScriptedOp.UPDATE);
    }

    @Test
    public void testSandboxSearchGenericSettings() {
        ImmutableSettings.Builder builder = ImmutableSettings.builder().put(randomGenericOpSettings(ScriptedOp.SEARCH), randomFrom(SANDBOX.values()));
        rarelyAddDisableDynamicSetting(builder);
        this.scriptModes = new ScriptModes(scriptEngines, builder.build());
        assertScriptModesAllTypes(ScriptMode.SANDBOX, ALL_LANGS, ScriptedOp.SEARCH);
        assertScriptModes(ScriptMode.ENABLE, ALL_LANGS, new ScriptType[]{ScriptType.FILE}, ScriptedOp.AGGS, ScriptedOp.MAPPING, ScriptedOp.UPDATE);
        assertScriptModes(ScriptMode.SANDBOX, ALL_LANGS, new ScriptType[]{ScriptType.INDEXED, ScriptType.INLINE}, ScriptedOp.AGGS, ScriptedOp.MAPPING, ScriptedOp.UPDATE);
    }

    @Test
    public void testEnableAggsGenericSettings() {
        ImmutableSettings.Builder builder = ImmutableSettings.builder().put(randomGenericOpSettings(ScriptedOp.AGGS), randomFrom(ENABLE.values()));
        rarelyAddDisableDynamicSetting(builder);
        this.scriptModes = new ScriptModes(scriptEngines, builder.build());
        assertScriptModesAllTypes(ScriptMode.ENABLE, ALL_LANGS, ScriptedOp.AGGS);
        assertScriptModes(ScriptMode.ENABLE, ALL_LANGS, new ScriptType[]{ScriptType.FILE}, ScriptedOp.SEARCH, ScriptedOp.MAPPING, ScriptedOp.UPDATE);
        assertScriptModes(ScriptMode.SANDBOX, ALL_LANGS, new ScriptType[]{ScriptType.INDEXED, ScriptType.INLINE}, ScriptedOp.SEARCH, ScriptedOp.MAPPING, ScriptedOp.UPDATE);
    }

    @Test
    public void testDisableAggsGenericSettings() {
        ImmutableSettings.Builder builder = ImmutableSettings.builder().put(randomGenericOpSettings(ScriptedOp.AGGS), randomFrom(DISABLE.values()));
        rarelyAddDisableDynamicSetting(builder);
        this.scriptModes = new ScriptModes(scriptEngines, builder.build());
        assertScriptModesAllTypes(ScriptMode.DISABLE, ALL_LANGS, ScriptedOp.AGGS);
        assertScriptModes(ScriptMode.ENABLE, ALL_LANGS, new ScriptType[]{ScriptType.FILE}, ScriptedOp.SEARCH, ScriptedOp.MAPPING, ScriptedOp.UPDATE);
        assertScriptModes(ScriptMode.SANDBOX, ALL_LANGS, new ScriptType[]{ScriptType.INDEXED, ScriptType.INLINE}, ScriptedOp.SEARCH, ScriptedOp.MAPPING, ScriptedOp.UPDATE);
    }

    @Test
    public void testSandboxAggsGenericSettings() {
        ImmutableSettings.Builder builder = ImmutableSettings.builder().put(randomGenericOpSettings(ScriptedOp.AGGS), randomFrom(SANDBOX.values()));
        rarelyAddDisableDynamicSetting(builder);
        this.scriptModes = new ScriptModes(scriptEngines, builder.build());
        assertScriptModesAllTypes(ScriptMode.SANDBOX, ALL_LANGS, ScriptedOp.AGGS);
        assertScriptModes(ScriptMode.ENABLE, ALL_LANGS, new ScriptType[]{ScriptType.FILE}, ScriptedOp.SEARCH, ScriptedOp.MAPPING, ScriptedOp.UPDATE);
        assertScriptModes(ScriptMode.SANDBOX, ALL_LANGS, new ScriptType[]{ScriptType.INDEXED, ScriptType.INLINE}, ScriptedOp.SEARCH, ScriptedOp.MAPPING, ScriptedOp.UPDATE);
    }

    @Test
    public void testEnableUpdateGenericSettings() {
        ImmutableSettings.Builder builder = ImmutableSettings.builder().put(randomGenericOpSettings(ScriptedOp.UPDATE), randomFrom(ENABLE.values()));
        rarelyAddDisableDynamicSetting(builder);
        this.scriptModes = new ScriptModes(scriptEngines, builder.build());
        assertScriptModesAllTypes(ScriptMode.ENABLE, ALL_LANGS, ScriptedOp.UPDATE);
        assertScriptModes(ScriptMode.ENABLE, ALL_LANGS, new ScriptType[]{ScriptType.FILE}, ScriptedOp.SEARCH, ScriptedOp.MAPPING, ScriptedOp.AGGS);
        assertScriptModes(ScriptMode.SANDBOX, ALL_LANGS, new ScriptType[]{ScriptType.INDEXED, ScriptType.INLINE}, ScriptedOp.SEARCH, ScriptedOp.MAPPING, ScriptedOp.AGGS);
    }

    @Test
    public void testDisableUpdateGenericSettings() {
        ImmutableSettings.Builder builder = ImmutableSettings.builder().put(randomGenericOpSettings(ScriptedOp.UPDATE), randomFrom(DISABLE.values()));
        rarelyAddDisableDynamicSetting(builder);
        this.scriptModes = new ScriptModes(scriptEngines, builder.build());
        assertScriptModesAllTypes(ScriptMode.DISABLE, ALL_LANGS, ScriptedOp.UPDATE);
        assertScriptModes(ScriptMode.ENABLE, ALL_LANGS, new ScriptType[]{ScriptType.FILE}, ScriptedOp.SEARCH, ScriptedOp.MAPPING, ScriptedOp.AGGS);
        assertScriptModes(ScriptMode.SANDBOX, ALL_LANGS, new ScriptType[]{ScriptType.INDEXED, ScriptType.INLINE}, ScriptedOp.SEARCH, ScriptedOp.MAPPING, ScriptedOp.AGGS);
    }

    @Test
    public void testSandboxUpdateGenericSettings() {
        ImmutableSettings.Builder builder = ImmutableSettings.builder().put(randomGenericOpSettings(ScriptedOp.UPDATE), randomFrom(SANDBOX.values()));
        rarelyAddDisableDynamicSetting(builder);
        this.scriptModes = new ScriptModes(scriptEngines, builder.build());
        assertScriptModesAllTypes(ScriptMode.SANDBOX, ALL_LANGS, ScriptedOp.UPDATE);
        assertScriptModes(ScriptMode.ENABLE, ALL_LANGS, new ScriptType[]{ScriptType.FILE}, ScriptedOp.SEARCH, ScriptedOp.MAPPING, ScriptedOp.AGGS);
        assertScriptModes(ScriptMode.SANDBOX, ALL_LANGS, new ScriptType[]{ScriptType.INDEXED, ScriptType.INLINE}, ScriptedOp.SEARCH, ScriptedOp.MAPPING, ScriptedOp.AGGS);
    }

    @Test
    public void testMultipleScriptedOpGenericSettings() {
        ImmutableSettings.Builder builder = ImmutableSettings.builder().put(randomGenericOpSettings(ScriptedOp.UPDATE), randomFrom(SANDBOX.values()))
                .put(randomGenericOpSettings(ScriptedOp.AGGS), randomFrom(DISABLE.values()))
                .put(randomGenericOpSettings(ScriptedOp.SEARCH), randomFrom(ENABLE.values()));
        rarelyAddDisableDynamicSetting(builder);
        this.scriptModes = new ScriptModes(scriptEngines, builder.build());
        assertScriptModesAllTypes(ScriptMode.SANDBOX, ALL_LANGS, ScriptedOp.UPDATE);
        assertScriptModesAllTypes(ScriptMode.DISABLE, ALL_LANGS, ScriptedOp.AGGS);
        assertScriptModesAllTypes(ScriptMode.ENABLE, ALL_LANGS, ScriptedOp.SEARCH);
        assertScriptModes(ScriptMode.ENABLE, ALL_LANGS, new ScriptType[]{ScriptType.FILE}, ScriptedOp.MAPPING);
        assertScriptModes(ScriptMode.SANDBOX, ALL_LANGS, new ScriptType[]{ScriptType.INDEXED, ScriptType.INLINE}, ScriptedOp.MAPPING);
    }

    @Test
    public void testConflictingScriptTypeAndOpGenericSettings() {
        ImmutableSettings.Builder builder = ImmutableSettings.builder().put(randomGenericOpSettings(ScriptedOp.UPDATE), randomFrom(DISABLE.values()))
                .put("script.indexed", randomFrom(ENABLE.values())).put("script.dynamic", randomFrom(SANDBOX.values()));
        rarelyAddDisableDynamicSetting(builder);
        //operations generic settings have precedence over script type generic settings
        this.scriptModes = new ScriptModes(scriptEngines, builder.build());
        assertScriptModesAllTypes(ScriptMode.DISABLE, ALL_LANGS, ScriptedOp.UPDATE);
        assertScriptModes(ScriptMode.ENABLE, ALL_LANGS, new ScriptType[]{ScriptType.FILE, ScriptType.INDEXED}, ScriptedOp.MAPPING, ScriptedOp.AGGS, ScriptedOp.SEARCH);
        assertScriptModes(ScriptMode.SANDBOX, ALL_LANGS, new ScriptType[]{ScriptType.INLINE}, ScriptedOp.MAPPING, ScriptedOp.AGGS, ScriptedOp.SEARCH);
    }

    @Test
    public void testEngineSpecificSettings() {
        ImmutableSettings.Builder builder = ImmutableSettings.builder()
                .put(randomSpecificEngineOpSettings(GroovyScriptEngineService.NAME, ScriptType.INLINE, ScriptedOp.MAPPING), randomFrom(DISABLE.values()))
                .put(randomSpecificEngineOpSettings(GroovyScriptEngineService.NAME, ScriptType.INLINE, ScriptedOp.UPDATE), randomFrom(DISABLE.values()));
        rarelyAddDisableDynamicSetting(builder);
        ImmutableSet<String> groovyLangSet = ImmutableSet.of(GroovyScriptEngineService.NAME);
        Set<String> allButGroovyLangSet = new HashSet<>(ALL_LANGS);
        allButGroovyLangSet.remove(GroovyScriptEngineService.NAME);
        this.scriptModes = new ScriptModes(scriptEngines, builder.build());
        assertScriptModes(ScriptMode.DISABLE, groovyLangSet, new ScriptType[]{ScriptType.INLINE}, ScriptedOp.MAPPING, ScriptedOp.UPDATE);
        assertScriptModes(ScriptMode.SANDBOX, groovyLangSet, new ScriptType[]{ScriptType.INLINE}, ScriptedOp.SEARCH, ScriptedOp.AGGS);
        assertScriptModesAllOps(ScriptMode.SANDBOX, allButGroovyLangSet, ScriptType.INLINE);
        assertScriptModesAllOps(ScriptMode.SANDBOX, ALL_LANGS, ScriptType.INDEXED);
        assertScriptModesAllOps(ScriptMode.ENABLE, ALL_LANGS, ScriptType.FILE);
    }

    @Test
    public void testInteractionBetweenGenericAndEngineSpecificSettings() {
        ImmutableSettings.Builder builder = ImmutableSettings.builder().put("script.dynamic", randomFrom(DISABLE.values()))
                .put(randomSpecificEngineOpSettings(MustacheScriptEngineService.NAME, ScriptType.INLINE, ScriptedOp.AGGS), randomFrom(ENABLE.values()))
                .put(randomSpecificEngineOpSettings(MustacheScriptEngineService.NAME, ScriptType.INLINE, ScriptedOp.SEARCH), randomFrom(ENABLE.values()));
        rarelyAddDisableDynamicSetting(builder);
        ImmutableSet<String> mustacheLangSet = ImmutableSet.of(MustacheScriptEngineService.NAME);
        Set<String> allButMustacheLangSet = new HashSet<>(ALL_LANGS);
        allButMustacheLangSet.remove(MustacheScriptEngineService.NAME);
        this.scriptModes = new ScriptModes(scriptEngines, builder.build());
        assertScriptModes(ScriptMode.ENABLE, mustacheLangSet, new ScriptType[]{ScriptType.INLINE}, ScriptedOp.AGGS, ScriptedOp.SEARCH);
        assertScriptModes(ScriptMode.DISABLE, mustacheLangSet, new ScriptType[]{ScriptType.INLINE}, ScriptedOp.MAPPING, ScriptedOp.UPDATE);
        assertScriptModesAllOps(ScriptMode.DISABLE, allButMustacheLangSet, ScriptType.INLINE);
        assertScriptModesAllOps(ScriptMode.SANDBOX, ALL_LANGS, ScriptType.INDEXED);
        assertScriptModesAllOps(ScriptMode.ENABLE, ALL_LANGS, ScriptType.FILE);
    }

    @Test
    public void testDefaultSettingsToString() {
        assertAllSettingsWereChecked = false;
        this.scriptModes = new ScriptModes(scriptEngines, ImmutableSettings.EMPTY);
        assertThat(scriptModes.toString(), equalTo(
                "script.engine.custom.dynamic.aggs: sandbox\n" +
                        "script.engine.custom.dynamic.mapping: sandbox\n" +
                        "script.engine.custom.dynamic.search: sandbox\n" +
                        "script.engine.custom.dynamic.update: sandbox\n" +
                        "script.engine.custom.file.aggs: enable\n" +
                        "script.engine.custom.file.mapping: enable\n" +
                        "script.engine.custom.file.search: enable\n" +
                        "script.engine.custom.file.update: enable\n" +
                        "script.engine.custom.indexed.aggs: sandbox\n" +
                        "script.engine.custom.indexed.mapping: sandbox\n" +
                        "script.engine.custom.indexed.search: sandbox\n" +
                        "script.engine.custom.indexed.update: sandbox\n" +
                        "script.engine.expression.dynamic.aggs: sandbox\n" +
                        "script.engine.expression.dynamic.mapping: sandbox\n" +
                        "script.engine.expression.dynamic.search: sandbox\n" +
                        "script.engine.expression.dynamic.update: sandbox\n" +
                        "script.engine.expression.file.aggs: enable\n" +
                        "script.engine.expression.file.mapping: enable\n" +
                        "script.engine.expression.file.search: enable\n" +
                        "script.engine.expression.file.update: enable\n" +
                        "script.engine.expression.indexed.aggs: sandbox\n" +
                        "script.engine.expression.indexed.mapping: sandbox\n" +
                        "script.engine.expression.indexed.search: sandbox\n" +
                        "script.engine.expression.indexed.update: sandbox\n" +
                        "script.engine.groovy.dynamic.aggs: sandbox\n" +
                        "script.engine.groovy.dynamic.mapping: sandbox\n" +
                        "script.engine.groovy.dynamic.search: sandbox\n" +
                        "script.engine.groovy.dynamic.update: sandbox\n" +
                        "script.engine.groovy.file.aggs: enable\n" +
                        "script.engine.groovy.file.mapping: enable\n" +
                        "script.engine.groovy.file.search: enable\n" +
                        "script.engine.groovy.file.update: enable\n" +
                        "script.engine.groovy.indexed.aggs: sandbox\n" +
                        "script.engine.groovy.indexed.mapping: sandbox\n" +
                        "script.engine.groovy.indexed.search: sandbox\n" +
                        "script.engine.groovy.indexed.update: sandbox\n" +
                        "script.engine.mustache.dynamic.aggs: sandbox\n" +
                        "script.engine.mustache.dynamic.mapping: sandbox\n" +
                        "script.engine.mustache.dynamic.search: sandbox\n" +
                        "script.engine.mustache.dynamic.update: sandbox\n" +
                        "script.engine.mustache.file.aggs: enable\n" +
                        "script.engine.mustache.file.mapping: enable\n" +
                        "script.engine.mustache.file.search: enable\n" +
                        "script.engine.mustache.file.update: enable\n" +
                        "script.engine.mustache.indexed.aggs: sandbox\n" +
                        "script.engine.mustache.indexed.mapping: sandbox\n" +
                        "script.engine.mustache.indexed.search: sandbox\n" +
                        "script.engine.mustache.indexed.update: sandbox\n" +
                        "script.engine.test.dynamic.aggs: sandbox\n" +
                        "script.engine.test.dynamic.mapping: sandbox\n" +
                        "script.engine.test.dynamic.search: sandbox\n" +
                        "script.engine.test.dynamic.update: sandbox\n" +
                        "script.engine.test.file.aggs: enable\n" +
                        "script.engine.test.file.mapping: enable\n" +
                        "script.engine.test.file.search: enable\n" +
                        "script.engine.test.file.update: enable\n" +
                        "script.engine.test.indexed.aggs: sandbox\n" +
                        "script.engine.test.indexed.mapping: sandbox\n" +
                        "script.engine.test.indexed.search: sandbox\n" +
                        "script.engine.test.indexed.update: sandbox\n"));
    }

    private void assertScriptModesAllOps(ScriptMode expectedScriptMode, Set<String> langs, ScriptType... scriptTypes) {
        assertScriptModes(expectedScriptMode, langs, scriptTypes, ScriptedOp.values());
    }

    private void assertScriptModesAllTypes(ScriptMode expectedScriptMode, Set<String> langs, ScriptedOp... scriptedOps) {
        assertScriptModes(expectedScriptMode, langs, ScriptType.values(), scriptedOps);
    }

    private void assertScriptModes(ScriptMode expectedScriptMode, Set<String> langs, ScriptType[] scriptTypes, ScriptedOp... scriptedOps) {
        assert langs.size() > 0;
        assert scriptTypes.length > 0;
        assert scriptedOps.length > 0;
        for (String lang : langs) {
            for (ScriptType scriptType : scriptTypes) {
                for (ScriptedOp scriptedOp : scriptedOps) {
                    assertThat(lang + "." + scriptType + "." + scriptedOp + " doesn't have the expected value", scriptModes.getScriptMode(lang, scriptType, scriptedOp), equalTo(expectedScriptMode));
                    checkedSettings.add(lang + "." + scriptType + "." + scriptedOp);
                }
            }
        }
    }

    private static void rarelyAddDisableDynamicSetting(ImmutableSettings.Builder builder) {
        if (rarely()) {
            builder.put(ScriptService.DISABLE_DYNAMIC_SCRIPTING_SETTING, randomFrom("all", "true", "none", "false", "sandbox", "sandboxed"));
        }
    }

    private static String randomGenericOpSettings(ScriptedOp scriptedOp) {
        return ScriptModes.SCRIPT_SETTINGS_PREFIX + randomScriptedOpName(scriptedOp);
    }

    private static String randomSpecificEngineOpSettings(String lang, ScriptType scriptType, ScriptedOp scriptedOp) {
        return ScriptModes.ENGINE_SETTINGS_PREFIX + "." + lang + "." + scriptType + "." + randomScriptedOpName(scriptedOp);
    }

    private static String randomScriptedOpName(ScriptedOp scriptedOp) {
        String[] names = new String[scriptedOp.alternateNames().length + 1];
        names[0] = scriptedOp.toString();
        System.arraycopy(scriptedOp.alternateNames(), 0, names, 1, scriptedOp.alternateNames().length);
        return randomFrom(names);
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

    static enum ENABLE {
        ENABLE, ENABLED, ON, TRUE;

        @Override
        public String toString() {
            return randomCase(name());
        }
    }

    static enum DISABLE {
        DISABLE, DISABLED, OFF, FALSE;

        @Override
        public String toString() {
            return randomCase(name());
        }
    }

    static enum SANDBOX {
        SANDBOX, SANDBOXED;

        @Override
        public String toString() {
            return randomCase(name());
        }
    }

    private static String randomCase(String name) {
        if (randomBoolean()) {
            return name.toLowerCase(Locale.ROOT);
        }
        return name;
    }
}
