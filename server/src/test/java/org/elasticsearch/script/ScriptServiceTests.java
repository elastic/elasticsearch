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
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.env.Environment;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import static org.elasticsearch.script.ScriptService.Cache;
import static org.elasticsearch.script.ScriptService.CacheSettings;
import static org.elasticsearch.script.ScriptService.MAX_COMPILATION_RATE_FUNCTION;
import static org.elasticsearch.script.ScriptService.SCRIPT_GENERAL_CACHE_EXPIRE_SETTING;
import static org.elasticsearch.script.ScriptService.SCRIPT_GENERAL_CACHE_SIZE_SETTING;
import static org.elasticsearch.script.ScriptService.SCRIPT_GENERAL_MAX_COMPILATIONS_RATE;
import static org.elasticsearch.script.ScriptService.SCRIPT_MAX_COMPILATIONS_RATE;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.sameInstance;

public class ScriptServiceTests extends ESTestCase {

    private ScriptEngine scriptEngine;
    private Map<String, ScriptEngine> engines;
    private Map<String, ScriptContext<?>> contexts;
    private ScriptService scriptService;
    private Settings baseSettings;
    private ClusterSettings clusterSettings;

    @Before
    public void setup() throws IOException {
        baseSettings = Settings.builder()
                .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString())
                .build();
        Map<String, Function<Map<String, Object>, Object>> scripts = new HashMap<>();
        for (int i = 0; i < 20; ++i) {
            scripts.put(i + "+" + i, p -> null); // only care about compilation, not execution
        }
        scripts.put("script", p -> null);
        scriptEngine = new MockScriptEngine(Script.DEFAULT_SCRIPT_LANG, scripts, Collections.emptyMap());
        //prevent duplicates using map
        contexts = new HashMap<>(ScriptModule.CORE_CONTEXTS);
        engines = new HashMap<>();
        engines.put(scriptEngine.getType(), scriptEngine);
        engines.put("test", new MockScriptEngine("test", scripts, Collections.emptyMap()));
        logger.info("--> setup script service");
    }

    private void buildScriptService(Settings additionalSettings) throws IOException {
        Settings finalSettings = Settings.builder().put(baseSettings).put(additionalSettings).build();
        scriptService = new ScriptService(finalSettings, engines, contexts) {
            @Override
            Map<String, StoredScriptSource> getScriptsFromClusterState() {
                Map<String, StoredScriptSource> scripts = new HashMap<>();
                scripts.put("test1", new StoredScriptSource("test", "1+1", Collections.emptyMap()));
                scripts.put("test2", new StoredScriptSource("test", "1", Collections.emptyMap()));
                return scripts;
            }

            @Override
            StoredScriptSource getScriptFromClusterState(String id) {
                //mock the script that gets retrieved from an index
                return new StoredScriptSource("test", "1+1", Collections.emptyMap());
            }
        };
        clusterSettings = new ClusterSettings(finalSettings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        scriptService.registerClusterSettingsListeners(clusterSettings);
    }

    public void testMaxCompilationRateSetting() throws Exception {
        assertThat(MAX_COMPILATION_RATE_FUNCTION.apply("10/1m"), is(Tuple.tuple(10, TimeValue.timeValueMinutes(1))));
        assertThat(MAX_COMPILATION_RATE_FUNCTION.apply("10/60s"), is(Tuple.tuple(10, TimeValue.timeValueMinutes(1))));
        assertException("10/m", IllegalArgumentException.class, "failed to parse [m]");
        assertException("6/1.6m", IllegalArgumentException.class, "failed to parse [1.6m], fractional time values are not supported");
        assertException("foo/bar", IllegalArgumentException.class, "could not parse [foo] as integer in value [foo/bar]");
        assertException("6.0/1m", IllegalArgumentException.class, "could not parse [6.0] as integer in value [6.0/1m]");
        assertException("6/-1m", IllegalArgumentException.class, "time value [-1m] must be positive");
        assertException("6/0m", IllegalArgumentException.class, "time value [0m] must be positive");
        assertException("10", IllegalArgumentException.class,
                "parameter must contain a positive integer and a timevalue, i.e. 10/1m, but was [10]");
        assertException("anything", IllegalArgumentException.class,
                "parameter must contain a positive integer and a timevalue, i.e. 10/1m, but was [anything]");
        assertException("/1m", IllegalArgumentException.class,
                "parameter must contain a positive integer and a timevalue, i.e. 10/1m, but was [/1m]");
        assertException("10/", IllegalArgumentException.class,
                "parameter must contain a positive integer and a timevalue, i.e. 10/1m, but was [10/]");
        assertException("-1/1m", IllegalArgumentException.class, "rate [-1] must be positive");
        assertException("10/5s", IllegalArgumentException.class, "time value [5s] must be at least on a one minute resolution");
    }

    private void assertException(String rate, Class<? extends Exception> clazz, String message) {
        Exception e = expectThrows(clazz, () -> MAX_COMPILATION_RATE_FUNCTION.apply(rate));
        assertThat(e.getMessage(), is(message));
    }

    public void testNotSupportedDisableDynamicSetting() throws IOException {
        try {
            buildScriptService(Settings.builder().put(
                    ScriptService.DISABLE_DYNAMIC_SCRIPTING_SETTING, randomUnicodeOfLength(randomIntBetween(1, 10))).build());
            fail("script service should have thrown exception due to non supported script.disable_dynamic setting");
        } catch(IllegalArgumentException e) {
            assertThat(e.getMessage(), containsString(ScriptService.DISABLE_DYNAMIC_SCRIPTING_SETTING +
                    " is not a supported setting, replace with fine-grained script settings"));
        }
    }

    public void testInlineScriptCompiledOnceCache() throws IOException {
        buildScriptService(Settings.EMPTY);
        Script script = new Script(ScriptType.INLINE, "test", "1+1", Collections.emptyMap());
        FieldScript.Factory factoryScript1 = scriptService.compile(script, FieldScript.CONTEXT);
        FieldScript.Factory factoryScript2 = scriptService.compile(script, FieldScript.CONTEXT);
        assertThat(factoryScript1, sameInstance(factoryScript2));
    }

    public void testAllowAllScriptTypeSettings() throws IOException {
        buildScriptService(Settings.EMPTY);

        assertCompileAccepted("painless", "script", ScriptType.INLINE, FieldScript.CONTEXT);
        assertCompileAccepted(null, "script", ScriptType.STORED, FieldScript.CONTEXT);
    }

    public void testAllowAllScriptContextSettings() throws IOException {
        buildScriptService(Settings.EMPTY);

        assertCompileAccepted("painless", "script", ScriptType.INLINE, FieldScript.CONTEXT);
        assertCompileAccepted("painless", "script", ScriptType.INLINE, AggregationScript.CONTEXT);
        assertCompileAccepted("painless", "script", ScriptType.INLINE, UpdateScript.CONTEXT);
        assertCompileAccepted("painless", "script", ScriptType.INLINE, IngestScript.CONTEXT);
    }

    public void testAllowSomeScriptTypeSettings() throws IOException {
        Settings.Builder builder = Settings.builder();
        builder.put("script.allowed_types", "inline");
        buildScriptService(builder.build());

        assertCompileAccepted("painless", "script", ScriptType.INLINE, FieldScript.CONTEXT);
        assertCompileRejected(null, "script", ScriptType.STORED, FieldScript.CONTEXT);
    }

    public void testAllowSomeScriptContextSettings() throws IOException {
        Settings.Builder builder = Settings.builder();
        builder.put("script.allowed_contexts", "field, aggs");
        buildScriptService(builder.build());

        assertCompileAccepted("painless", "script", ScriptType.INLINE, FieldScript.CONTEXT);
        assertCompileAccepted("painless", "script", ScriptType.INLINE, AggregationScript.CONTEXT);
        assertCompileRejected("painless", "script", ScriptType.INLINE, UpdateScript.CONTEXT);
    }

    public void testAllowNoScriptTypeSettings() throws IOException {
        Settings.Builder builder = Settings.builder();
        builder.put("script.allowed_types", "none");
        buildScriptService(builder.build());

        assertCompileRejected("painless", "script", ScriptType.INLINE, FieldScript.CONTEXT);
        assertCompileRejected(null, "script", ScriptType.STORED, FieldScript.CONTEXT);
    }

    public void testAllowNoScriptContextSettings() throws IOException {
        Settings.Builder builder = Settings.builder();
        builder.put("script.allowed_contexts", "none");
        buildScriptService(builder.build());

        assertCompileRejected("painless", "script", ScriptType.INLINE, FieldScript.CONTEXT);
        assertCompileRejected("painless", "script", ScriptType.INLINE, AggregationScript.CONTEXT);
    }

    public void testCompileNonRegisteredContext() throws IOException {
        contexts.remove(IngestScript.CONTEXT.name);
        buildScriptService(Settings.EMPTY);

        String type = scriptEngine.getType();
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () ->
            scriptService.compile(new Script(ScriptType.INLINE, type, "test", Collections.emptyMap()), IngestScript.CONTEXT));
        assertThat(e.getMessage(), containsString("script context [" + IngestScript.CONTEXT.name + "] not supported"));
    }

    public void testCompileCountedInCompilationStats() throws IOException {
        buildScriptService(Settings.EMPTY);
        scriptService.compile(new Script(ScriptType.INLINE, "test", "1+1", Collections.emptyMap()), randomFrom(contexts.values()));
        assertEquals(1L, scriptService.stats().getCompilations());
    }

    public void testMultipleCompilationsCountedInCompilationStats() throws IOException {
        buildScriptService(Settings.EMPTY);
        int numberOfCompilations = randomIntBetween(1, 20);
        for (int i = 0; i < numberOfCompilations; i++) {
            scriptService
                    .compile(new Script(ScriptType.INLINE, "test", i + "+" + i, Collections.emptyMap()), randomFrom(contexts.values()));
        }
        assertEquals(numberOfCompilations, scriptService.stats().getCompilations());
    }

    public void testCompilationStatsOnCacheHit() throws IOException {
        Settings.Builder builder = Settings.builder();
        builder.put(ScriptService.SCRIPT_GENERAL_CACHE_SIZE_SETTING.getKey(), 1);
        buildScriptService(builder.build());
        Script script = new Script(ScriptType.INLINE, "test", "1+1", Collections.emptyMap());
        ScriptContext<?> context = randomFrom(contexts.values());
        scriptService.compile(script, context);
        scriptService.compile(script, context);
        assertEquals(1L, scriptService.stats().getCompilations());
    }

    public void testIndexedScriptCountedInCompilationStats() throws IOException {
        buildScriptService(Settings.EMPTY);
        scriptService.compile(new Script(ScriptType.STORED, null, "script", Collections.emptyMap()), randomFrom(contexts.values()));
        assertEquals(1L, scriptService.stats().getCompilations());
    }

    public void testCacheEvictionCountedInCacheEvictionsStats() throws IOException {
        Settings.Builder builder = Settings.builder();
        builder.put(ScriptService.SCRIPT_GENERAL_CACHE_SIZE_SETTING.getKey(), 1);
        buildScriptService(builder.build());
        scriptService.compile(new Script(ScriptType.INLINE, "test", "1+1", Collections.emptyMap()), randomFrom(contexts.values()));
        scriptService.compile(new Script(ScriptType.INLINE, "test", "2+2", Collections.emptyMap()), randomFrom(contexts.values()));
        assertEquals(2L, scriptService.stats().getCompilations());
        assertEquals(1L, scriptService.stats().getCacheEvictions());
    }

    public void testStoreScript() throws Exception {
        BytesReference script = BytesReference.bytes(XContentFactory.jsonBuilder()
            .startObject()
            .field("script")
            .startObject()
            .field("lang", "_lang")
            .field("source", "abc")
            .endObject()
            .endObject());
        ScriptMetaData scriptMetaData = ScriptMetaData.putStoredScript(null, "_id", StoredScriptSource.parse(script, XContentType.JSON));
        assertNotNull(scriptMetaData);
        assertEquals("abc", scriptMetaData.getStoredScript("_id").getSource());
    }

    public void testDeleteScript() throws Exception {
        ScriptMetaData scriptMetaData = ScriptMetaData.putStoredScript(null, "_id",
            StoredScriptSource.parse(new BytesArray("{\"script\": {\"lang\": \"_lang\", \"source\": \"abc\"} }"), XContentType.JSON));
        scriptMetaData = ScriptMetaData.deleteStoredScript(scriptMetaData, "_id");
        assertNotNull(scriptMetaData);
        assertNull(scriptMetaData.getStoredScript("_id"));

        ScriptMetaData errorMetaData = scriptMetaData;
        ResourceNotFoundException e = expectThrows(ResourceNotFoundException.class, () -> {
            ScriptMetaData.deleteStoredScript(errorMetaData, "_id");
        });
        assertEquals("stored script [_id] does not exist and cannot be deleted", e.getMessage());
    }

    public void testGetStoredScript() throws Exception {
        buildScriptService(Settings.EMPTY);
        ClusterState cs = ClusterState.builder(new ClusterName("_name"))
            .metaData(MetaData.builder()
                .putCustom(ScriptMetaData.TYPE,
                    new ScriptMetaData.Builder(null).storeScript("_id",
                        StoredScriptSource.parse(new BytesArray("{\"script\": {\"lang\": \"_lang\", \"source\": \"abc\"} }"),
                            XContentType.JSON)).build()))
            .build();

        assertEquals("abc", scriptService.getStoredScript(cs, new GetStoredScriptRequest("_id")).getSource());

        cs = ClusterState.builder(new ClusterName("_name")).build();
        assertNull(scriptService.getStoredScript(cs, new GetStoredScriptRequest("_id")));
    }

    public void testMaxSizeLimit() throws Exception {
        buildScriptService(Settings.builder().put(ScriptService.SCRIPT_MAX_SIZE_IN_BYTES.getKey(), 4).build());
        scriptService.compile(new Script(ScriptType.INLINE, "test", "1+1", Collections.emptyMap()), randomFrom(contexts.values()));
        IllegalArgumentException iae = expectThrows(IllegalArgumentException.class, () -> {
            scriptService.compile(new Script(ScriptType.INLINE, "test", "10+10", Collections.emptyMap()), randomFrom(contexts.values()));
        });
        assertEquals("exceeded max allowed inline script size in bytes [4] with size [5] for script [10+10]", iae.getMessage());
        clusterSettings.applySettings(Settings.builder().put(ScriptService.SCRIPT_MAX_SIZE_IN_BYTES.getKey(), 6).build());
        scriptService.compile(new Script(ScriptType.INLINE, "test", "10+10", Collections.emptyMap()), randomFrom(contexts.values()));
        clusterSettings.applySettings(Settings.builder().put(ScriptService.SCRIPT_MAX_SIZE_IN_BYTES.getKey(), 5).build());
        scriptService.compile(new Script(ScriptType.INLINE, "test", "10+10", Collections.emptyMap()), randomFrom(contexts.values()));
        iae = expectThrows(IllegalArgumentException.class, () -> {
            clusterSettings.applySettings(Settings.builder().put(ScriptService.SCRIPT_MAX_SIZE_IN_BYTES.getKey(), 2).build());
        });
        assertEquals("script.max_size_in_bytes cannot be set to [2], stored script [test1] exceeds the new value with a size of [3]",
                iae.getMessage());
    }

    public void testGeneralCacheConstructor() {
        CacheSettings general = randomCacheSettings();
        Set<String> contexts = randomContexts();
        Cache cache = new Cache(general, contexts);
        assertNotNull(cache.general);
        assertEquals(cache.contextCache.keySet(), contexts);
        for (ScriptCache cachePerContext: cache.contextCache.values()) {
            assertSame(cache.general, cachePerContext);
        }
    }

    public void testContextCacheConstructor() {
        Map<String, CacheSettings> contextSettings = new HashMap<>();
        List<String> contexts = List.of("abc", "def", "hij", "lmn", "opq", "rst");
        for (String context: contexts) {
            contextSettings.put(context, randomCacheSettings());
        }
        CacheSettings generalSettings = randomCacheSettings();
        Set<String> contextSizeSet = new HashSet<>(contexts.subList(0, 2));
        Set<String> contextExpireSet = new HashSet<>(contexts.subList(2, 4));

        // General unset: always use context settings
        Cache cache = new Cache(contextSettings, generalSettings, false, contextSizeSet, false, contextExpireSet);
        assertNull(cache.general);
        for (Map.Entry<String, ScriptCache> entry: cache.contextCache.entrySet()) {
            ScriptCache contextCache = entry.getValue();
            CacheSettings contextSetting = contextSettings.get(entry.getKey());
            assertEquals(contextSetting.size.intValue(), contextCache.cacheSize);
            assertEquals(contextSetting.expire, contextCache.cacheExpire);
            assertEquals(contextSetting.compileRate, contextCache.rate);
        }

        // General set: use context settings if set, otherwise use general
        cache = new Cache(contextSettings, generalSettings, true, contextSizeSet, true, contextExpireSet);
        // size not set, use general
        for (String context: List.of("hij", "lmn", "opq", "rst")) {
            assertEquals(generalSettings.size.intValue(), cache.contextCache.get(context).cacheSize);
        }
        // size is set, use context size
        for (String context: List.of("abc", "def")) {
            assertEquals(contextSettings.get(context).size.intValue(), cache.contextCache.get(context).cacheSize);
        }
        // expire not set, use general
        for (String context: List.of("abc", "def", "opq", "rst")) {
            assertEquals(generalSettings.expire, cache.contextCache.get(context).cacheExpire);
        }
        // expire is set, use context expire
        for (String context: List.of("hij", "lmn")) {
            assertEquals(contextSettings.get(context).expire, cache.contextCache.get(context).cacheExpire);
        }
    }

    private Set<String> randomContexts() {
        return new HashSet<>(randomList(10, () -> randomAlphaOfLengthBetween(1, 20)));
    }

    private TimeValue randomTimeValueObject() {
        return TimeValue.timeValueSeconds(randomIntBetween(0, 3600));
    }

    private CacheSettings randomCacheSettings() {
        return new CacheSettings(
            randomIntBetween(0, 1000),
            randomTimeValueObject(),
            new Tuple<>(randomIntBetween(10, 1000), randomTimeValueObject())
        );
    }

    public void testGeneralSettingsCacheUpdater() {
        Settings settings = Settings.builder()
            .put(SCRIPT_GENERAL_CACHE_SIZE_SETTING.getKey(), 101)
            .put(SCRIPT_GENERAL_CACHE_EXPIRE_SETTING.getKey(), TimeValue.timeValueMinutes(60))
            .put(SCRIPT_GENERAL_MAX_COMPILATIONS_RATE.getKey(), "80/2m")
            .build();
        List<ScriptContext<?>> contextList = List.of(FieldScript.CONTEXT, AggregationScript.CONTEXT, UpdateScript.CONTEXT,
                                                        IngestScript.CONTEXT);
        Map<String, ScriptContext<?>> contexts = new HashMap<>();
        for (ScriptContext<?> context: contextList) {
            contexts.put(context.name, context);
        }

        AtomicReference<Cache> cacheRef = new AtomicReference<>();
        ScriptService.CacheUpdater updater = new ScriptService.CacheUpdater(settings, contexts, true, cacheRef);

        // SCRIPT_GENERAL_MAX_COMPILATIONS_RATE is set, should get general settings
        assertFalse(updater.isContextCacheEnabled());
        assertNotNull(cacheRef.get().general);
        assertEquals(0, updater.contextSizeSet.size());
        assertEquals(0, updater.contextExpireSet.size());

        // Test flipping to the "use-context" rate
        updater.setMaxCompilationRate(ScriptService.USE_CONTEXT_RATE_VALUE);
        assertTrue(updater.isContextCacheEnabled());
        assertNull(cacheRef.get().general);

        HashMap<String, CacheSettings> contextSettings = new HashMap<>();
        for (String context: contexts.keySet()) {
            CacheSettings cacheSetting = randomCacheSettings();
            contextSettings.put(context, cacheSetting);
            updater.setMaxCompilationRate(context, cacheSetting.compileRate);
            updater.setScriptCacheExpire(context, cacheSetting.expire);
            updater.setScriptCacheSize(context, cacheSetting.size);
        }

        Map<String, ScriptCache> contextCaches = cacheRef.get().contextCache;
        for (String context: contexts.keySet()) {
            ScriptCache cache = contextCaches.get(context);
            assertNotNull(cache);
            CacheSettings contextSetting = contextSettings.get(context);
            assertEquals(cache.cacheExpire, contextSetting.expire);
            assertEquals(cache.cacheSize, contextSetting.size.intValue());
            assertEquals(cache.rate, contextSetting.compileRate);
        }

        // Test flipping back from the "use-context" rate
        updater.setMaxCompilationRate(new Tuple<>(80, TimeValue.timeValueMinutes(3)));
        assertFalse(updater.isContextCacheEnabled());
        assertNotNull(cacheRef.get().general);
    }

    public void testContextSettingsCacheUpdater() {
        List<ScriptContext<?>> contextList = List.of(FieldScript.CONTEXT, AggregationScript.CONTEXT, UpdateScript.CONTEXT,
                                                    IngestScript.CONTEXT);
        Map<String, ScriptContext<?>> contexts = new HashMap<>();
        for (ScriptContext<?> context : contextList) {
            contexts.put(context.name, context);
        }

        Map<String, CacheSettings> contextSettings = new HashMap<>();
        Settings.Builder settingBuilder = Settings.builder();
        for (ScriptContext<?> context: contextList.subList(0, 2)) {
            CacheSettings cs = randomCacheSettings();
            contextSettings.put(context.name, cs);
            settingBuilder.put(ScriptService.SCRIPT_CACHE_SIZE_SETTING.getConcreteSettingForNamespace(context.name).getKey(),
                               cs.size.toString());
            settingBuilder.put(ScriptService.SCRIPT_CACHE_EXPIRE_SETTING.getConcreteSettingForNamespace(context.name).getKey(),
                               cs.expire.seconds() + "s");
            settingBuilder.put(ScriptService.SCRIPT_MAX_COMPILATIONS_RATE.getConcreteSettingForNamespace(context.name).getKey(),
                               cs.compileRate.v1() + "/" +  cs.compileRate.v2().getMillis() + "ms"
            );
        }

        CacheSettings generalSettings = randomCacheSettings();
        settingBuilder.put(SCRIPT_GENERAL_CACHE_SIZE_SETTING.getKey(), generalSettings.size.toString());
        settingBuilder.put(SCRIPT_GENERAL_CACHE_EXPIRE_SETTING.getKey(), generalSettings.expire.seconds() + "s");
        settingBuilder.put(ScriptService.SCRIPT_GENERAL_MAX_COMPILATIONS_RATE.getKey(), ScriptService.USE_CONTEXT_RATE_KEY);

        AtomicReference<Cache> cacheRef = new AtomicReference<>();
        ScriptService.CacheUpdater updater = new ScriptService.CacheUpdater(settingBuilder.build(), contexts, true, cacheRef);
        assertTrue(updater.isContextCacheEnabled());
        Cache cache = cacheRef.get();
        assertNull(cache.general);

        for (ScriptContext<?> context: contextList) {
            String name = context.name;
            ScriptCache sc = cache.contextCache.get(name);
            assertNotNull(sc);
            if (contextSettings.containsKey(name)) {
                CacheSettings cs = contextSettings.get(name);
                assertEquals(sc.cacheExpire, cs.expire);
                assertEquals(sc.cacheSize, cs.size.intValue());
                assertEquals(sc.rate, cs.compileRate);
            } else {
                assertEquals(sc.cacheExpire, generalSettings.expire);
                assertEquals(sc.cacheSize, generalSettings.size.intValue());
                assertEquals(sc.rate, SCRIPT_MAX_COMPILATIONS_RATE.getDefault(Settings.EMPTY));
            }
        }
    }

    private void assertCompileRejected(String lang, String script, ScriptType scriptType, ScriptContext scriptContext) {
        try {
            scriptService.compile(new Script(scriptType, lang, script, Collections.emptyMap()), scriptContext);
            fail("compile should have been rejected for lang [" + lang + "], " +
                    "script_type [" + scriptType + "], scripted_op [" + scriptContext + "]");
        } catch (IllegalArgumentException | IllegalStateException e) {
            // pass
        }
    }

    private void assertCompileAccepted(String lang, String script, ScriptType scriptType, ScriptContext scriptContext) {
        assertThat(
                scriptService.compile(new Script(scriptType, lang, script, Collections.emptyMap()), scriptContext),
                notNullValue()
        );
    }
}
