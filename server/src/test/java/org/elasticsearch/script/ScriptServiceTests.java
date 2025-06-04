/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.script;

import org.apache.logging.log4j.Level;
import org.elasticsearch.action.admin.cluster.storedscripts.GetStoredScriptRequest;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.env.Environment;
import org.elasticsearch.exception.ResourceNotFoundException;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.junit.Before;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.elasticsearch.script.ScriptService.SCRIPT_CACHE_EXPIRE_SETTING;
import static org.elasticsearch.script.ScriptService.SCRIPT_CACHE_SIZE_SETTING;
import static org.elasticsearch.script.ScriptService.SCRIPT_GENERAL_CACHE_EXPIRE_SETTING;
import static org.elasticsearch.script.ScriptService.SCRIPT_GENERAL_CACHE_SIZE_SETTING;
import static org.elasticsearch.script.ScriptService.SCRIPT_GENERAL_MAX_COMPILATIONS_RATE_SETTING;
import static org.elasticsearch.script.ScriptService.SCRIPT_MAX_COMPILATIONS_RATE_SETTING;
import static org.elasticsearch.script.ScriptService.USE_CONTEXT_RATE_KEY;
import static org.elasticsearch.script.ScriptService.USE_CONTEXT_RATE_KEY_DEPRECATION_MESSAGE;
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
    private Map<String, ScriptContext<?>> rateLimitedContexts;

    @Before
    public void setup() throws IOException {
        baseSettings = Settings.builder().put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString()).build();
        Map<String, Function<Map<String, Object>, Object>> scripts = new HashMap<>();
        for (int i = 0; i < 20; ++i) {
            scripts.put(i + "+" + i, p -> null); // only care about compilation, not execution
        }
        scripts.put("script", p -> null);
        scriptEngine = new MockScriptEngine(Script.DEFAULT_SCRIPT_LANG, scripts, Collections.emptyMap());
        // prevent duplicates using map
        contexts = new HashMap<>(ScriptModule.CORE_CONTEXTS);
        engines = new HashMap<>();
        engines.put(scriptEngine.getType(), scriptEngine);
        engines.put("test", new MockScriptEngine("test", scripts, Collections.emptyMap()));
        logger.info("--> setup script service");
        rateLimitedContexts = compilationRateLimitedContexts();
    }

    private void buildScriptService(Settings additionalSettings) throws IOException {
        Settings finalSettings = Settings.builder().put(baseSettings).put(additionalSettings).build();
        scriptService = new ScriptService(finalSettings, engines, contexts, () -> 1L) {
            @Override
            Map<String, StoredScriptSource> getScriptsFromClusterState() {
                Map<String, StoredScriptSource> scripts = new HashMap<>();
                scripts.put("test1", new StoredScriptSource("test", "1+1", Collections.emptyMap()));
                scripts.put("test2", new StoredScriptSource("test", "1", Collections.emptyMap()));
                return scripts;
            }

            @Override
            protected StoredScriptSource getScriptFromClusterState(String id) {
                // mock the script that gets retrieved from an index
                return new StoredScriptSource("test", "1+1", Collections.emptyMap());
            }
        };
        clusterSettings = new ClusterSettings(finalSettings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        scriptService.registerClusterSettingsListeners(clusterSettings);
    }

    public void testMaxCompilationRateSetting() throws Exception {
        assertThat(new ScriptCache.CompilationRate("10/1m"), is(new ScriptCache.CompilationRate(10, TimeValue.timeValueMinutes(1))));
        assertThat(new ScriptCache.CompilationRate("10/60s"), is(new ScriptCache.CompilationRate(10, TimeValue.timeValueMinutes(1))));
        assertException("10/m", IllegalArgumentException.class, "failed to parse [m]");
        assertException("6/1.6m", IllegalArgumentException.class, "failed to parse [1.6m], fractional time values are not supported");
        assertException("foo/bar", IllegalArgumentException.class, "could not parse [foo] as integer in value [foo/bar]");
        assertException("6.0/1m", IllegalArgumentException.class, "could not parse [6.0] as integer in value [6.0/1m]");
        assertException("6/-1m", IllegalArgumentException.class, "time value [-1m] must be positive");
        assertException("6/0m", IllegalArgumentException.class, "time value [0m] must be positive");
        assertException(
            "10",
            IllegalArgumentException.class,
            "parameter must contain a positive integer and a timevalue, i.e. 10/1m, but was [10]"
        );
        assertException(
            "anything",
            IllegalArgumentException.class,
            "parameter must contain a positive integer and a timevalue, i.e. 10/1m, but was [anything]"
        );
        assertException(
            "/1m",
            IllegalArgumentException.class,
            "parameter must contain a positive integer and a timevalue, i.e. 10/1m, but was [/1m]"
        );
        assertException(
            "10/",
            IllegalArgumentException.class,
            "parameter must contain a positive integer and a timevalue, i.e. 10/1m, but was [10/]"
        );
        assertException("-1/1m", IllegalArgumentException.class, "rate [-1] must be positive");
        assertException("10/5s", IllegalArgumentException.class, "time value [5s] must be at least on a one minute resolution");
    }

    private void assertException(String rate, Class<? extends Exception> clazz, String message) {
        Exception e = expectThrows(clazz, () -> new ScriptCache.CompilationRate(rate));
        assertThat(e.getMessage(), is(message));
    }

    public void testNotSupportedDisableDynamicSetting() throws IOException {
        try {
            buildScriptService(
                Settings.builder()
                    .put(ScriptService.DISABLE_DYNAMIC_SCRIPTING_SETTING, randomUnicodeOfLength(randomIntBetween(1, 10)))
                    .build()
            );
            fail("script service should have thrown exception due to non supported script.disable_dynamic setting");
        } catch (IllegalArgumentException e) {
            assertThat(
                e.getMessage(),
                containsString(
                    ScriptService.DISABLE_DYNAMIC_SCRIPTING_SETTING
                        + " is not a supported setting, replace with fine-grained script settings"
                )
            );
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
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> scriptService.compile(new Script(ScriptType.INLINE, type, "test", Collections.emptyMap()), IngestScript.CONTEXT)
        );
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
            scriptService.compile(
                new Script(ScriptType.INLINE, "test", i + "+" + i, Collections.emptyMap()),
                randomFrom(contexts.values())
            );
        }
        assertEquals(numberOfCompilations, scriptService.stats().getCompilations());
    }

    public void testCompilationGeneralStatsOnCacheHit() throws IOException {
        buildScriptService(Settings.EMPTY);
        Script script = new Script(ScriptType.INLINE, "test", "1+1", Collections.emptyMap());
        ScriptContext<?> context = randomFrom(contexts.values());
        scriptService.compile(script, context);
        scriptService.compile(script, context);
        assertEquals(1L, scriptService.stats().getCompilations());
    }

    public void testIndexedScriptCountedInGeneralCompilationStats() throws IOException {
        buildScriptService(Settings.EMPTY);
        ScriptContext<?> ctx = randomFrom(contexts.values());
        scriptService.compile(new Script(ScriptType.STORED, null, "script", Collections.emptyMap()), ctx);
        assertEquals(1L, scriptService.stats().getCompilations());
    }

    public void testContextCompilationStatsOnCacheHit() throws IOException {
        buildScriptService(Settings.builder().put(SCRIPT_GENERAL_MAX_COMPILATIONS_RATE_SETTING.getKey(), USE_CONTEXT_RATE_KEY).build());
        Script script = new Script(ScriptType.INLINE, "test", "1+1", Collections.emptyMap());
        ScriptContext<?> context = randomFrom(contexts.values());
        scriptService.compile(script, context);
        scriptService.compile(script, context);
        assertEquals(1L, scriptService.stats().getCompilations());
        assertWarnings(true, new DeprecationWarning(Level.WARN, USE_CONTEXT_RATE_KEY_DEPRECATION_MESSAGE));
    }

    public void testGeneralCompilationStatsOnCacheHit() throws IOException {
        Settings.Builder builder = Settings.builder()
            .put(SCRIPT_GENERAL_CACHE_SIZE_SETTING.getKey(), 1)
            .put(SCRIPT_GENERAL_MAX_COMPILATIONS_RATE_SETTING.getKey(), "2/1m");
        buildScriptService(builder.build());
        Script script = new Script(ScriptType.INLINE, "test", "1+1", Collections.emptyMap());
        ScriptContext<?> context = randomFrom(contexts.values());
        scriptService.compile(script, context);
        scriptService.compile(script, context);
        assertEquals(1L, scriptService.stats().getCompilations());
    }

    public void testGeneralIndexedScriptCountedInCompilationStats() throws IOException {
        buildScriptService(Settings.EMPTY);
        ScriptContext<?> ctx = randomFrom(contexts.values());
        scriptService.compile(new Script(ScriptType.STORED, null, "script", Collections.emptyMap()), ctx);
        assertEquals(1L, scriptService.stats().getCompilations());
    }

    public void testContextIndexedScriptCountedInCompilationStats() throws IOException {
        buildScriptService(Settings.builder().put(SCRIPT_GENERAL_MAX_COMPILATIONS_RATE_SETTING.getKey(), USE_CONTEXT_RATE_KEY).build());
        ScriptContext<?> ctx = randomFrom(contexts.values());
        scriptService.compile(new Script(ScriptType.STORED, null, "script", Collections.emptyMap()), ctx);
        assertEquals(1L, scriptService.stats().getCompilations());
        assertEquals(1L, scriptService.cacheStats().getContextStats().get(ctx.name).getCompilations());
        assertWarnings(true, new DeprecationWarning(Level.WARN, USE_CONTEXT_RATE_KEY_DEPRECATION_MESSAGE));
    }

    public void testCacheEvictionCountedInCacheEvictionsStats() throws IOException {
        ScriptContext<?> context = randomFrom(contexts.values());
        Setting<?> contextCacheSizeSetting = SCRIPT_CACHE_SIZE_SETTING.getConcreteSettingForNamespace(context.name);
        buildScriptService(
            Settings.builder()
                .put(SCRIPT_GENERAL_MAX_COMPILATIONS_RATE_SETTING.getKey(), USE_CONTEXT_RATE_KEY)
                .put(contextCacheSizeSetting.getKey(), 1)
                .build()
        );
        scriptService.compile(new Script(ScriptType.INLINE, "test", "1+1", Collections.emptyMap()), context);
        scriptService.compile(new Script(ScriptType.INLINE, "test", "2+2", Collections.emptyMap()), context);
        assertEquals(2L, scriptService.stats().getCompilations());
        assertEquals(1L, scriptService.stats().getCacheEvictions());
        assertSettingDeprecationsAndWarnings(
            new Setting<?>[] { contextCacheSizeSetting },
            new DeprecationWarning(Level.WARN, USE_CONTEXT_RATE_KEY_DEPRECATION_MESSAGE)
        );
    }

    public void testGeneralCacheEvictionCountedInCacheEvictionsStats() throws IOException {
        Settings.Builder builder = Settings.builder();
        builder.put(SCRIPT_GENERAL_CACHE_SIZE_SETTING.getKey(), 1);
        builder.put(SCRIPT_GENERAL_MAX_COMPILATIONS_RATE_SETTING.getKey(), "10/1m");
        buildScriptService(builder.build());
        scriptService.compile(new Script(ScriptType.INLINE, "test", "1+1", Collections.emptyMap()), randomFrom(contexts.values()));
        scriptService.compile(new Script(ScriptType.INLINE, "test", "2+2", Collections.emptyMap()), randomFrom(contexts.values()));
        assertEquals(2L, scriptService.stats().getCompilations());
        assertEquals(2L, scriptService.cacheStats().getGeneralStats().getCompilations());
        assertEquals(1L, scriptService.stats().getCacheEvictions());
        assertEquals(1L, scriptService.cacheStats().getGeneralStats().getCacheEvictions());
    }

    public void testContextCacheStats() throws IOException {
        ScriptContext<?> contextA = randomFrom(rateLimitedContexts.values());
        String aRate = "2/10m";
        ScriptContext<?> contextB = randomValueOtherThan(contextA, () -> randomFrom(rateLimitedContexts.values()));
        String bRate = "3/10m";
        BiFunction<String, String, String> msg = (rate, ctx) -> ("[script] Too many dynamic script compilations within, max: ["
            + rate
            + "]; please use indexed, or scripts with parameters instead; this limit can be changed by the [script.context."
            + ctx
            + ".max_compilations_rate] setting");
        Setting<?> cacheSizeA = SCRIPT_CACHE_SIZE_SETTING.getConcreteSettingForNamespace(contextA.name);
        Setting<?> compilationRateA = SCRIPT_MAX_COMPILATIONS_RATE_SETTING.getConcreteSettingForNamespace(contextA.name);

        Setting<?> cacheSizeB = SCRIPT_CACHE_SIZE_SETTING.getConcreteSettingForNamespace(contextB.name);
        Setting<?> compilationRateB = SCRIPT_MAX_COMPILATIONS_RATE_SETTING.getConcreteSettingForNamespace(contextB.name);

        buildScriptService(
            Settings.builder()
                .put(SCRIPT_GENERAL_MAX_COMPILATIONS_RATE_SETTING.getKey(), USE_CONTEXT_RATE_KEY)
                .put(cacheSizeA.getKey(), 1)
                .put(compilationRateA.getKey(), aRate)
                .put(cacheSizeB.getKey(), 2)
                .put(compilationRateB.getKey(), bRate)
                .build()
        );

        // Context A
        scriptService.compile(new Script(ScriptType.INLINE, "test", "1+1", Collections.emptyMap()), contextA);
        scriptService.compile(new Script(ScriptType.INLINE, "test", "2+2", Collections.emptyMap()), contextA);
        GeneralScriptException gse = expectThrows(
            GeneralScriptException.class,
            () -> scriptService.compile(new Script(ScriptType.INLINE, "test", "3+3", Collections.emptyMap()), contextA)
        );
        assertEquals(msg.apply(aRate, contextA.name), gse.getRootCause().getMessage());
        assertEquals(CircuitBreakingException.class, gse.getRootCause().getClass());

        // Context B
        scriptService.compile(new Script(ScriptType.INLINE, "test", "4+4", Collections.emptyMap()), contextB);
        scriptService.compile(new Script(ScriptType.INLINE, "test", "5+5", Collections.emptyMap()), contextB);
        scriptService.compile(new Script(ScriptType.INLINE, "test", "6+6", Collections.emptyMap()), contextB);
        gse = expectThrows(
            GeneralScriptException.class,
            () -> scriptService.compile(new Script(ScriptType.INLINE, "test", "7+7", Collections.emptyMap()), contextB)
        );
        assertEquals(msg.apply(bRate, contextB.name), gse.getRootCause().getMessage());
        gse = expectThrows(
            GeneralScriptException.class,
            () -> scriptService.compile(new Script(ScriptType.INLINE, "test", "8+8", Collections.emptyMap()), contextB)
        );
        assertEquals(msg.apply(bRate, contextB.name), gse.getRootCause().getMessage());
        assertEquals(CircuitBreakingException.class, gse.getRootCause().getClass());

        // Context specific
        ScriptStats stats = scriptService.stats();
        assertEquals(2L, getByContext(stats, contextA.name).getCompilations());
        assertEquals(1L, getByContext(stats, contextA.name).getCacheEvictions());
        assertEquals(1L, getByContext(stats, contextA.name).getCompilationLimitTriggered());

        assertEquals(3L, getByContext(stats, contextB.name).getCompilations());
        assertEquals(1L, getByContext(stats, contextB.name).getCacheEvictions());
        assertEquals(2L, getByContext(stats, contextB.name).getCompilationLimitTriggered());

        // Summed up
        assertEquals(5L, scriptService.stats().getCompilations());
        assertEquals(2L, scriptService.stats().getCacheEvictions());
        assertEquals(3L, scriptService.stats().getCompilationLimitTriggered());

        assertSettingDeprecationsAndWarnings(
            new Setting<?>[] { cacheSizeA, compilationRateA, cacheSizeB, compilationRateB },
            new DeprecationWarning(Level.WARN, USE_CONTEXT_RATE_KEY_DEPRECATION_MESSAGE)
        );
    }

    private ScriptContextStats getByContext(ScriptStats stats, String context) {
        List<ScriptContextStats> maybeContextStats = stats.getContextStats().stream().filter(c -> c.getContext().equals(context)).toList();
        assertEquals(1, maybeContextStats.size());
        return maybeContextStats.get(0);
    }

    public void testStoreScript() throws Exception {
        BytesReference script = BytesReference.bytes(
            XContentFactory.jsonBuilder()
                .startObject()
                .field("script")
                .startObject()
                .field("lang", "_lang")
                .field("source", "abc")
                .endObject()
                .endObject()
        );
        ScriptMetadata scriptMetadata = ScriptMetadata.putStoredScript(null, "_id", StoredScriptSource.parse(script, XContentType.JSON));
        assertNotNull(scriptMetadata);
        assertEquals("abc", scriptMetadata.getStoredScript("_id").getSource());
    }

    public void testDeleteScript() throws Exception {
        ScriptMetadata scriptMetadata = ScriptMetadata.putStoredScript(null, "_id", StoredScriptSource.parse(new BytesArray("""
            {"script": {"lang": "_lang", "source": "abc"} }"""), XContentType.JSON));
        scriptMetadata = ScriptMetadata.deleteStoredScript(scriptMetadata, "_id");
        assertNotNull(scriptMetadata);
        assertNull(scriptMetadata.getStoredScript("_id"));

        ScriptMetadata errorMetadata = scriptMetadata;
        ResourceNotFoundException e = expectThrows(ResourceNotFoundException.class, () -> {
            ScriptMetadata.deleteStoredScript(errorMetadata, "_id");
        });
        assertEquals("stored script [_id] does not exist and cannot be deleted", e.getMessage());
    }

    public void testGetStoredScript() throws Exception {
        buildScriptService(Settings.EMPTY);
        ClusterState cs = ClusterState.builder(new ClusterName("_name"))
            .metadata(
                Metadata.builder()
                    .putCustom(
                        ScriptMetadata.TYPE,
                        new ScriptMetadata.Builder(null).storeScript("_id", StoredScriptSource.parse(new BytesArray("""
                            {"script": {"lang": "_lang", "source": "abc"} }"""), XContentType.JSON)).build()
                    )
            )
            .build();

        assertEquals("abc", ScriptService.getStoredScript(cs, new GetStoredScriptRequest(TEST_REQUEST_TIMEOUT, "_id")).getSource());

        cs = ClusterState.builder(new ClusterName("_name")).build();
        assertNull(ScriptService.getStoredScript(cs, new GetStoredScriptRequest(TEST_REQUEST_TIMEOUT, "_id")));
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
        assertEquals(
            "script.max_size_in_bytes cannot be set to [2], stored script [test1] exceeds the new value with a size of [3]",
            iae.getMessage()
        );
    }

    public void testConflictContextSettings() throws IOException {
        String fieldCacheKey = ScriptService.SCRIPT_CACHE_SIZE_SETTING.getConcreteSettingForNamespace("field").getKey();
        IllegalArgumentException illegal = expectThrows(IllegalArgumentException.class, () -> {
            buildScriptService(
                Settings.builder().put(SCRIPT_GENERAL_MAX_COMPILATIONS_RATE_SETTING.getKey(), "10/1m").put(fieldCacheKey, 123).build()
            );
        });
        assertEquals(
            "Context cache settings [script.context.field.cache_max_size] are incompatible with [script.max_compilations_rate]"
                + " set to non-default value [10/1m]. Either remove the incompatible settings (recommended) or set"
                + " [script.max_compilations_rate] to [use-context] to use per-context settings",
            illegal.getMessage()
        );

        String ingestExpireKey = ScriptService.SCRIPT_CACHE_EXPIRE_SETTING.getConcreteSettingForNamespace("ingest").getKey();
        illegal = expectThrows(IllegalArgumentException.class, () -> {
            buildScriptService(
                Settings.builder().put(SCRIPT_GENERAL_MAX_COMPILATIONS_RATE_SETTING.getKey(), "10/1m").put(ingestExpireKey, "5m").build()
            );
        });

        assertEquals(
            "Context cache settings [script.context.ingest.cache_expire] are incompatible with [script.max_compilations_rate]"
                + " set to non-default value [10/1m]. Either remove the incompatible settings (recommended) or set"
                + " [script.max_compilations_rate] to [use-context] to use per-context settings",
            illegal.getMessage()
        );

        String scoreCompileKey = ScriptService.SCRIPT_MAX_COMPILATIONS_RATE_SETTING.getConcreteSettingForNamespace("score").getKey();
        illegal = expectThrows(IllegalArgumentException.class, () -> {
            buildScriptService(
                Settings.builder().put(SCRIPT_GENERAL_MAX_COMPILATIONS_RATE_SETTING.getKey(), "10/1m").put(scoreCompileKey, "50/5m").build()
            );
        });

        assertEquals(
            "Context cache settings [script.context.score.max_compilations_rate] are incompatible with [script.max_compilations_rate]"
                + " set to non-default value [10/1m]. Either remove the incompatible settings (recommended) or set"
                + " [script.max_compilations_rate] to [use-context] to use per-context settings",
            illegal.getMessage()
        );

        Setting<?> ingestExpire = ScriptService.SCRIPT_CACHE_EXPIRE_SETTING.getConcreteSettingForNamespace("ingest");
        Setting<?> fieldSize = ScriptService.SCRIPT_CACHE_SIZE_SETTING.getConcreteSettingForNamespace("field");
        Setting<?> scoreCompilation = ScriptService.SCRIPT_MAX_COMPILATIONS_RATE_SETTING.getConcreteSettingForNamespace("score");

        buildScriptService(
            Settings.builder()
                .put(SCRIPT_GENERAL_MAX_COMPILATIONS_RATE_SETTING.getKey(), USE_CONTEXT_RATE_KEY)
                .put(ingestExpire.getKey(), "5m")
                .put(fieldSize.getKey(), 123)
                .put(scoreCompilation.getKey(), "50/5m")
                .build()
        );
        assertSettingDeprecationsAndWarnings(
            new Setting<?>[] { ingestExpire, fieldSize, scoreCompilation },
            new DeprecationWarning(Level.WARN, USE_CONTEXT_RATE_KEY_DEPRECATION_MESSAGE),
            new DeprecationWarning(Level.WARN, implicitContextCacheMessage(fieldCacheKey)),
            new DeprecationWarning(Level.WARN, implicitContextCacheMessage(ingestExpireKey)),
            new DeprecationWarning(Level.WARN, implicitContextCacheMessage(scoreCompileKey))
        );
    }

    protected static String implicitContextCacheMessage(String... settings) {
        return "Implicitly using the script context cache is deprecated, remove settings ["
            + Arrays.stream(settings).sorted().collect(Collectors.joining(", "))
            + "] to use the script general cache.";
    }

    public void testFallbackContextSettings() {
        int cacheSizeBackup = randomIntBetween(0, 1024);
        int cacheSizeFoo = randomValueOtherThan(cacheSizeBackup, () -> randomIntBetween(0, 1024));

        var cacheExpireBackupTimeValue = randomTimeValue(1, 1000, TimeUnit.HOURS);
        var cacheExpireFooTimeValue = randomValueOtherThan(cacheExpireBackupTimeValue, () -> randomTimeValue(1, 1000, TimeUnit.HOURS));

        Setting<?> cacheSizeSetting = ScriptService.SCRIPT_CACHE_SIZE_SETTING.getConcreteSettingForNamespace("foo");
        Setting<?> cacheExpireSetting = ScriptService.SCRIPT_CACHE_EXPIRE_SETTING.getConcreteSettingForNamespace("foo");
        Settings s = Settings.builder()
            .put(SCRIPT_GENERAL_CACHE_SIZE_SETTING.getKey(), cacheSizeBackup)
            .put(cacheSizeSetting.getKey(), cacheSizeFoo)
            .put(SCRIPT_GENERAL_CACHE_EXPIRE_SETTING.getKey(), cacheExpireBackupTimeValue)
            .put(cacheExpireSetting.getKey(), cacheExpireFooTimeValue)
            .build();

        assertEquals(cacheSizeFoo, ScriptService.SCRIPT_CACHE_SIZE_SETTING.getConcreteSettingForNamespace("foo").get(s).intValue());
        assertEquals(cacheSizeBackup, ScriptService.SCRIPT_CACHE_SIZE_SETTING.getConcreteSettingForNamespace("bar").get(s).intValue());

        assertEquals(cacheExpireFooTimeValue, ScriptService.SCRIPT_CACHE_EXPIRE_SETTING.getConcreteSettingForNamespace("foo").get(s));
        assertEquals(cacheExpireBackupTimeValue, ScriptService.SCRIPT_CACHE_EXPIRE_SETTING.getConcreteSettingForNamespace("bar").get(s));
        assertSettingDeprecationsAndWarnings(new Setting<?>[] { cacheExpireSetting, cacheExpireSetting });
    }

    public void testUseContextSettingValue() {
        Setting<?> contextMaxCompilationRate = ScriptService.SCRIPT_MAX_COMPILATIONS_RATE_SETTING.getConcreteSettingForNamespace("foo");
        Settings s = Settings.builder()
            .put(ScriptService.SCRIPT_GENERAL_MAX_COMPILATIONS_RATE_SETTING.getKey(), ScriptService.USE_CONTEXT_RATE_KEY)
            .put(contextMaxCompilationRate.getKey(), ScriptService.USE_CONTEXT_RATE_KEY)
            .build();

        assertEquals(ScriptService.SCRIPT_GENERAL_MAX_COMPILATIONS_RATE_SETTING.get(s), ScriptService.USE_CONTEXT_RATE_VALUE);

        IllegalArgumentException illegal = expectThrows(IllegalArgumentException.class, () -> {
            ScriptService.SCRIPT_MAX_COMPILATIONS_RATE_SETTING.getAsMap(s);
        });

        assertEquals("parameter must contain a positive integer and a timevalue, i.e. 10/1m, but was [use-context]", illegal.getMessage());
        assertSettingDeprecationsAndWarnings(new Setting<?>[] { contextMaxCompilationRate });
    }

    public void testCacheHolderGeneralConstructor() throws IOException {
        String compilationRate = "77/5m";
        buildScriptService(Settings.builder().put(SCRIPT_GENERAL_MAX_COMPILATIONS_RATE_SETTING.getKey(), compilationRate).build());

        ScriptService.CacheHolder holder = scriptService.cacheHolder.get();

        assertNotNull(holder.general);
        assertNull(holder.contextCache);
        assertEquals(holder.general.rate, new ScriptCache.CompilationRate(compilationRate));
    }

    public void testCacheHolderContextConstructor() throws IOException {
        String a = randomFrom(rateLimitedContexts.keySet());
        String b = randomValueOtherThan(a, () -> randomFrom(rateLimitedContexts.keySet()));
        String aCompilationRate = "77/5m";
        String bCompilationRate = "78/6m";

        Setting<?> aSetting = SCRIPT_MAX_COMPILATIONS_RATE_SETTING.getConcreteSettingForNamespace(a);
        Setting<?> bSetting = SCRIPT_MAX_COMPILATIONS_RATE_SETTING.getConcreteSettingForNamespace(b);
        buildScriptService(
            Settings.builder()
                .put(SCRIPT_GENERAL_MAX_COMPILATIONS_RATE_SETTING.getKey(), USE_CONTEXT_RATE_KEY)
                .put(aSetting.getKey(), aCompilationRate)
                .put(bSetting.getKey(), bCompilationRate)
                .build()
        );

        assertNull(scriptService.cacheHolder.get().general);
        assertNotNull(scriptService.cacheHolder.get().contextCache);
        assertEquals(contexts.keySet(), scriptService.cacheHolder.get().contextCache.keySet());

        assertEquals(new ScriptCache.CompilationRate(aCompilationRate), scriptService.cacheHolder.get().contextCache.get(a).get().rate);
        assertEquals(new ScriptCache.CompilationRate(bCompilationRate), scriptService.cacheHolder.get().contextCache.get(b).get().rate);
        assertSettingDeprecationsAndWarnings(
            new Setting<?>[] { aSetting, bSetting },
            new DeprecationWarning(Level.WARN, USE_CONTEXT_RATE_KEY_DEPRECATION_MESSAGE)
        );
    }

    public void testImplicitContextCache() throws IOException {
        String a = randomFrom(rateLimitedContexts.keySet());
        String b = randomValueOtherThan(a, () -> randomFrom(rateLimitedContexts.keySet()));
        String aCompilationRate = "77/5m";
        String bCompilationRate = "78/6m";

        Setting<?> aSetting = SCRIPT_MAX_COMPILATIONS_RATE_SETTING.getConcreteSettingForNamespace(a);
        Setting<?> bSetting = SCRIPT_MAX_COMPILATIONS_RATE_SETTING.getConcreteSettingForNamespace(b);
        buildScriptService(Settings.builder().put(aSetting.getKey(), aCompilationRate).put(bSetting.getKey(), bCompilationRate).build());

        assertNull(scriptService.cacheHolder.get().general);
        assertNotNull(scriptService.cacheHolder.get().contextCache);
        assertEquals(contexts.keySet(), scriptService.cacheHolder.get().contextCache.keySet());

        assertEquals(new ScriptCache.CompilationRate(aCompilationRate), scriptService.cacheHolder.get().contextCache.get(a).get().rate);
        assertEquals(new ScriptCache.CompilationRate(bCompilationRate), scriptService.cacheHolder.get().contextCache.get(b).get().rate);
        assertSettingDeprecationsAndWarnings(
            new Setting<?>[] { aSetting, bSetting },
            new DeprecationWarning(Level.WARN, implicitContextCacheMessage(aSetting.getKey(), bSetting.getKey()))
        );
    }

    public void testImplicitContextCacheWithoutCompilationRate() throws IOException {
        String a = randomFrom(rateLimitedContexts.keySet());
        String b = randomValueOtherThan(a, () -> randomFrom(rateLimitedContexts.keySet()));
        String aExpire = "20m";
        int bSize = 2000;

        Setting<?> aSetting = SCRIPT_CACHE_EXPIRE_SETTING.getConcreteSettingForNamespace(a);
        Setting<?> bSetting = SCRIPT_CACHE_SIZE_SETTING.getConcreteSettingForNamespace(b);
        buildScriptService(Settings.builder().put(aSetting.getKey(), aExpire).put(bSetting.getKey(), bSize).build());

        assertNull(scriptService.cacheHolder.get().general);
        assertNotNull(scriptService.cacheHolder.get().contextCache);
        assertEquals(contexts.keySet(), scriptService.cacheHolder.get().contextCache.keySet());

        assertEquals(
            TimeValue.parseTimeValue("20m", aSetting.getKey()),
            scriptService.cacheHolder.get().contextCache.get(a).get().cacheExpire
        );
        assertEquals(bSize, scriptService.cacheHolder.get().contextCache.get(b).get().cacheSize);
        assertSettingDeprecationsAndWarnings(
            new Setting<?>[] { aSetting, bSetting },
            new DeprecationWarning(Level.WARN, implicitContextCacheMessage(aSetting.getKey(), bSetting.getKey()))
        );
    }

    public void testCompilationRateUnlimitedContextOnly() throws IOException {
        IllegalArgumentException illegal = expectThrows(IllegalArgumentException.class, () -> {
            buildScriptService(
                Settings.builder()
                    .put(SCRIPT_GENERAL_MAX_COMPILATIONS_RATE_SETTING.getKey(), ScriptService.UNLIMITED_COMPILATION_RATE_KEY)
                    .build()
            );
        });
        assertEquals("parameter must contain a positive integer and a timevalue, i.e. 10/1m, but was [unlimited]", illegal.getMessage());

        Setting<?> field = SCRIPT_MAX_COMPILATIONS_RATE_SETTING.getConcreteSettingForNamespace("field");
        Setting<?> ingest = SCRIPT_MAX_COMPILATIONS_RATE_SETTING.getConcreteSettingForNamespace("ingest");
        // Should not throw.
        buildScriptService(
            Settings.builder()
                .put(field.getKey(), ScriptService.UNLIMITED_COMPILATION_RATE_KEY)
                .put(ingest.getKey(), ScriptService.UNLIMITED_COMPILATION_RATE_KEY)
                .put(SCRIPT_GENERAL_MAX_COMPILATIONS_RATE_SETTING.getKey(), ScriptService.USE_CONTEXT_RATE_KEY)
                .build()
        );
        assertSettingDeprecationsAndWarnings(
            new Setting<?>[] { field, ingest },
            new DeprecationWarning(Level.WARN, USE_CONTEXT_RATE_KEY_DEPRECATION_MESSAGE)
        );
    }

    public void testDisableCompilationRateSetting() throws IOException {
        IllegalArgumentException illegal = expectThrows(IllegalArgumentException.class, () -> {
            buildScriptService(
                Settings.builder()
                    .put("script.max_compilations_rate", "use-context")
                    .put("script.context.ingest.max_compilations_rate", "76/10m")
                    .put("script.context.field.max_compilations_rate", "77/10m")
                    .put("script.disable_max_compilations_rate", true)
                    .build()
            );
        });
        assertEquals(
            "Cannot set custom context compilation rates [script.context.field.max_compilations_rate, "
                + "script.context.ingest.max_compilations_rate] if compile rates disabled via "
                + "[script.disable_max_compilations_rate]",
            illegal.getMessage()
        );

        illegal = expectThrows(IllegalArgumentException.class, () -> {
            buildScriptService(
                Settings.builder().put("script.disable_max_compilations_rate", true).put("script.max_compilations_rate", "76/10m").build()
            );
        });
        assertEquals(
            "Cannot set custom general compilation rates [script.max_compilations_rate] "
                + "to [76/10m] if compile rates disabled via [script.disable_max_compilations_rate]",
            illegal.getMessage()
        );

        buildScriptService(Settings.builder().put("script.disable_max_compilations_rate", true).build());
        assertSettingDeprecationsAndWarnings(
            new Setting<?>[] {
                SCRIPT_MAX_COMPILATIONS_RATE_SETTING.getConcreteSettingForNamespace("field"),
                SCRIPT_MAX_COMPILATIONS_RATE_SETTING.getConcreteSettingForNamespace("ingest") },
            new DeprecationWarning(Level.WARN, USE_CONTEXT_RATE_KEY_DEPRECATION_MESSAGE)
        );
    }

    public void testCacheHolderChangeSettings() throws IOException {
        Set<String> contextNames = rateLimitedContexts.keySet();
        String a = randomFrom(contextNames);
        String aRate = "77/5m";
        String b = randomValueOtherThan(a, () -> randomFrom(contextNames));
        String bRate = "78/6m";
        String c = randomValueOtherThanMany(s -> a.equals(s) || b.equals(s), () -> randomFrom(contextNames));
        String compilationRate = "77/5m";
        ScriptCache.CompilationRate generalRate = new ScriptCache.CompilationRate(compilationRate);

        Settings s = Settings.builder().put(SCRIPT_GENERAL_MAX_COMPILATIONS_RATE_SETTING.getKey(), compilationRate).build();

        buildScriptService(s);

        assertNotNull(scriptService.cacheHolder.get().general);
        // Set should not throw when using general cache
        scriptService.cacheHolder.get().set(c, scriptService.contextCache(s, contexts.get(c)));
        assertNull(scriptService.cacheHolder.get().contextCache);
        assertEquals(generalRate, scriptService.cacheHolder.get().general.rate);

        Setting<?> compilationRateA = SCRIPT_MAX_COMPILATIONS_RATE_SETTING.getConcreteSettingForNamespace(a);
        Setting<?> compilationRateB = SCRIPT_MAX_COMPILATIONS_RATE_SETTING.getConcreteSettingForNamespace(b);
        Setting<?> compilationRateC = SCRIPT_MAX_COMPILATIONS_RATE_SETTING.getConcreteSettingForNamespace(c);

        scriptService.setCacheHolder(
            Settings.builder()
                .put(SCRIPT_GENERAL_MAX_COMPILATIONS_RATE_SETTING.getKey(), USE_CONTEXT_RATE_KEY)
                .put(compilationRateA.getKey(), aRate)
                .put(compilationRateB.getKey(), bRate)
                .put(compilationRateC.getKey(), ScriptService.UNLIMITED_COMPILATION_RATE_KEY)
                .build()
        );

        assertNull(scriptService.cacheHolder.get().general);
        assertNotNull(scriptService.cacheHolder.get().contextCache);
        // get of missing context should be null
        assertNull(
            scriptService.cacheHolder.get().get(randomValueOtherThanMany(contexts.keySet()::contains, () -> randomAlphaOfLength(8)))
        );
        assertEquals(contexts.keySet(), scriptService.cacheHolder.get().contextCache.keySet());

        assertEquals(new ScriptCache.CompilationRate(aRate), scriptService.cacheHolder.get().contextCache.get(a).get().rate);
        assertEquals(new ScriptCache.CompilationRate(bRate), scriptService.cacheHolder.get().contextCache.get(b).get().rate);
        assertEquals(ScriptCache.UNLIMITED_COMPILATION_RATE, scriptService.cacheHolder.get().contextCache.get(c).get().rate);

        scriptService.cacheHolder.get()
            .set(
                b,
                scriptService.contextCache(
                    Settings.builder().put(SCRIPT_MAX_COMPILATIONS_RATE_SETTING.getConcreteSettingForNamespace(b).getKey(), aRate).build(),
                    contexts.get(b)
                )
            );
        assertEquals(new ScriptCache.CompilationRate(aRate), scriptService.cacheHolder.get().contextCache.get(b).get().rate);

        scriptService.setCacheHolder(s);
        assertNotNull(scriptService.cacheHolder.get().general);
        assertNull(scriptService.cacheHolder.get().contextCache);
        assertEquals(generalRate, scriptService.cacheHolder.get().general.rate);

        scriptService.setCacheHolder(Settings.builder().put(SCRIPT_GENERAL_MAX_COMPILATIONS_RATE_SETTING.getKey(), bRate).build());

        assertNotNull(scriptService.cacheHolder.get().general);
        assertNull(scriptService.cacheHolder.get().contextCache);
        assertEquals(new ScriptCache.CompilationRate(bRate), scriptService.cacheHolder.get().general.rate);

        ScriptService.CacheHolder holder = scriptService.cacheHolder.get();
        scriptService.setCacheHolder(Settings.builder().put(SCRIPT_GENERAL_MAX_COMPILATIONS_RATE_SETTING.getKey(), bRate).build());
        assertEquals(holder, scriptService.cacheHolder.get());

        assertSettingDeprecationsAndWarnings(new Setting<?>[] { compilationRateA, compilationRateB, compilationRateC });
    }

    public void testFallbackToContextDefaults() throws IOException {
        String contextRateStr = randomIntBetween(10, 1024) + "/" + randomIntBetween(10, 200) + "m";
        ScriptCache.CompilationRate contextRate = new ScriptCache.CompilationRate(contextRateStr);
        int contextCacheSize = randomIntBetween(1, 1024);
        TimeValue contextExpire = TimeValue.timeValueMinutes(randomIntBetween(10, 200));

        buildScriptService(Settings.builder().put(SCRIPT_GENERAL_MAX_COMPILATIONS_RATE_SETTING.getKey(), "75/5m").build());

        String name = "score";

        Setting<?> cacheSizeContextSetting = SCRIPT_CACHE_SIZE_SETTING.getConcreteSettingForNamespace(name);
        Setting<?> cacheExpireContextSetting = SCRIPT_CACHE_EXPIRE_SETTING.getConcreteSettingForNamespace(name);
        Setting<?> compilationRateContextSetting = SCRIPT_MAX_COMPILATIONS_RATE_SETTING.getConcreteSettingForNamespace(name);
        // Use context specific
        scriptService.setCacheHolder(
            Settings.builder()
                .put(SCRIPT_GENERAL_MAX_COMPILATIONS_RATE_SETTING.getKey(), USE_CONTEXT_RATE_KEY)
                .put(cacheSizeContextSetting.getKey(), contextCacheSize)
                .put(cacheExpireContextSetting.getKey(), contextExpire)
                .put(compilationRateContextSetting.getKey(), contextRateStr)
                .build()
        );

        ScriptService.CacheHolder holder = scriptService.cacheHolder.get();
        assertNotNull(holder.contextCache);
        assertNotNull(holder.contextCache.get(name));
        assertNotNull(holder.contextCache.get(name).get());

        assertEquals(contextRate, holder.contextCache.get(name).get().rate);
        assertEquals(contextCacheSize, holder.contextCache.get(name).get().cacheSize);
        assertEquals(contextExpire, holder.contextCache.get(name).get().cacheExpire);

        ScriptContext<?> score = contexts.get(name);
        // Fallback to context defaults
        buildScriptService(Settings.builder().put(SCRIPT_GENERAL_MAX_COMPILATIONS_RATE_SETTING.getKey(), USE_CONTEXT_RATE_KEY).build());

        holder = scriptService.cacheHolder.get();
        assertNotNull(holder.contextCache);
        assertNotNull(holder.contextCache.get(name));
        assertNotNull(holder.contextCache.get(name).get());

        assertEquals(ScriptContext.DEFAULT_COMPILATION_RATE_LIMIT, holder.contextCache.get(name).get().rate.asTuple());
        assertEquals(score.cacheSizeDefault, holder.contextCache.get(name).get().cacheSize);
        assertEquals(score.cacheExpireDefault, holder.contextCache.get(name).get().cacheExpire);

        assertSettingDeprecationsAndWarnings(
            new Setting<?>[] { cacheSizeContextSetting, cacheExpireContextSetting, compilationRateContextSetting },
            new DeprecationWarning(Level.WARN, USE_CONTEXT_RATE_KEY_DEPRECATION_MESSAGE)
        );
    }

    protected HashMap<String, ScriptContext<?>> compilationRateLimitedContexts() {
        HashMap<String, ScriptContext<?>> rateLimited = new HashMap<>();
        for (Map.Entry<String, ScriptContext<?>> entry : contexts.entrySet()) {
            if (entry.getValue().compilationRateLimited) {
                rateLimited.put(entry.getKey(), entry.getValue());
            }
        }
        return rateLimited;
    }

    private void assertCompileRejected(String lang, String script, ScriptType scriptType, ScriptContext<?> scriptContext) {
        try {
            scriptService.compile(new Script(scriptType, lang, script, Collections.emptyMap()), scriptContext);
            fail(
                "compile should have been rejected for lang ["
                    + lang
                    + "], "
                    + "script_type ["
                    + scriptType
                    + "], scripted_op ["
                    + scriptContext
                    + "]"
            );
        } catch (IllegalArgumentException | IllegalStateException e) {
            // pass
        }
    }

    private void assertCompileAccepted(String lang, String script, ScriptType scriptType, ScriptContext<?> scriptContext) {
        assertThat(scriptService.compile(new Script(scriptType, lang, script, Collections.emptyMap()), scriptContext), notNullValue());
    }
}
