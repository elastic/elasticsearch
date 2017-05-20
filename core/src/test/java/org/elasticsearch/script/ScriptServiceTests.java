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

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.admin.cluster.storedscripts.GetStoredScriptRequest;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.env.Environment;
import org.elasticsearch.search.lookup.SearchLookup;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.sameInstance;

//TODO: this needs to be a base test class, and all scripting engines extend it
public class ScriptServiceTests extends ESTestCase {

    private ScriptEngine scriptEngine;
    private ScriptEngine dangerousScriptEngine;
    private Map<String, ScriptEngine> engines;
    private ScriptContextRegistry scriptContextRegistry;
    private ScriptContext[] scriptContexts;
    private ScriptService scriptService;
    private Settings baseSettings;

    private static final Map<ScriptType, Boolean> DEFAULT_SCRIPT_ENABLED = new HashMap<>();

    static {
        DEFAULT_SCRIPT_ENABLED.put(ScriptType.STORED, false);
        DEFAULT_SCRIPT_ENABLED.put(ScriptType.INLINE, false);
    }

    @Before
    public void setup() throws IOException {
        Path genericConfigFolder = createTempDir();
        baseSettings = Settings.builder()
                .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString())
                .put(Environment.PATH_CONF_SETTING.getKey(), genericConfigFolder)
                .put(ScriptService.SCRIPT_MAX_COMPILATIONS_PER_MINUTE.getKey(), 10000)
                .build();
        scriptEngine = new TestEngine();
        TestEngine defaultScriptServiceEngine = new TestEngine(Script.DEFAULT_SCRIPT_LANG) {};
        //randomly register custom script contexts
        int randomInt = randomIntBetween(0, 3);
        //prevent duplicates using map
        Map<String, ScriptContext.Plugin> contexts = new HashMap<>();
        for (int i = 0; i < randomInt; i++) {
            String plugin;
            do {
                plugin = randomAlphaOfLength(randomIntBetween(1, 10));
            } while (ScriptContextRegistry.RESERVED_SCRIPT_CONTEXTS.contains(plugin));
            String operation;
            do {
                operation = randomAlphaOfLength(randomIntBetween(1, 30));
            } while (ScriptContextRegistry.RESERVED_SCRIPT_CONTEXTS.contains(operation));
            String context = plugin + "_" + operation;
            contexts.put(context, new ScriptContext.Plugin(plugin, operation));
        }
        engines = new HashMap<>();
        engines.put(scriptEngine.getType(), scriptEngine);
        engines.put(defaultScriptServiceEngine.getType(), defaultScriptServiceEngine);
        scriptContextRegistry = new ScriptContextRegistry(contexts.values());
        scriptContexts = scriptContextRegistry.scriptContexts().toArray(new ScriptContext[scriptContextRegistry.scriptContexts().size()]);
        logger.info("--> setup script service");
    }

    private void buildScriptService(Settings additionalSettings) throws IOException {
        Settings finalSettings = Settings.builder().put(baseSettings).put(additionalSettings).build();
        scriptService = new ScriptService(finalSettings, engines, scriptContextRegistry) {
            @Override
            StoredScriptSource getScriptFromClusterState(String id, String lang) {
                //mock the script that gets retrieved from an index
                return new StoredScriptSource(lang, "100", Collections.emptyMap());
            }
        };
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

    public void testNotSupportedDisableDynamicSetting() throws IOException {
        try {
            buildScriptService(Settings.builder().put(ScriptService.DISABLE_DYNAMIC_SCRIPTING_SETTING, randomUnicodeOfLength(randomIntBetween(1, 10))).build());
            fail("script service should have thrown exception due to non supported script.disable_dynamic setting");
        } catch(IllegalArgumentException e) {
            assertThat(e.getMessage(), containsString(ScriptService.DISABLE_DYNAMIC_SCRIPTING_SETTING + " is not a supported setting, replace with fine-grained script settings"));
        }
    }

    public void testInlineScriptCompiledOnceCache() throws IOException {
        buildScriptService(Settings.EMPTY);
        CompiledScript compiledScript1 = scriptService.compile(new Script(ScriptType.INLINE, "test", "1+1", Collections.emptyMap()),
                randomFrom(scriptContexts));
        CompiledScript compiledScript2 = scriptService.compile(new Script(ScriptType.INLINE, "test", "1+1", Collections.emptyMap()),
                randomFrom(scriptContexts));
        assertThat(compiledScript1.compiled(), sameInstance(compiledScript2.compiled()));
    }

    public void testAllowAllScriptTypeSettings() throws IOException {
        buildScriptService(Settings.EMPTY);

        assertCompileAccepted("painless", "script", ScriptType.INLINE, ScriptContext.Standard.SEARCH);
        assertCompileAccepted("painless", "script", ScriptType.STORED, ScriptContext.Standard.SEARCH);
    }

    public void testAllowAllScriptContextSettings() throws IOException {
        buildScriptService(Settings.EMPTY);

        assertCompileAccepted("painless", "script", ScriptType.INLINE, ScriptContext.Standard.SEARCH);
        assertCompileAccepted("painless", "script", ScriptType.INLINE, ScriptContext.Standard.AGGS);
        assertCompileAccepted("painless", "script", ScriptType.INLINE, ScriptContext.Standard.UPDATE);
        assertCompileAccepted("painless", "script", ScriptType.INLINE, ScriptContext.Standard.INGEST);
    }

    public void testAllowSomeScriptTypeSettings() throws IOException {
        Settings.Builder builder = Settings.builder();
        builder.put("script.types_allowed", "inline");
        buildScriptService(builder.build());

        assertCompileAccepted("painless", "script", ScriptType.INLINE, ScriptContext.Standard.SEARCH);
        assertCompileRejected("painless", "script", ScriptType.STORED, ScriptContext.Standard.SEARCH);
    }

    public void testAllowSomeScriptContextSettings() throws IOException {
        Settings.Builder builder = Settings.builder();
        builder.put("script.contexts_allowed", "search, aggs");
        buildScriptService(builder.build());

        assertCompileAccepted("painless", "script", ScriptType.INLINE, ScriptContext.Standard.SEARCH);
        assertCompileAccepted("painless", "script", ScriptType.INLINE, ScriptContext.Standard.AGGS);
        assertCompileRejected("painless", "script", ScriptType.INLINE, ScriptContext.Standard.UPDATE);
    }

    public void testAllowNoScriptTypeSettings() throws IOException {
        Settings.Builder builder = Settings.builder();
        builder.put("script.types_allowed", "none");
        buildScriptService(builder.build());

        assertCompileRejected("painless", "script", ScriptType.INLINE, ScriptContext.Standard.SEARCH);
        assertCompileRejected("painless", "script", ScriptType.STORED, ScriptContext.Standard.SEARCH);
    }

    public void testAllowNoScriptContextSettings() throws IOException {
        Settings.Builder builder = Settings.builder();
        builder.put("script.contexts_allowed", "none");
        buildScriptService(builder.build());

        assertCompileRejected("painless", "script", ScriptType.INLINE, ScriptContext.Standard.SEARCH);
        assertCompileRejected("painless", "script", ScriptType.INLINE, ScriptContext.Standard.AGGS);
        assertCompileRejected("painless", "script", ScriptType.INLINE, ScriptContext.Standard.UPDATE);
        assertCompileRejected("painless", "script", ScriptType.INLINE, ScriptContext.Standard.INGEST);
    }

    public void testCompileNonRegisteredContext() throws IOException {
        buildScriptService(Settings.EMPTY);
        String pluginName;
        String unknownContext;
        do {
            pluginName = randomAlphaOfLength(randomIntBetween(1, 10));
            unknownContext = randomAlphaOfLength(randomIntBetween(1, 30));
        } while(scriptContextRegistry.isSupportedContext(new ScriptContext.Plugin(pluginName, unknownContext).getKey()));

        String type = scriptEngine.getType();
        try {
            scriptService.compile(new Script(randomFrom(ScriptType.values()), type, "test", Collections.emptyMap()),
                new ScriptContext.Plugin(pluginName, unknownContext));
            fail("script compilation should have been rejected");
        } catch(IllegalArgumentException e) {
            assertThat(e.getMessage(), containsString("script context [" + pluginName + "_" + unknownContext + "] not supported"));
        }
    }

    public void testCompileCountedInCompilationStats() throws IOException {
        buildScriptService(Settings.EMPTY);
        scriptService.compile(new Script(ScriptType.INLINE, "test", "1+1", Collections.emptyMap()), randomFrom(scriptContexts));
        assertEquals(1L, scriptService.stats().getCompilations());
    }

    public void testExecutableCountedInCompilationStats() throws IOException {
        buildScriptService(Settings.EMPTY);
        Script script = new Script(ScriptType.INLINE, "test", "1+1", Collections.emptyMap());
        CompiledScript compiledScript = scriptService.compile(script, randomFrom(scriptContexts));
        scriptService.executable(compiledScript, script.getParams());
        assertEquals(1L, scriptService.stats().getCompilations());
    }

    public void testSearchCountedInCompilationStats() throws IOException {
        buildScriptService(Settings.EMPTY);
        scriptService.search(null, new Script(ScriptType.INLINE, "test", "1+1", Collections.emptyMap()), randomFrom(scriptContexts));
        assertEquals(1L, scriptService.stats().getCompilations());
    }

    public void testMultipleCompilationsCountedInCompilationStats() throws IOException {
        buildScriptService(Settings.EMPTY);
        int numberOfCompilations = randomIntBetween(1, 1024);
        for (int i = 0; i < numberOfCompilations; i++) {
            scriptService
                    .compile(new Script(ScriptType.INLINE, "test", i + " + " + i, Collections.emptyMap()), randomFrom(scriptContexts));
        }
        assertEquals(numberOfCompilations, scriptService.stats().getCompilations());
    }

    public void testCompilationStatsOnCacheHit() throws IOException {
        Settings.Builder builder = Settings.builder();
        builder.put(ScriptService.SCRIPT_CACHE_SIZE_SETTING.getKey(), 1);
        buildScriptService(builder.build());
        Script script = new Script(ScriptType.INLINE, "test", "1+1", Collections.emptyMap());
        scriptService.compile(script, randomFrom(scriptContexts));
        scriptService.compile(script, randomFrom(scriptContexts));
        assertEquals(1L, scriptService.stats().getCompilations());
    }

    public void testIndexedScriptCountedInCompilationStats() throws IOException {
        buildScriptService(Settings.EMPTY);
        scriptService.compile(new Script(ScriptType.STORED, "test", "script", Collections.emptyMap()), randomFrom(scriptContexts));
        assertEquals(1L, scriptService.stats().getCompilations());
    }

    public void testCacheEvictionCountedInCacheEvictionsStats() throws IOException {
        Settings.Builder builder = Settings.builder();
        builder.put(ScriptService.SCRIPT_CACHE_SIZE_SETTING.getKey(), 1);
        buildScriptService(builder.build());
        scriptService.compile(new Script(ScriptType.INLINE, "test", "1+1", Collections.emptyMap()), randomFrom(scriptContexts));
        scriptService.compile(new Script(ScriptType.INLINE, "test", "2+2", Collections.emptyMap()), randomFrom(scriptContexts));
        assertEquals(2L, scriptService.stats().getCompilations());
        assertEquals(1L, scriptService.stats().getCacheEvictions());
    }

    public void testDefaultLanguage() throws IOException {
        Settings.Builder builder = Settings.builder();
        buildScriptService(builder.build());
        CompiledScript script = scriptService.compile(
            new Script(ScriptType.INLINE, Script.DEFAULT_SCRIPT_LANG, "1 + 1", Collections.emptyMap()), randomFrom(scriptContexts));
        assertEquals(script.lang(), Script.DEFAULT_SCRIPT_LANG);
    }

    public void testStoreScript() throws Exception {
        BytesReference script = XContentFactory.jsonBuilder().startObject()
                    .field("script", "abc")
                .endObject().bytes();

        ScriptMetaData scriptMetaData = ScriptMetaData.putStoredScript(null, "_id",
            StoredScriptSource.parse("_lang", script, XContentType.JSON));
        assertNotNull(scriptMetaData);
        assertEquals("abc", scriptMetaData.getStoredScript("_id", "_lang").getCode());
    }

    public void testDeleteScript() throws Exception {
        ScriptMetaData scriptMetaData = ScriptMetaData.putStoredScript(null, "_id",
            StoredScriptSource.parse("_lang", new BytesArray("{\"script\":\"abc\"}"), XContentType.JSON));
        scriptMetaData = ScriptMetaData.deleteStoredScript(scriptMetaData, "_id", "_lang");
        assertNotNull(scriptMetaData);
        assertNull(scriptMetaData.getStoredScript("_id", "_lang"));

        ScriptMetaData errorMetaData = scriptMetaData;
        ResourceNotFoundException e = expectThrows(ResourceNotFoundException.class, () -> {
            ScriptMetaData.deleteStoredScript(errorMetaData, "_id", "_lang");
        });
        assertEquals("stored script [_id] using lang [_lang] does not exist and cannot be deleted", e.getMessage());
    }

    public void testGetStoredScript() throws Exception {
        buildScriptService(Settings.EMPTY);
        ClusterState cs = ClusterState.builder(new ClusterName("_name"))
            .metaData(MetaData.builder()
                .putCustom(ScriptMetaData.TYPE,
                    new ScriptMetaData.Builder(null).storeScript("_id",
                        StoredScriptSource.parse("_lang", new BytesArray("{\"script\":\"abc\"}"), XContentType.JSON)).build()))
            .build();

        assertEquals("abc", scriptService.getStoredScript(cs, new GetStoredScriptRequest("_id", "_lang")).getCode());
        assertNull(scriptService.getStoredScript(cs, new GetStoredScriptRequest("_id2", "_lang")));

        cs = ClusterState.builder(new ClusterName("_name")).build();
        assertNull(scriptService.getStoredScript(cs, new GetStoredScriptRequest("_id", "_lang")));
    }

    private void assertCompileRejected(String lang, String script, ScriptType scriptType, ScriptContext scriptContext) {
        try {
            scriptService.compile(new Script(scriptType, lang, script, Collections.emptyMap()), scriptContext);
            fail("compile should have been rejected for lang [" + lang + "], script_type [" + scriptType + "], scripted_op [" + scriptContext + "]");
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

    public static class TestEngine implements ScriptEngine {

        public static final String NAME = "test";

        private final String name;

        public TestEngine() {
            this(NAME);
        }

        public TestEngine(String name) {
            this.name = name;
        }

        @Override
        public String getType() {
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
    }
}
