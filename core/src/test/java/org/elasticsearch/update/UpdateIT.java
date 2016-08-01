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

package org.elasticsearch.update;

import org.elasticsearch.ElasticsearchTimeoutException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateRequestBuilder;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.transport.NoNodeAvailableException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.MergePolicyConfig;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.index.engine.DocumentMissingException;
import org.elasticsearch.index.engine.VersionConflictEngineException;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.ScriptPlugin;
import org.elasticsearch.script.CompiledScript;
import org.elasticsearch.script.ExecutableScript;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptEngineService;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.script.SearchScript;
import org.elasticsearch.search.lookup.SearchLookup;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.InternalSettingsPlugin;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertThrows;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class UpdateIT extends ESIntegTestCase {

    public static class PutFieldValuesScriptPlugin extends Plugin implements ScriptPlugin {
        @Override
        public ScriptEngineService getScriptEngineService(Settings settings) {
            return new PutFieldValuesScriptEngine();
        }
    }

    public static class PutFieldValuesScriptEngine implements ScriptEngineService {

        public static final String NAME = "put_values";

        @Override
        public void close() throws IOException {
        }

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
            return new Object(); // unused
        }

        @Override
        public ExecutableScript executable(CompiledScript compiledScript, Map<String, Object> originalParams) {
            return new ExecutableScript() {

                Map<String, Object> vars = new HashMap<>();

                @Override
                public void setNextVar(String name, Object value) {
                    vars.put(name, value);
                }

                @Override
                public Object run() {
                    Map<String, Object> ctx = (Map<String, Object>) vars.get("ctx");
                    assertNotNull(ctx);

                    Map<String, Object> params = new HashMap<>(originalParams);

                    Map<String, Object> newCtx = (Map<String, Object>) params.remove("_ctx");
                    if (newCtx != null) {
                        assertFalse(newCtx.containsKey("_source"));
                        ctx.putAll(newCtx);
                    }

                    Map<String, Object> source = (Map<String, Object>) ctx.get("_source");
                    source.putAll(params);

                    return ctx;
                }
            };
        }

        @Override
        public SearchScript search(CompiledScript compiledScript, SearchLookup lookup, Map<String, Object> vars) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean isInlineScriptEnabled() {
            return true;
        }
    }

    public static class FieldIncrementScriptPlugin extends Plugin implements ScriptPlugin {
        @Override
        public ScriptEngineService getScriptEngineService(Settings settings) {
            return new FieldIncrementScriptEngine();
        }
    }

    public static class FieldIncrementScriptEngine implements ScriptEngineService {

        public static final String NAME = "field_inc";

        @Override
        public void close() throws IOException {
        }

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
            return scriptSource;
        }

        @Override
        public ExecutableScript executable(CompiledScript compiledScript, Map<String, Object> params) {
            final String field = (String) compiledScript.compiled();
            return new ExecutableScript() {

                Map<String, Object> vars = new HashMap<>();

                @Override
                public void setNextVar(String name, Object value) {
                    vars.put(name, value);
                }

                @Override
                public Object run() {
                    Map<String, Object> ctx = (Map<String, Object>) vars.get("ctx");
                    assertNotNull(ctx);
                    Map<String, Object> source = (Map<String, Object>) ctx.get("_source");
                    Number currentValue = (Number) source.get(field);
                    Number inc = params == null ? 1L : (Number) params.getOrDefault("inc", 1);
                    source.put(field, currentValue.longValue() + inc.longValue());
                    return ctx;
                }
            };
        }

        @Override
        public SearchScript search(CompiledScript compiledScript, SearchLookup lookup, Map<String, Object> vars) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean isInlineScriptEnabled() {
            return true;
        }
    }

    public static class ScriptedUpsertScriptPlugin extends Plugin implements ScriptPlugin {
        @Override
        public ScriptEngineService getScriptEngineService(Settings settings) {
            return new ScriptedUpsertScriptEngine();
        }
    }

    public static class ScriptedUpsertScriptEngine implements ScriptEngineService {

        public static final String NAME = "scripted_upsert";

        @Override
        public void close() throws IOException {
        }

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
            return new Object(); // unused
        }

        @Override
        public ExecutableScript executable(CompiledScript compiledScript, Map<String, Object> params) {
            return new ExecutableScript() {

                Map<String, Object> vars = new HashMap<>();

                @Override
                public void setNextVar(String name, Object value) {
                    vars.put(name, value);
                }

                @Override
                public Object run() {
                    Map<String, Object> ctx = (Map<String, Object>) vars.get("ctx");
                    assertNotNull(ctx);
                    Map<String, Object> source = (Map<String, Object>) ctx.get("_source");
                    Number payment = (Number) params.get("payment");
                    Number oldBalance = (Number) source.get("balance");
                    int deduction = "create".equals(ctx.get("op")) ? payment.intValue() / 2 : payment.intValue();
                    source.put("balance", oldBalance.intValue() - deduction);
                    return ctx;
                }
            };
        }

        @Override
        public SearchScript search(CompiledScript compiledScript, SearchLookup lookup, Map<String, Object> vars) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean isInlineScriptEnabled() {
            return true;
        }

    }

    public static class ExtractContextInSourceScriptPlugin extends Plugin implements ScriptPlugin {
        @Override
        public ScriptEngineService getScriptEngineService(Settings settings) {
            return new ExtractContextInSourceScriptEngine();
        }
    }

    public static class ExtractContextInSourceScriptEngine implements ScriptEngineService {

        public static final String NAME = "extract_ctx";

        @Override
        public void close() throws IOException {
        }

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
            return new Object(); // unused
        }

        @Override
        public ExecutableScript executable(CompiledScript compiledScript, Map<String, Object> params) {
            return new ExecutableScript() {

                Map<String, Object> vars = new HashMap<>();

                @Override
                public void setNextVar(String name, Object value) {
                    vars.put(name, value);
                }

                @Override
                public Object run() {
                    Map<String, Object> ctx = (Map<String, Object>) vars.get("ctx");
                    assertNotNull(ctx);

                    Map<String, Object> source = (Map<String, Object>) ctx.get("_source");
                    Map<String, Object> ctxWithoutSource = new HashMap<>(ctx);
                    ctxWithoutSource.remove("_source");
                    source.put("update_context", ctxWithoutSource);

                    return ctx;
                }
            };
        }

        @Override
        public SearchScript search(CompiledScript compiledScript, SearchLookup lookup, Map<String, Object> vars) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean isInlineScriptEnabled() {
            return true;
        }
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(
                PutFieldValuesScriptPlugin.class,
                FieldIncrementScriptPlugin.class,
                ScriptedUpsertScriptPlugin.class,
                ExtractContextInSourceScriptPlugin.class,
                InternalSettingsPlugin.class
        );
    }

    private void createTestIndex() throws Exception {
        logger.info("--> creating index test");

        assertAcked(prepareCreate("test").addAlias(new Alias("alias")));
    }

    public void testUpsert() throws Exception {
        createTestIndex();
        ensureGreen();

        UpdateResponse updateResponse = client().prepareUpdate(indexOrAlias(), "type1", "1")
                .setUpsert(XContentFactory.jsonBuilder().startObject().field("field", 1).endObject())
                .setScript(new Script("field", ScriptService.ScriptType.INLINE, "field_inc", null))
                .execute().actionGet();
        assertEquals(DocWriteResponse.Result.CREATED, updateResponse.getResult());
        assertThat(updateResponse.getIndex(), equalTo("test"));

        for (int i = 0; i < 5; i++) {
            GetResponse getResponse = client().prepareGet("test", "type1", "1").execute().actionGet();
            assertThat(getResponse.getSourceAsMap().get("field").toString(), equalTo("1"));
        }

        updateResponse = client().prepareUpdate(indexOrAlias(), "type1", "1")
                .setUpsert(XContentFactory.jsonBuilder().startObject().field("field", 1).endObject())
                .setScript(new Script("field", ScriptService.ScriptType.INLINE, "field_inc", null))
                .execute().actionGet();
        assertEquals(DocWriteResponse.Result.UPDATED, updateResponse.getResult());
        assertThat(updateResponse.getIndex(), equalTo("test"));

        for (int i = 0; i < 5; i++) {
            GetResponse getResponse = client().prepareGet("test", "type1", "1").execute().actionGet();
            assertThat(getResponse.getSourceAsMap().get("field").toString(), equalTo("2"));
        }
    }

    public void testScriptedUpsert() throws Exception {
        createTestIndex();
        ensureGreen();

        // Script logic is
        // 1) New accounts take balance from "balance" in upsert doc and first payment is charged at 50%
        // 2) Existing accounts subtract full payment from balance stored in elasticsearch

        int openingBalance=10;

        Map<String, Object> params = new HashMap<>();
        params.put("payment", 2);

        // Pay money from what will be a new account and opening balance comes from upsert doc
        // provided by client
        UpdateResponse updateResponse = client().prepareUpdate(indexOrAlias(), "type1", "1")
                .setUpsert(XContentFactory.jsonBuilder().startObject().field("balance", openingBalance).endObject())
                .setScriptedUpsert(true)
                .setScript(new Script("", ScriptService.ScriptType.INLINE, "scripted_upsert", params))
                .execute().actionGet();
        assertEquals(DocWriteResponse.Result.CREATED, updateResponse.getResult());
        assertThat(updateResponse.getIndex(), equalTo("test"));

        for (int i = 0; i < 5; i++) {
            GetResponse getResponse = client().prepareGet("test", "type1", "1").execute().actionGet();
            assertThat(getResponse.getSourceAsMap().get("balance").toString(), equalTo("9"));
        }

        // Now pay money for an existing account where balance is stored in es
        updateResponse = client().prepareUpdate(indexOrAlias(), "type1", "1")
                .setUpsert(XContentFactory.jsonBuilder().startObject().field("balance", openingBalance).endObject())
                .setScriptedUpsert(true)
                .setScript(new Script("", ScriptService.ScriptType.INLINE, "scripted_upsert", params))
                .execute().actionGet();
        assertEquals(DocWriteResponse.Result.UPDATED, updateResponse.getResult());
        assertThat(updateResponse.getIndex(), equalTo("test"));

        for (int i = 0; i < 5; i++) {
            GetResponse getResponse = client().prepareGet("test", "type1", "1").execute().actionGet();
            assertThat(getResponse.getSourceAsMap().get("balance").toString(), equalTo("7"));
        }
    }

    public void testUpsertDoc() throws Exception {
        createTestIndex();
        ensureGreen();

        UpdateResponse updateResponse = client().prepareUpdate(indexOrAlias(), "type1", "1")
                .setDoc(XContentFactory.jsonBuilder().startObject().field("bar", "baz").endObject())
                .setDocAsUpsert(true)
                .setFields("_source")
                .execute().actionGet();
        assertThat(updateResponse.getIndex(), equalTo("test"));
        assertThat(updateResponse.getGetResult(), notNullValue());
        assertThat(updateResponse.getGetResult().getIndex(), equalTo("test"));
        assertThat(updateResponse.getGetResult().sourceAsMap().get("bar").toString(), equalTo("baz"));
    }

    // Issue #3265
    public void testNotUpsertDoc() throws Exception {
        createTestIndex();
        ensureGreen();

        assertThrows(client().prepareUpdate(indexOrAlias(), "type1", "1")
                .setDoc(XContentFactory.jsonBuilder().startObject().field("bar", "baz").endObject())
                .setDocAsUpsert(false)
                .setFields("_source")
                .execute(), DocumentMissingException.class);
    }

    public void testUpsertFields() throws Exception {
        createTestIndex();
        ensureGreen();

        UpdateResponse updateResponse = client().prepareUpdate(indexOrAlias(), "type1", "1")
                .setUpsert(XContentFactory.jsonBuilder().startObject().field("bar", "baz").endObject())
                .setScript(new Script("", ScriptService.ScriptType.INLINE, "put_values", Collections.singletonMap("extra", "foo")))
                .setFields("_source")
                .execute().actionGet();

        assertThat(updateResponse.getIndex(), equalTo("test"));
        assertThat(updateResponse.getGetResult(), notNullValue());
        assertThat(updateResponse.getGetResult().getIndex(), equalTo("test"));
        assertThat(updateResponse.getGetResult().sourceAsMap().get("bar").toString(), equalTo("baz"));
        assertThat(updateResponse.getGetResult().sourceAsMap().get("extra"), nullValue());

        updateResponse = client().prepareUpdate(indexOrAlias(), "type1", "1")
                .setUpsert(XContentFactory.jsonBuilder().startObject().field("bar", "baz").endObject())
                .setScript(new Script("", ScriptService.ScriptType.INLINE, "put_values", Collections.singletonMap("extra", "foo")))
                .setFields("_source")
                .execute().actionGet();

        assertThat(updateResponse.getIndex(), equalTo("test"));
        assertThat(updateResponse.getGetResult(), notNullValue());
        assertThat(updateResponse.getGetResult().getIndex(), equalTo("test"));
        assertThat(updateResponse.getGetResult().sourceAsMap().get("bar").toString(), equalTo("baz"));
        assertThat(updateResponse.getGetResult().sourceAsMap().get("extra").toString(), equalTo("foo"));
    }

    public void testVersionedUpdate() throws Exception {
        assertAcked(prepareCreate("test").addAlias(new Alias("alias")));
        ensureGreen();

        index("test", "type", "1", "text", "value"); // version is now 1

        assertThrows(client().prepareUpdate(indexOrAlias(), "type", "1")
                        .setScript(new Script("", ScriptService.ScriptType.INLINE, "put_values", Collections.singletonMap("text", "v2"))).setVersion(2)
                        .execute(),
                VersionConflictEngineException.class);

        client().prepareUpdate(indexOrAlias(), "type", "1")
                .setScript(new Script("", ScriptService.ScriptType.INLINE, "put_values", Collections.singletonMap("text", "v2"))).setVersion(1).get();
        assertThat(client().prepareGet("test", "type", "1").get().getVersion(), equalTo(2L));

        // and again with a higher version..
        client().prepareUpdate(indexOrAlias(), "type", "1")
                .setScript(new Script("", ScriptService.ScriptType.INLINE, "put_values", Collections.singletonMap("text", "v3"))).setVersion(2).get();

        assertThat(client().prepareGet("test", "type", "1").get().getVersion(), equalTo(3L));

        // after delete
        client().prepareDelete("test", "type", "1").get();
        assertThrows(client().prepareUpdate("test", "type", "1")
                        .setScript(new Script("", ScriptService.ScriptType.INLINE, "put_values", Collections.singletonMap("text", "v2"))).setVersion(3)
                        .execute(),
                DocumentMissingException.class);

        // external versioning
        client().prepareIndex("test", "type", "2").setSource("text", "value").setVersion(10).setVersionType(VersionType.EXTERNAL).get();

        assertThrows(client().prepareUpdate(indexOrAlias(), "type", "2")
                        .setScript(new Script("", ScriptService.ScriptType.INLINE, "put_values", Collections.singletonMap("text", "v2"))).setVersion(2)
                        .setVersionType(VersionType.EXTERNAL).execute(),
                ActionRequestValidationException.class);


        // With force version
        client().prepareUpdate(indexOrAlias(), "type", "2")
                .setScript(new Script("", ScriptService.ScriptType.INLINE,  "put_values", Collections.singletonMap("text", "v10")))
                .setVersion(10).setVersionType(VersionType.FORCE).get();

        GetResponse get = get("test", "type", "2");
        assertThat(get.getVersion(), equalTo(10L));
        assertThat((String) get.getSource().get("text"), equalTo("v10"));

        // upserts - the combination with versions is a bit weird. Test are here to ensure we do not change our behavior unintentionally

        // With internal versions, tt means "if object is there with version X, update it or explode. If it is not there, index.
        client().prepareUpdate(indexOrAlias(), "type", "3")
                .setScript(new Script("", ScriptService.ScriptType.INLINE, "put_values", Collections.singletonMap("text", "v2")))
                .setVersion(10).setUpsert("{ \"text\": \"v0\" }").get();
        get = get("test", "type", "3");
        assertThat(get.getVersion(), equalTo(1L));
        assertThat((String) get.getSource().get("text"), equalTo("v0"));

        // retry on conflict is rejected:
        assertThrows(client().prepareUpdate(indexOrAlias(), "type", "1").setVersion(10).setRetryOnConflict(5), ActionRequestValidationException.class);
    }

    public void testIndexAutoCreation() throws Exception {
        UpdateResponse updateResponse = client().prepareUpdate("test", "type1", "1")
                .setUpsert(XContentFactory.jsonBuilder().startObject().field("bar", "baz").endObject())
                .setScript(new Script("", ScriptService.ScriptType.INLINE, "put_values", Collections.singletonMap("extra", "foo")))
                .setFields("_source")
                .execute().actionGet();

        assertThat(updateResponse.getIndex(), equalTo("test"));
        assertThat(updateResponse.getGetResult(), notNullValue());
        assertThat(updateResponse.getGetResult().getIndex(), equalTo("test"));
        assertThat(updateResponse.getGetResult().sourceAsMap().get("bar").toString(), equalTo("baz"));
        assertThat(updateResponse.getGetResult().sourceAsMap().get("extra"), nullValue());
    }

    public void testUpdate() throws Exception {
        createTestIndex();
        ensureGreen();

        try {
            client().prepareUpdate(indexOrAlias(), "type1", "1")
                    .setScript(new Script("field", ScriptService.ScriptType.INLINE, "field_inc", null)).execute().actionGet();
            fail();
        } catch (DocumentMissingException e) {
            // all is well
        }

        client().prepareIndex("test", "type1", "1").setSource("field", 1).execute().actionGet();

        UpdateResponse updateResponse = client().prepareUpdate(indexOrAlias(), "type1", "1")
                .setScript(new Script("field", ScriptService.ScriptType.INLINE, "field_inc", null)).execute().actionGet();
        assertThat(updateResponse.getVersion(), equalTo(2L));
        assertEquals(DocWriteResponse.Result.UPDATED, updateResponse.getResult());
        assertThat(updateResponse.getIndex(), equalTo("test"));

        for (int i = 0; i < 5; i++) {
            GetResponse getResponse = client().prepareGet("test", "type1", "1").execute().actionGet();
            assertThat(getResponse.getSourceAsMap().get("field").toString(), equalTo("2"));
        }

        Map<String, Object> params = new HashMap<>();
        params.put("inc", 3);
        updateResponse = client().prepareUpdate(indexOrAlias(), "type1", "1")
                .setScript(new Script("field", ScriptService.ScriptType.INLINE, "field_inc", params)).execute().actionGet();
        assertThat(updateResponse.getVersion(), equalTo(3L));
        assertEquals(DocWriteResponse.Result.UPDATED, updateResponse.getResult());
        assertThat(updateResponse.getIndex(), equalTo("test"));

        for (int i = 0; i < 5; i++) {
            GetResponse getResponse = client().prepareGet("test", "type1", "1").execute().actionGet();
            assertThat(getResponse.getSourceAsMap().get("field").toString(), equalTo("5"));
        }

        // check noop
        updateResponse = client().prepareUpdate(indexOrAlias(), "type1", "1")
                .setScript(new Script("", ScriptService.ScriptType.INLINE, "put_values", Collections.singletonMap("_ctx", Collections.singletonMap("op", "none")))).execute().actionGet();
        assertThat(updateResponse.getVersion(), equalTo(3L));
        assertEquals(DocWriteResponse.Result.NOOP, updateResponse.getResult());
        assertThat(updateResponse.getIndex(), equalTo("test"));

        for (int i = 0; i < 5; i++) {
            GetResponse getResponse = client().prepareGet("test", "type1", "1").execute().actionGet();
            assertThat(getResponse.getSourceAsMap().get("field").toString(), equalTo("5"));
        }

        // check delete
        updateResponse = client().prepareUpdate(indexOrAlias(), "type1", "1")
                .setScript(new Script("", ScriptService.ScriptType.INLINE, "put_values", Collections.singletonMap("_ctx", Collections.singletonMap("op", "delete")))).execute().actionGet();
        assertThat(updateResponse.getVersion(), equalTo(4L));
        assertEquals(DocWriteResponse.Result.DELETED, updateResponse.getResult());
        assertThat(updateResponse.getIndex(), equalTo("test"));

        for (int i = 0; i < 5; i++) {
            GetResponse getResponse = client().prepareGet("test", "type1", "1").execute().actionGet();
            assertThat(getResponse.isExists(), equalTo(false));
        }

        // check fields parameter
        client().prepareIndex("test", "type1", "1").setSource("field", 1).execute().actionGet();
        updateResponse = client().prepareUpdate(indexOrAlias(), "type1", "1")
                .setScript(new Script("field", ScriptService.ScriptType.INLINE, "field_inc", null)).setFields("_source", "field")
                .execute().actionGet();
        assertThat(updateResponse.getIndex(), equalTo("test"));
        assertThat(updateResponse.getGetResult(), notNullValue());
        assertThat(updateResponse.getGetResult().getIndex(), equalTo("test"));
        assertThat(updateResponse.getGetResult().sourceRef(), notNullValue());
        assertThat(updateResponse.getGetResult().field("field").getValue(), notNullValue());

        // check updates without script
        // add new field
        client().prepareIndex("test", "type1", "1").setSource("field", 1).execute().actionGet();
        updateResponse = client().prepareUpdate(indexOrAlias(), "type1", "1").setDoc(XContentFactory.jsonBuilder().startObject().field("field2", 2).endObject()).execute().actionGet();
        for (int i = 0; i < 5; i++) {
            GetResponse getResponse = client().prepareGet("test", "type1", "1").execute().actionGet();
            assertThat(getResponse.getSourceAsMap().get("field").toString(), equalTo("1"));
            assertThat(getResponse.getSourceAsMap().get("field2").toString(), equalTo("2"));
        }

        // change existing field
        updateResponse = client().prepareUpdate(indexOrAlias(), "type1", "1").setDoc(XContentFactory.jsonBuilder().startObject().field("field", 3).endObject()).execute().actionGet();
        for (int i = 0; i < 5; i++) {
            GetResponse getResponse = client().prepareGet("test", "type1", "1").execute().actionGet();
            assertThat(getResponse.getSourceAsMap().get("field").toString(), equalTo("3"));
            assertThat(getResponse.getSourceAsMap().get("field2").toString(), equalTo("2"));
        }

        // recursive map
        Map<String, Object> testMap = new HashMap<>();
        Map<String, Object> testMap2 = new HashMap<>();
        Map<String, Object> testMap3 = new HashMap<>();
        testMap3.put("commonkey", testMap);
        testMap3.put("map3", 5);
        testMap2.put("map2", 6);
        testMap.put("commonkey", testMap2);
        testMap.put("map1", 8);

        client().prepareIndex("test", "type1", "1").setSource("map", testMap).execute().actionGet();
        updateResponse = client().prepareUpdate(indexOrAlias(), "type1", "1").setDoc(XContentFactory.jsonBuilder().startObject().field("map", testMap3).endObject()).execute().actionGet();
        for (int i = 0; i < 5; i++) {
            GetResponse getResponse = client().prepareGet("test", "type1", "1").execute().actionGet();
            Map map1 = (Map) getResponse.getSourceAsMap().get("map");
            assertThat(map1.size(), equalTo(3));
            assertThat(map1.containsKey("map1"), equalTo(true));
            assertThat(map1.containsKey("map3"), equalTo(true));
            assertThat(map1.containsKey("commonkey"), equalTo(true));
            Map map2 = (Map) map1.get("commonkey");
            assertThat(map2.size(), equalTo(3));
            assertThat(map2.containsKey("map1"), equalTo(true));
            assertThat(map2.containsKey("map2"), equalTo(true));
            assertThat(map2.containsKey("commonkey"), equalTo(true));
        }
    }

    public void testUpdateRequestWithBothScriptAndDoc() throws Exception {
        createTestIndex();
        ensureGreen();

        try {
            client().prepareUpdate(indexOrAlias(), "type1", "1")
                    .setDoc(XContentFactory.jsonBuilder().startObject().field("field", 1).endObject())
                    .setScript(new Script("field", ScriptService.ScriptType.INLINE, "field_inc", null))
                    .execute().actionGet();
            fail("Should have thrown ActionRequestValidationException");
        } catch (ActionRequestValidationException e) {
            assertThat(e.validationErrors().size(), equalTo(1));
            assertThat(e.validationErrors().get(0), containsString("can't provide both script and doc"));
            assertThat(e.getMessage(), containsString("can't provide both script and doc"));
        }
    }

    public void testUpdateRequestWithScriptAndShouldUpsertDoc() throws Exception {
        createTestIndex();
        ensureGreen();
        try {
            client().prepareUpdate(indexOrAlias(), "type1", "1")
                    .setScript(new Script("field", ScriptService.ScriptType.INLINE, "field_inc", null))
                    .setDocAsUpsert(true)
                    .execute().actionGet();
            fail("Should have thrown ActionRequestValidationException");
        } catch (ActionRequestValidationException e) {
            assertThat(e.validationErrors().size(), equalTo(1));
            assertThat(e.validationErrors().get(0), containsString("doc must be specified if doc_as_upsert is enabled"));
            assertThat(e.getMessage(), containsString("doc must be specified if doc_as_upsert is enabled"));
        }
    }

    public void testContextVariables() throws Exception {
        assertAcked(prepareCreate("test").addAlias(new Alias("alias"))
                        .addMapping("type1", XContentFactory.jsonBuilder()
                                .startObject()
                                .startObject("type1")
                                .endObject()
                                .endObject())
                        .addMapping("subtype1", XContentFactory.jsonBuilder()
                                .startObject()
                                .startObject("subtype1")
                                .startObject("_parent").field("type", "type1").endObject()
                                .endObject()
                                .endObject())
        );
        ensureGreen();

        // Index some documents
        long timestamp = System.currentTimeMillis();
        client().prepareIndex()
                .setIndex("test")
                .setType("type1")
                .setId("parentId1")
                .setTimestamp(String.valueOf(timestamp-1))
                .setSource("field1", 0, "content", "bar")
                .execute().actionGet();

        long ttl = 10000;
        client().prepareIndex()
                .setIndex("test")
                .setType("subtype1")
                .setId("id1")
                .setParent("parentId1")
                .setRouting("routing1")
                .setTimestamp(String.valueOf(timestamp))
                .setTTL(ttl)
                .setSource("field1", 1, "content", "foo")
                .execute().actionGet();

        // Update the first object and note context variables values
        UpdateResponse updateResponse = client().prepareUpdate("test", "subtype1", "id1")
                .setRouting("routing1")
                .setScript(new Script("", ScriptService.ScriptType.INLINE, "extract_ctx", null))
                .execute().actionGet();

        assertEquals(2, updateResponse.getVersion());

        GetResponse getResponse = client().prepareGet("test", "subtype1", "id1").setRouting("routing1").execute().actionGet();
        Map<String, Object> updateContext = (Map<String, Object>) getResponse.getSourceAsMap().get("update_context");
        assertEquals("test", updateContext.get("_index"));
        assertEquals("subtype1", updateContext.get("_type"));
        assertEquals("id1", updateContext.get("_id"));
        assertEquals(1, updateContext.get("_version"));
        assertEquals("parentId1", updateContext.get("_parent"));
        assertEquals("routing1", updateContext.get("_routing"));

        // Idem with the second object
        updateResponse = client().prepareUpdate("test", "type1", "parentId1")
                .setScript(new Script("", ScriptService.ScriptType.INLINE, "extract_ctx", null))
                .execute().actionGet();

        assertEquals(2, updateResponse.getVersion());

        getResponse = client().prepareGet("test", "type1", "parentId1").execute().actionGet();
        updateContext = (Map<String, Object>) getResponse.getSourceAsMap().get("update_context");
        assertEquals("test", updateContext.get("_index"));
        assertEquals("type1", updateContext.get("_type"));
        assertEquals("parentId1", updateContext.get("_id"));
        assertEquals(1, updateContext.get("_version"));
        assertNull(updateContext.get("_parent"));
        assertNull(updateContext.get("_routing"));
        assertNull(updateContext.get("_ttl"));
    }

    public void testConcurrentUpdateWithRetryOnConflict() throws Exception {
        final boolean useBulkApi = randomBoolean();
        createTestIndex();
        ensureGreen();

        int numberOfThreads = scaledRandomIntBetween(2,5);
        final CountDownLatch latch = new CountDownLatch(numberOfThreads);
        final CountDownLatch startLatch = new CountDownLatch(1);
        final int numberOfUpdatesPerThread = scaledRandomIntBetween(100, 500);
        final List<Exception> failures = new CopyOnWriteArrayList<>();

        for (int i = 0; i < numberOfThreads; i++) {
            Runnable r = new Runnable() {
                @Override
                public void run() {
                    try {
                        startLatch.await();
                        for (int i = 0; i < numberOfUpdatesPerThread; i++) {
                            if (i % 100 == 0) {
                                logger.debug("Client [{}] issued [{}] of [{}] requests", Thread.currentThread().getName(), i, numberOfUpdatesPerThread);
                            }
                            if (useBulkApi) {
                                UpdateRequestBuilder updateRequestBuilder = client().prepareUpdate(indexOrAlias(), "type1", Integer.toString(i))
                                        .setScript(new Script("field", ScriptService.ScriptType.INLINE, "field_inc", null))
                                        .setRetryOnConflict(Integer.MAX_VALUE)
                                        .setUpsert(jsonBuilder().startObject().field("field", 1).endObject());
                                client().prepareBulk().add(updateRequestBuilder).execute().actionGet();
                            } else {
                                client().prepareUpdate(indexOrAlias(), "type1", Integer.toString(i))
                                        .setScript(new Script("field", ScriptService.ScriptType.INLINE, "field_inc", null))
                                        .setRetryOnConflict(Integer.MAX_VALUE)
                                        .setUpsert(jsonBuilder().startObject().field("field", 1).endObject())
                                        .execute().actionGet();
                            }
                        }
                        logger.info("Client [{}] issued all [{}] requests.", Thread.currentThread().getName(), numberOfUpdatesPerThread);
                    } catch (InterruptedException e) {
                        // test infrastructure kills long-running tests by interrupting them, thus we handle this case separately
                        logger.warn("Test was forcefully stopped. Client [{}] may still have outstanding requests.", Thread.currentThread().getName());
                        failures.add(e);
                        Thread.currentThread().interrupt();
                    } catch (Exception e) {
                        failures.add(e);
                    } finally {
                        latch.countDown();
                    }
                }

            };
            Thread updater = new Thread(r);
            updater.setName("UpdateIT-Client-" + i);
            updater.start();
        }
        startLatch.countDown();
        latch.await();
        for (Throwable throwable : failures) {
            logger.info("Captured failure on concurrent update:", throwable);
        }
        assertThat(failures.size(), equalTo(0));
        for (int i = 0; i < numberOfUpdatesPerThread; i++) {
            GetResponse response = client().prepareGet("test", "type1", Integer.toString(i)).execute().actionGet();
            assertThat(response.getId(), equalTo(Integer.toString(i)));
            assertThat(response.isExists(), equalTo(true));
            assertThat(response.getVersion(), equalTo((long) numberOfThreads));
            assertThat((Integer) response.getSource().get("field"), equalTo(numberOfThreads));
        }
    }

    public void testStressUpdateDeleteConcurrency() throws Exception {
        //We create an index with merging disabled so that deletes don't get merged away
        assertAcked(prepareCreate("test")
                .setSettings(Settings.builder().put(MergePolicyConfig.INDEX_MERGE_ENABLED, false)));
        ensureGreen();

        final int numberOfThreads = scaledRandomIntBetween(3,5);
        final int numberOfIdsPerThread = scaledRandomIntBetween(3,10);
        final int numberOfUpdatesPerId = scaledRandomIntBetween(10,100);
        final int retryOnConflict = randomIntBetween(0,1);
        final CountDownLatch latch = new CountDownLatch(numberOfThreads);
        final CountDownLatch startLatch = new CountDownLatch(1);
        final List<Throwable> failures = new CopyOnWriteArrayList<>();

        final class UpdateThread extends Thread {
            final Map<Integer,Integer> failedMap = new HashMap<>();
            final int numberOfIds;
            final int updatesPerId;
            final int maxUpdateRequests = numberOfIdsPerThread*numberOfUpdatesPerId;
            final int maxDeleteRequests = numberOfIdsPerThread*numberOfUpdatesPerId;
            private final Semaphore updateRequestsOutstanding = new Semaphore(maxUpdateRequests);
            private final Semaphore deleteRequestsOutstanding = new Semaphore(maxDeleteRequests);

            public UpdateThread(int numberOfIds, int updatesPerId) {
                this.numberOfIds = numberOfIds;
                this.updatesPerId = updatesPerId;
            }

            final class UpdateListener implements ActionListener<UpdateResponse> {
                int id;

                public UpdateListener(int id) {
                    this.id = id;
                }

                @Override
                public void onResponse(UpdateResponse updateResponse) {
                    updateRequestsOutstanding.release(1);
                }

                @Override
                public void onFailure(Exception e) {
                    synchronized (failedMap) {
                        incrementMapValue(id, failedMap);
                    }
                    updateRequestsOutstanding.release(1);
                }

            }

            final class DeleteListener implements ActionListener<DeleteResponse> {
                int id;

                public DeleteListener(int id) {
                    this.id = id;
                }

                @Override
                public void onResponse(DeleteResponse deleteResponse) {
                    deleteRequestsOutstanding.release(1);
                }

                @Override
                public void onFailure(Exception e) {
                    synchronized (failedMap) {
                        incrementMapValue(id, failedMap);
                    }
                    deleteRequestsOutstanding.release(1);
                }
            }

            @Override
            public void run(){
                try {
                    startLatch.await();
                    boolean hasWaitedForNoNode = false;
                    for (int j = 0; j < numberOfIds; j++) {
                        for (int k = 0; k < numberOfUpdatesPerId; ++k) {
                            updateRequestsOutstanding.acquire();
                            try {
                                UpdateRequest ur = client().prepareUpdate("test", "type1", Integer.toString(j))
                                        .setScript(new Script("field", ScriptService.ScriptType.INLINE, "field_inc", null))
                                        .setRetryOnConflict(retryOnConflict)
                                        .setUpsert(jsonBuilder().startObject().field("field", 1).endObject())
                                        .request();
                                client().update(ur, new UpdateListener(j));
                            } catch (NoNodeAvailableException nne) {
                                updateRequestsOutstanding.release();
                                synchronized (failedMap) {
                                    incrementMapValue(j, failedMap);
                                }
                                if (hasWaitedForNoNode) {
                                    throw nne;
                                }
                                logger.warn("Got NoNodeException waiting for 1 second for things to recover.");
                                hasWaitedForNoNode = true;
                                Thread.sleep(1000);
                            }

                            try {
                                deleteRequestsOutstanding.acquire();
                                DeleteRequest dr = client().prepareDelete("test", "type1", Integer.toString(j)).request();
                                client().delete(dr, new DeleteListener(j));
                            } catch (NoNodeAvailableException nne) {
                                deleteRequestsOutstanding.release();
                                synchronized (failedMap) {
                                    incrementMapValue(j, failedMap);
                                }
                                if (hasWaitedForNoNode) {
                                    throw nne;
                                }
                                logger.warn("Got NoNodeException waiting for 1 second for things to recover.");
                                hasWaitedForNoNode = true;
                                Thread.sleep(1000); //Wait for no-node to clear
                            }
                        }
                    }
                } catch (Exception e) {
                    logger.error("Something went wrong", e);
                    failures.add(e);
                } finally {
                    try {
                        waitForOutstandingRequests(TimeValue.timeValueSeconds(60), updateRequestsOutstanding, maxUpdateRequests, "Update");
                        waitForOutstandingRequests(TimeValue.timeValueSeconds(60), deleteRequestsOutstanding, maxDeleteRequests, "Delete");
                    } catch (ElasticsearchTimeoutException ete) {
                        failures.add(ete);
                    }
                    latch.countDown();
                }
            }

            private void incrementMapValue(int j, Map<Integer,Integer> map) {
                if (!map.containsKey(j)) {
                    map.put(j, 0);
                }
                map.put(j, map.get(j) + 1);
            }

            private void waitForOutstandingRequests(TimeValue timeOut, Semaphore requestsOutstanding, int maxRequests, String name) {
                long start = System.currentTimeMillis();
                do {
                    long msRemaining = timeOut.getMillis() - (System.currentTimeMillis() - start);
                    logger.info("[{}] going to try and acquire [{}] in [{}]ms [{}] available to acquire right now",name, maxRequests,msRemaining, requestsOutstanding.availablePermits());
                    try {
                        requestsOutstanding.tryAcquire(maxRequests, msRemaining, TimeUnit.MILLISECONDS );
                        return;
                    } catch (InterruptedException ie) {
                        //Just keep swimming
                    }
                } while ((System.currentTimeMillis() - start) < timeOut.getMillis());
                throw new ElasticsearchTimeoutException("Requests were still outstanding after the timeout [" + timeOut + "] for type [" + name + "]" );
            }
        }
        final List<UpdateThread> threads = new ArrayList<>();

        for (int i = 0; i < numberOfThreads; i++) {
            UpdateThread ut = new UpdateThread(numberOfIdsPerThread, numberOfUpdatesPerId);
            ut.start();
            threads.add(ut);
        }

        startLatch.countDown();
        latch.await();

        for (UpdateThread ut : threads){
            ut.join(); //Threads should have finished because of the latch.await
        }

        //If are no errors every request received a response otherwise the test would have timedout
        //aquiring the request outstanding semaphores.
        for (Throwable throwable : failures) {
            logger.info("Captured failure on concurrent update:", throwable);
        }

        assertThat(failures.size(), equalTo(0));

        //Upsert all the ids one last time to make sure they are available at get time
        //This means that we add 1 to the expected versions and attempts
        //All the previous operations should be complete or failed at this point
        for (int i = 0; i < numberOfIdsPerThread; ++i) {
            UpdateResponse ur = client().prepareUpdate("test", "type1", Integer.toString(i))
                    .setScript(new Script("field", ScriptService.ScriptType.INLINE, "field_inc", null))
                .setRetryOnConflict(Integer.MAX_VALUE)
                .setUpsert(jsonBuilder().startObject().field("field", 1).endObject())
                .execute().actionGet();
        }

        refresh();

        for (int i = 0; i < numberOfIdsPerThread; ++i) {
            int totalFailures = 0;
            GetResponse response = client().prepareGet("test", "type1", Integer.toString(i)).execute().actionGet();
            if (response.isExists()) {
                assertThat(response.getId(), equalTo(Integer.toString(i)));
                int expectedVersion = (numberOfThreads * numberOfUpdatesPerId * 2) + 1;
                for (UpdateThread ut : threads) {
                    if (ut.failedMap.containsKey(i)) {
                        totalFailures += ut.failedMap.get(i);
                    }
                }
                expectedVersion -= totalFailures;
                logger.error("Actual version [{}] Expected version [{}] Total failures [{}]", response.getVersion(), expectedVersion, totalFailures);
                assertThat(response.getVersion(), equalTo((long) expectedVersion));
                assertThat(response.getVersion() + totalFailures,
                        equalTo(
                                (long)((numberOfUpdatesPerId * numberOfThreads * 2) + 1)
                ));
            }
        }
    }

    private static String indexOrAlias() {
        return randomBoolean() ? "test" : "alias";
    }
}
