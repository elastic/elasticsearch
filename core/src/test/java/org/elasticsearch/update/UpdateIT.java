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
import org.elasticsearch.index.VersionType;
import org.elasticsearch.index.engine.DocumentMissingException;
import org.elasticsearch.index.engine.VersionConflictEngineException;
import org.elasticsearch.index.shard.MergePolicyConfig;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.test.ESIntegTestCase;
import org.junit.Test;

import java.util.ArrayList;
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
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class UpdateIT extends ESIntegTestCase {

    private void createTestIndex() throws Exception {
        logger.info("--> creating index test");

        assertAcked(prepareCreate("test").addAlias(new Alias("alias"))
                .addMapping("type1", XContentFactory.jsonBuilder()
                        .startObject()
                        .startObject("type1")
                        .startObject("_timestamp").field("enabled", true).endObject()
                        .startObject("_ttl").field("enabled", true).endObject()
                        .endObject()
                        .endObject()));
    }

    @Test
    public void testUpsert() throws Exception {
        createTestIndex();
        ensureGreen();

        UpdateResponse updateResponse = client().prepareUpdate(indexOrAlias(), "type1", "1")
                .setUpsert(XContentFactory.jsonBuilder().startObject().field("field", 1).endObject())
                .setScript(new Script("ctx._source.field += 1", ScriptService.ScriptType.INLINE, null, null))
                .execute().actionGet();
        assertTrue(updateResponse.isCreated());
        assertThat(updateResponse.getIndex(), equalTo("test"));

        for (int i = 0; i < 5; i++) {
            GetResponse getResponse = client().prepareGet("test", "type1", "1").execute().actionGet();
            assertThat(getResponse.getSourceAsMap().get("field").toString(), equalTo("1"));
        }

        updateResponse = client().prepareUpdate(indexOrAlias(), "type1", "1")
                .setUpsert(XContentFactory.jsonBuilder().startObject().field("field", 1).endObject())
                .setScript(new Script("ctx._source.field += 1", ScriptService.ScriptType.INLINE, null, null))
                .execute().actionGet();
        assertFalse(updateResponse.isCreated());
        assertThat(updateResponse.getIndex(), equalTo("test"));

        for (int i = 0; i < 5; i++) {
            GetResponse getResponse = client().prepareGet("test", "type1", "1").execute().actionGet();
            assertThat(getResponse.getSourceAsMap().get("field").toString(), equalTo("2"));
        }
    }

    @Test
    public void testScriptedUpsert() throws Exception {
        createTestIndex();
        ensureGreen();
        
        // Script logic is 
        // 1) New accounts take balance from "balance" in upsert doc and first payment is charged at 50%
        // 2) Existing accounts subtract full payment from balance stored in elasticsearch
        
        String script="int oldBalance=ctx._source.balance;"+
                      "int deduction=ctx.op == \"create\" ? (payment/2) :  payment;"+
                      "ctx._source.balance=oldBalance-deduction;";
        int openingBalance=10;

        Map<String, Object> params = new HashMap<>();
        params.put("payment", 2);

        // Pay money from what will be a new account and opening balance comes from upsert doc
        // provided by client
        UpdateResponse updateResponse = client().prepareUpdate(indexOrAlias(), "type1", "1")
                .setUpsert(XContentFactory.jsonBuilder().startObject().field("balance", openingBalance).endObject())
                .setScriptedUpsert(true)
.setScript(new Script(script, ScriptService.ScriptType.INLINE, null, params))
                .execute().actionGet();
        assertTrue(updateResponse.isCreated());
        assertThat(updateResponse.getIndex(), equalTo("test"));

        for (int i = 0; i < 5; i++) {
            GetResponse getResponse = client().prepareGet("test", "type1", "1").execute().actionGet();
            assertThat(getResponse.getSourceAsMap().get("balance").toString(), equalTo("9"));
        }

        // Now pay money for an existing account where balance is stored in es 
        updateResponse = client().prepareUpdate(indexOrAlias(), "type1", "1")
                .setUpsert(XContentFactory.jsonBuilder().startObject().field("balance", openingBalance).endObject())
                .setScriptedUpsert(true)
.setScript(new Script(script, ScriptService.ScriptType.INLINE, null, params))
                .execute().actionGet();
        assertFalse(updateResponse.isCreated());
        assertThat(updateResponse.getIndex(), equalTo("test"));

        for (int i = 0; i < 5; i++) {
            GetResponse getResponse = client().prepareGet("test", "type1", "1").execute().actionGet();
            assertThat(getResponse.getSourceAsMap().get("balance").toString(), equalTo("7"));
        }
    }

    @Test
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

    @Test
    // See: https://github.com/elasticsearch/elasticsearch/issues/3265
    public void testNotUpsertDoc() throws Exception {
        createTestIndex();
        ensureGreen();

        assertThrows(client().prepareUpdate(indexOrAlias(), "type1", "1")
                .setDoc(XContentFactory.jsonBuilder().startObject().field("bar", "baz").endObject())
                .setDocAsUpsert(false)
                .setFields("_source")
                .execute(), DocumentMissingException.class);
    }

    @Test
    public void testUpsertFields() throws Exception {
        createTestIndex();
        ensureGreen();

        UpdateResponse updateResponse = client().prepareUpdate(indexOrAlias(), "type1", "1")
                .setUpsert(XContentFactory.jsonBuilder().startObject().field("bar", "baz").endObject())
                .setScript(new Script("ctx._source.extra = \"foo\"", ScriptService.ScriptType.INLINE, null, null))
                .setFields("_source")
                .execute().actionGet();

        assertThat(updateResponse.getIndex(), equalTo("test"));
        assertThat(updateResponse.getGetResult(), notNullValue());
        assertThat(updateResponse.getGetResult().getIndex(), equalTo("test"));
        assertThat(updateResponse.getGetResult().sourceAsMap().get("bar").toString(), equalTo("baz"));
        assertThat(updateResponse.getGetResult().sourceAsMap().get("extra"), nullValue());

        updateResponse = client().prepareUpdate(indexOrAlias(), "type1", "1")
                .setUpsert(XContentFactory.jsonBuilder().startObject().field("bar", "baz").endObject())
                .setScript(new Script("ctx._source.extra = \"foo\"", ScriptService.ScriptType.INLINE, null, null))
                .setFields("_source")
                .execute().actionGet();

        assertThat(updateResponse.getIndex(), equalTo("test"));
        assertThat(updateResponse.getGetResult(), notNullValue());
        assertThat(updateResponse.getGetResult().getIndex(), equalTo("test"));
        assertThat(updateResponse.getGetResult().sourceAsMap().get("bar").toString(), equalTo("baz"));
        assertThat(updateResponse.getGetResult().sourceAsMap().get("extra").toString(), equalTo("foo"));
    }

    @Test
    public void testVersionedUpdate() throws Exception {
        assertAcked(prepareCreate("test").addAlias(new Alias("alias")));
        ensureGreen();

        index("test", "type", "1", "text", "value"); // version is now 1

        assertThrows(client().prepareUpdate(indexOrAlias(), "type", "1")
                        .setScript(new Script("ctx._source.text = 'v2'", ScriptService.ScriptType.INLINE, null, null)).setVersion(2)
                        .execute(),
                VersionConflictEngineException.class);

        client().prepareUpdate(indexOrAlias(), "type", "1")
                .setScript(new Script("ctx._source.text = 'v2'", ScriptService.ScriptType.INLINE, null, null)).setVersion(1).get();
        assertThat(client().prepareGet("test", "type", "1").get().getVersion(), equalTo(2l));

        // and again with a higher version..
        client().prepareUpdate(indexOrAlias(), "type", "1")
                .setScript(new Script("ctx._source.text = 'v3'", ScriptService.ScriptType.INLINE, null, null)).setVersion(2).get();

        assertThat(client().prepareGet("test", "type", "1").get().getVersion(), equalTo(3l));

        // after delete
        client().prepareDelete("test", "type", "1").get();
        assertThrows(client().prepareUpdate("test", "type", "1")
                        .setScript(new Script("ctx._source.text = 'v2'", ScriptService.ScriptType.INLINE, null, null)).setVersion(3)
                        .execute(),
                DocumentMissingException.class);

        // external versioning
        client().prepareIndex("test", "type", "2").setSource("text", "value").setVersion(10).setVersionType(VersionType.EXTERNAL).get();

        assertThrows(client().prepareUpdate(indexOrAlias(), "type", "2")
                        .setScript(new Script("ctx._source.text = 'v2'", ScriptService.ScriptType.INLINE, null, null)).setVersion(2)
                        .setVersionType(VersionType.EXTERNAL).execute(),
                ActionRequestValidationException.class);

        // upserts - the combination with versions is a bit weird. Test are here to ensure we do not change our behavior unintentionally

        // With internal versions, tt means "if object is there with version X, update it or explode. If it is not there, index.
        client().prepareUpdate(indexOrAlias(), "type", "3")
                .setScript(new Script("ctx._source.text = 'v2'", ScriptService.ScriptType.INLINE, null, null))
                .setVersion(10).setUpsert("{ \"text\": \"v0\" }").get();
        GetResponse get = get("test", "type", "3");
        assertThat(get.getVersion(), equalTo(1l));
        assertThat((String) get.getSource().get("text"), equalTo("v0"));

        // With force version
        client().prepareUpdate(indexOrAlias(), "type", "4")
                .setScript(new Script("ctx._source.text = 'v2'", ScriptService.ScriptType.INLINE, null, null))
                .setVersion(10).setVersionType(VersionType.FORCE).setUpsert("{ \"text\": \"v0\" }").get();

        get = get("test", "type", "4");
        assertThat(get.getVersion(), equalTo(10l));
        assertThat((String) get.getSource().get("text"), equalTo("v0"));


        // retry on conflict is rejected:
        assertThrows(client().prepareUpdate(indexOrAlias(), "type", "1").setVersion(10).setRetryOnConflict(5), ActionRequestValidationException.class);
    }

    @Test
    public void testIndexAutoCreation() throws Exception {
        UpdateResponse updateResponse = client().prepareUpdate("test", "type1", "1")
                .setUpsert(XContentFactory.jsonBuilder().startObject().field("bar", "baz").endObject())
                .setScript(new Script("ctx._source.extra = \"foo\"", ScriptService.ScriptType.INLINE, null, null))
                .setFields("_source")
                .execute().actionGet();

        assertThat(updateResponse.getIndex(), equalTo("test"));
        assertThat(updateResponse.getGetResult(), notNullValue());
        assertThat(updateResponse.getGetResult().getIndex(), equalTo("test"));
        assertThat(updateResponse.getGetResult().sourceAsMap().get("bar").toString(), equalTo("baz"));
        assertThat(updateResponse.getGetResult().sourceAsMap().get("extra"), nullValue());
    }

    @Test
    public void testUpdate() throws Exception {
        createTestIndex();
        ensureGreen();

        try {
            client().prepareUpdate(indexOrAlias(), "type1", "1")
                    .setScript(new Script("ctx._source.field++", ScriptService.ScriptType.INLINE, null, null)).execute().actionGet();
            fail();
        } catch (DocumentMissingException e) {
            // all is well
        }

        client().prepareIndex("test", "type1", "1").setSource("field", 1).execute().actionGet();

        UpdateResponse updateResponse = client().prepareUpdate(indexOrAlias(), "type1", "1")
                .setScript(new Script("ctx._source.field += 1", ScriptService.ScriptType.INLINE, null, null)).execute().actionGet();
        assertThat(updateResponse.getVersion(), equalTo(2L));
        assertFalse(updateResponse.isCreated());
        assertThat(updateResponse.getIndex(), equalTo("test"));

        for (int i = 0; i < 5; i++) {
            GetResponse getResponse = client().prepareGet("test", "type1", "1").execute().actionGet();
            assertThat(getResponse.getSourceAsMap().get("field").toString(), equalTo("2"));
        }

        Map<String, Object> params = new HashMap<>();
        params.put("count", 3);
        updateResponse = client().prepareUpdate(indexOrAlias(), "type1", "1")
                .setScript(new Script("ctx._source.field += count", ScriptService.ScriptType.INLINE, null, params)).execute().actionGet();
        assertThat(updateResponse.getVersion(), equalTo(3L));
        assertFalse(updateResponse.isCreated());
        assertThat(updateResponse.getIndex(), equalTo("test"));

        for (int i = 0; i < 5; i++) {
            GetResponse getResponse = client().prepareGet("test", "type1", "1").execute().actionGet();
            assertThat(getResponse.getSourceAsMap().get("field").toString(), equalTo("5"));
        }

        // check noop
        updateResponse = client().prepareUpdate(indexOrAlias(), "type1", "1")
                .setScript(new Script("ctx.op = 'none'", ScriptService.ScriptType.INLINE, null, null)).execute().actionGet();
        assertThat(updateResponse.getVersion(), equalTo(3L));
        assertFalse(updateResponse.isCreated());
        assertThat(updateResponse.getIndex(), equalTo("test"));

        for (int i = 0; i < 5; i++) {
            GetResponse getResponse = client().prepareGet("test", "type1", "1").execute().actionGet();
            assertThat(getResponse.getSourceAsMap().get("field").toString(), equalTo("5"));
        }

        // check delete
        updateResponse = client().prepareUpdate(indexOrAlias(), "type1", "1")
                .setScript(new Script("ctx.op = 'delete'", ScriptService.ScriptType.INLINE, null, null)).execute().actionGet();
        assertThat(updateResponse.getVersion(), equalTo(4L));
        assertFalse(updateResponse.isCreated());
        assertThat(updateResponse.getIndex(), equalTo("test"));

        for (int i = 0; i < 5; i++) {
            GetResponse getResponse = client().prepareGet("test", "type1", "1").execute().actionGet();
            assertThat(getResponse.isExists(), equalTo(false));
        }

        // check TTL is kept after an update without TTL
        client().prepareIndex("test", "type1", "2").setSource("field", 1).setTTL(86400000L).setRefresh(true).execute().actionGet();
        GetResponse getResponse = client().prepareGet("test", "type1", "2").setFields("_ttl").execute().actionGet();
        long ttl = ((Number) getResponse.getField("_ttl").getValue()).longValue();
        assertThat(ttl, greaterThan(0L));
        client().prepareUpdate(indexOrAlias(), "type1", "2")
                .setScript(new Script("ctx._source.field += 1", ScriptService.ScriptType.INLINE, null, null)).execute().actionGet();
        getResponse = client().prepareGet("test", "type1", "2").setFields("_ttl").execute().actionGet();
        ttl = ((Number) getResponse.getField("_ttl").getValue()).longValue();
        assertThat(ttl, greaterThan(0L));

        // check TTL update
        client().prepareUpdate(indexOrAlias(), "type1", "2")
                .setScript(new Script("ctx._ttl = 3600000", ScriptService.ScriptType.INLINE, null, null)).execute().actionGet();
        getResponse = client().prepareGet("test", "type1", "2").setFields("_ttl").execute().actionGet();
        ttl = ((Number) getResponse.getField("_ttl").getValue()).longValue();
        assertThat(ttl, greaterThan(0L));
        assertThat(ttl, lessThanOrEqualTo(3600000L));

        // check timestamp update
        client().prepareIndex("test", "type1", "3").setSource("field", 1).setRefresh(true).execute().actionGet();
        client().prepareUpdate(indexOrAlias(), "type1", "3")
                .setScript(new Script("ctx._timestamp = \"2009-11-15T14:12:12\"", ScriptService.ScriptType.INLINE, null, null)).execute()
                .actionGet();
        getResponse = client().prepareGet("test", "type1", "3").setFields("_timestamp").execute().actionGet();
        long timestamp = ((Number) getResponse.getField("_timestamp").getValue()).longValue();
        assertThat(timestamp, equalTo(1258294332000L));

        // check fields parameter
        client().prepareIndex("test", "type1", "1").setSource("field", 1).execute().actionGet();
        updateResponse = client().prepareUpdate(indexOrAlias(), "type1", "1")
                .setScript(new Script("ctx._source.field += 1", ScriptService.ScriptType.INLINE, null, null)).setFields("_source", "field")
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
            getResponse = client().prepareGet("test", "type1", "1").execute().actionGet();
            assertThat(getResponse.getSourceAsMap().get("field").toString(), equalTo("1"));
            assertThat(getResponse.getSourceAsMap().get("field2").toString(), equalTo("2"));
        }

        // change existing field
        updateResponse = client().prepareUpdate(indexOrAlias(), "type1", "1").setDoc(XContentFactory.jsonBuilder().startObject().field("field", 3).endObject()).execute().actionGet();
        for (int i = 0; i < 5; i++) {
            getResponse = client().prepareGet("test", "type1", "1").execute().actionGet();
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
            getResponse = client().prepareGet("test", "type1", "1").execute().actionGet();
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

    @Test
    public void testUpdateRequestWithBothScriptAndDoc() throws Exception {
        createTestIndex();
        ensureGreen();

        try {
            client().prepareUpdate(indexOrAlias(), "type1", "1")
                    .setDoc(XContentFactory.jsonBuilder().startObject().field("field", 1).endObject())
                    .setScript(new Script("ctx._source.field += 1", ScriptService.ScriptType.INLINE, null, null))
                    .execute().actionGet();
            fail("Should have thrown ActionRequestValidationException");
        } catch (ActionRequestValidationException e) {
            assertThat(e.validationErrors().size(), equalTo(1));
            assertThat(e.validationErrors().get(0), containsString("can't provide both script and doc"));
            assertThat(e.getMessage(), containsString("can't provide both script and doc"));
        }
    }

    @Test
    public void testUpdateRequestWithScriptAndShouldUpsertDoc() throws Exception {
        createTestIndex();
        ensureGreen();
        try {
            client().prepareUpdate(indexOrAlias(), "type1", "1")
                    .setScript(new Script("ctx._source.field += 1", ScriptService.ScriptType.INLINE, null, null))
                    .setDocAsUpsert(true)
                    .execute().actionGet();
            fail("Should have thrown ActionRequestValidationException");
        } catch (ActionRequestValidationException e) {
            assertThat(e.validationErrors().size(), equalTo(1));
            assertThat(e.validationErrors().get(0), containsString("doc must be specified if doc_as_upsert is enabled"));
            assertThat(e.getMessage(), containsString("doc must be specified if doc_as_upsert is enabled"));
        }
    }

    @Test
    public void testContextVariables() throws Exception {
        assertAcked(prepareCreate("test").addAlias(new Alias("alias"))
                        .addMapping("type1", XContentFactory.jsonBuilder()
                                .startObject()
                                .startObject("type1")
                                .startObject("_timestamp").field("enabled", true).endObject()
                                .startObject("_ttl").field("enabled", true).endObject()
                                .endObject()
                                .endObject())
                        .addMapping("subtype1", XContentFactory.jsonBuilder()
                                .startObject()
                                .startObject("subtype1")
                                .startObject("_parent").field("type", "type1").endObject()
                                .startObject("_timestamp").field("enabled", true).endObject()
                                .startObject("_ttl").field("enabled", true).endObject()
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
        Map<String, Object> scriptParams = new HashMap<>();
        scriptParams.put("delim", "_");
        UpdateResponse updateResponse = client().prepareUpdate("test", "subtype1", "id1")
                .setRouting("routing1")
                .setScript(
                        new Script("assert ctx._index == \"test\" : \"index should be \\\"test\\\"\"\n"
                                +
                                "assert ctx._type == \"subtype1\" : \"type should be \\\"subtype1\\\"\"\n" +
                                "assert ctx._id == \"id1\" : \"id should be \\\"id1\\\"\"\n" +
                                "assert ctx._version == 1 : \"version should be 1\"\n" +
                                "assert ctx._parent == \"parentId1\" : \"parent should be \\\"parentId1\\\"\"\n" +
                                "assert ctx._routing == \"routing1\" : \"routing should be \\\"routing1\\\"\"\n" +
                                "assert ctx._timestamp == " + timestamp + " : \"timestamp should be " + timestamp + "\"\n" +
                                // ttl has a 3-second leeway, because it's always counting down
                                "assert ctx._ttl <= " + ttl + " : \"ttl should be <= " + ttl + " but was \" + ctx._ttl\n" +
                                "assert ctx._ttl >= " + (ttl-3000) + " : \"ttl should be <= " + (ttl-3000) + " but was \" + ctx._ttl\n" +
                                "ctx._source.content = ctx._source.content + delim + ctx._source.content;\n" +
                                "ctx._source.field1 += 1;\n",
 ScriptService.ScriptType.INLINE, null, scriptParams))
                .execute().actionGet();

        assertEquals(2, updateResponse.getVersion());

        GetResponse getResponse = client().prepareGet("test", "subtype1", "id1").setRouting("routing1").execute().actionGet();
        assertEquals(2, getResponse.getSourceAsMap().get("field1"));
        assertEquals("foo_foo", getResponse.getSourceAsMap().get("content"));

        // Idem with the second object
        scriptParams = new HashMap<>();
        scriptParams.put("delim", "_");
        updateResponse = client().prepareUpdate("test", "type1", "parentId1")
                .setScript(
                        new Script(
                        "assert ctx._index == \"test\" : \"index should be \\\"test\\\"\"\n" +
                        "assert ctx._type == \"type1\" : \"type should be \\\"type1\\\"\"\n" +
                        "assert ctx._id == \"parentId1\" : \"id should be \\\"parentId1\\\"\"\n" +
                        "assert ctx._version == 1 : \"version should be 1\"\n" +
                        "assert ctx._parent == null : \"parent should be null\"\n" +
                        "assert ctx._routing == null : \"routing should be null\"\n" +
                        "assert ctx._timestamp == " + (timestamp - 1) + " : \"timestamp should be " + (timestamp - 1) + "\"\n" +
                        "assert ctx._ttl == null : \"ttl should be null\"\n" +
                        "ctx._source.content = ctx._source.content + delim + ctx._source.content;\n" +
                        "ctx._source.field1 += 1;\n",
 ScriptService.ScriptType.INLINE, null, scriptParams))
                .execute().actionGet();

        assertEquals(2, updateResponse.getVersion());

        getResponse = client().prepareGet("test", "type1", "parentId1").execute().actionGet();
        assertEquals(1, getResponse.getSourceAsMap().get("field1"));
        assertEquals("bar_bar", getResponse.getSourceAsMap().get("content"));
    }

    @Test
    public void testConcurrentUpdateWithRetryOnConflict() throws Exception {
        final boolean useBulkApi = randomBoolean();
        createTestIndex();
        ensureGreen();

        int numberOfThreads = scaledRandomIntBetween(2,5);
        final CountDownLatch latch = new CountDownLatch(numberOfThreads);
        final CountDownLatch startLatch = new CountDownLatch(1);
        final int numberOfUpdatesPerThread = scaledRandomIntBetween(100, 10000);
        final List<Throwable> failures = new CopyOnWriteArrayList<>();
        for (int i = 0; i < numberOfThreads; i++) {
            Runnable r = new Runnable() {

                @Override
                public void run() {
                    try {
                        startLatch.await();
                        for (int i = 0; i < numberOfUpdatesPerThread; i++) {
                            if (useBulkApi) {
                                UpdateRequestBuilder updateRequestBuilder = client().prepareUpdate(indexOrAlias(), "type1", Integer.toString(i))
                                        .setScript(new Script("ctx._source.field += 1", ScriptService.ScriptType.INLINE, null, null))
                                        .setRetryOnConflict(Integer.MAX_VALUE)
                                        .setUpsert(jsonBuilder().startObject().field("field", 1).endObject());
                                client().prepareBulk().add(updateRequestBuilder).execute().actionGet();
                            } else {
                                client().prepareUpdate(indexOrAlias(), "type1", Integer.toString(i))
                                        .setScript(new Script("ctx._source.field += 1", ScriptService.ScriptType.INLINE, null, null))
                                        .setRetryOnConflict(Integer.MAX_VALUE)
                                        .setUpsert(jsonBuilder().startObject().field("field", 1).endObject())
                                        .execute().actionGet();
                            }
                        }
                    } catch (Throwable e) {
                        failures.add(e);
                    } finally {
                        latch.countDown();
                    }
                }

            };
            new Thread(r).start();
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

    @Test
    public void stressUpdateDeleteConcurrency() throws Exception {
        //We create an index with merging disabled so that deletes don't get merged away
        assertAcked(prepareCreate("test")
                .addMapping("type1", XContentFactory.jsonBuilder()
                        .startObject()
                        .startObject("type1")
                        .startObject("_timestamp").field("enabled", true).endObject()
                        .startObject("_ttl").field("enabled", true).endObject()
                        .endObject()
                        .endObject())
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
                public void onFailure(Throwable e) {
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
                public void onFailure(Throwable e) {
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
                                        .setScript(new Script("ctx._source.field += 1", ScriptService.ScriptType.INLINE, null, null))
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
                } catch (Throwable e) {
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
                    .setScript(new Script("ctx._source.field += 1", ScriptService.ScriptType.INLINE, null, null))
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
