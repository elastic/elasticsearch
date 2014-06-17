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

import org.apache.lucene.util.LuceneTestCase.Slow;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateRequestBuilder;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.index.engine.DocumentMissingException;
import org.elasticsearch.index.engine.VersionConflictEngineException;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertThrows;
import static org.hamcrest.Matchers.*;

public class UpdateTests extends ElasticsearchIntegrationTest {


    protected void createIndex() throws Exception {
        logger.info("--> creating index test");

        client().admin().indices().prepareCreate("test")
                .addMapping("type1", XContentFactory.jsonBuilder()
                        .startObject()
                        .startObject("type1")
                        .startObject("_timestamp").field("enabled", true).field("store", "yes").endObject()
                        .startObject("_ttl").field("enabled", true).field("store", "yes").endObject()
                        .endObject()
                        .endObject())
                .execute().actionGet();
    }

    @Test
    public void testUpdateRequest() throws Exception {
        UpdateRequest request = new UpdateRequest("test", "type", "1");
        // simple script
        request.source(XContentFactory.jsonBuilder().startObject()
                .field("script", "script1")
                .endObject());
        assertThat(request.script(), equalTo("script1"));

        // script with params
        request = new UpdateRequest("test", "type", "1");
        request.source(XContentFactory.jsonBuilder().startObject()
                .field("script", "script1")
                .startObject("params").field("param1", "value1").endObject()
                .endObject());
        assertThat(request.script(), equalTo("script1"));
        assertThat(request.scriptParams().get("param1").toString(), equalTo("value1"));

        request = new UpdateRequest("test", "type", "1");
        request.source(XContentFactory.jsonBuilder().startObject()
                .startObject("params").field("param1", "value1").endObject()
                .field("script", "script1")
                .endObject());
        assertThat(request.script(), equalTo("script1"));
        assertThat(request.scriptParams().get("param1").toString(), equalTo("value1"));

        // script with params and upsert
        request = new UpdateRequest("test", "type", "1");
        request.source(XContentFactory.jsonBuilder().startObject()
                .startObject("params").field("param1", "value1").endObject()
                .field("script", "script1")
                .startObject("upsert").field("field1", "value1").startObject("compound").field("field2", "value2").endObject().endObject()
                .endObject());
        assertThat(request.script(), equalTo("script1"));
        assertThat(request.scriptParams().get("param1").toString(), equalTo("value1"));
        Map<String, Object> upsertDoc = XContentHelper.convertToMap(request.upsertRequest().source(), true).v2();
        assertThat(upsertDoc.get("field1").toString(), equalTo("value1"));
        assertThat(((Map) upsertDoc.get("compound")).get("field2").toString(), equalTo("value2"));

        request = new UpdateRequest("test", "type", "1");
        request.source(XContentFactory.jsonBuilder().startObject()
                .startObject("upsert").field("field1", "value1").startObject("compound").field("field2", "value2").endObject().endObject()
                .startObject("params").field("param1", "value1").endObject()
                .field("script", "script1")
                .endObject());
        assertThat(request.script(), equalTo("script1"));
        assertThat(request.scriptParams().get("param1").toString(), equalTo("value1"));
        upsertDoc = XContentHelper.convertToMap(request.upsertRequest().source(), true).v2();
        assertThat(upsertDoc.get("field1").toString(), equalTo("value1"));
        assertThat(((Map) upsertDoc.get("compound")).get("field2").toString(), equalTo("value2"));

        request = new UpdateRequest("test", "type", "1");
        request.source(XContentFactory.jsonBuilder().startObject()
                .startObject("params").field("param1", "value1").endObject()
                .startObject("upsert").field("field1", "value1").startObject("compound").field("field2", "value2").endObject().endObject()
                .field("script", "script1")
                .endObject());
        assertThat(request.script(), equalTo("script1"));
        assertThat(request.scriptParams().get("param1").toString(), equalTo("value1"));
        upsertDoc = XContentHelper.convertToMap(request.upsertRequest().source(), true).v2();
        assertThat(upsertDoc.get("field1").toString(), equalTo("value1"));
        assertThat(((Map) upsertDoc.get("compound")).get("field2").toString(), equalTo("value2"));

        // script with doc
        request = new UpdateRequest("test", "type", "1");
        request.source(XContentFactory.jsonBuilder().startObject()
                .startObject("doc").field("field1", "value1").startObject("compound").field("field2", "value2").endObject().endObject()
                .endObject());
        Map<String, Object> doc = request.doc().sourceAsMap();
        assertThat(doc.get("field1").toString(), equalTo("value1"));
        assertThat(((Map) doc.get("compound")).get("field2").toString(), equalTo("value2"));
    }

    @Test
    public void testUpsert() throws Exception {
        createIndex();
        ensureGreen();

        UpdateResponse updateResponse = client().prepareUpdate("test", "type1", "1")
                .setUpsert(XContentFactory.jsonBuilder().startObject().field("field", 1).endObject())
                .setScript("ctx._source.field += 1")
                .execute().actionGet();
        assertTrue(updateResponse.isCreated());

        for (int i = 0; i < 5; i++) {
            GetResponse getResponse = client().prepareGet("test", "type1", "1").execute().actionGet();
            assertThat(getResponse.getSourceAsMap().get("field").toString(), equalTo("1"));
        }

        updateResponse = client().prepareUpdate("test", "type1", "1")
                .setUpsert(XContentFactory.jsonBuilder().startObject().field("field", 1).endObject())
                .setScript("ctx._source.field += 1")
                .execute().actionGet();
        assertFalse(updateResponse.isCreated());


        for (int i = 0; i < 5; i++) {
            GetResponse getResponse = client().prepareGet("test", "type1", "1").execute().actionGet();
            assertThat(getResponse.getSourceAsMap().get("field").toString(), equalTo("2"));
        }
    }

    @Test
    public void testUpsertDoc() throws Exception {
        createIndex();
        ensureGreen();

        UpdateResponse updateResponse = client().prepareUpdate("test", "type1", "1")
                .setDoc(XContentFactory.jsonBuilder().startObject().field("bar", "baz").endObject())
                .setDocAsUpsert(true)
                .setFields("_source")
                .execute().actionGet();
        assertThat(updateResponse.getGetResult(), notNullValue());
        assertThat(updateResponse.getGetResult().sourceAsMap().get("bar").toString(), equalTo("baz"));
    }

    @Test
    // See: https://github.com/elasticsearch/elasticsearch/issues/3265
    public void testNotUpsertDoc() throws Exception {
        createIndex();
        ensureGreen();

        assertThrows(client().prepareUpdate("test", "type1", "1")
                .setDoc(XContentFactory.jsonBuilder().startObject().field("bar", "baz").endObject())
                .setDocAsUpsert(false)
                .setFields("_source")
                .execute(), DocumentMissingException.class);
    }

    @Test
    public void testUpsertFields() throws Exception {
        createIndex();
        ensureGreen();

        UpdateResponse updateResponse = client().prepareUpdate("test", "type1", "1")
                .setUpsert(XContentFactory.jsonBuilder().startObject().field("bar", "baz").endObject())
                .setScript("ctx._source.extra = \"foo\"")
                .setFields("_source")
                .execute().actionGet();

        assertThat(updateResponse.getGetResult(), notNullValue());
        assertThat(updateResponse.getGetResult().sourceAsMap().get("bar").toString(), equalTo("baz"));
        assertThat(updateResponse.getGetResult().sourceAsMap().get("extra"), nullValue());

        updateResponse = client().prepareUpdate("test", "type1", "1")
                .setUpsert(XContentFactory.jsonBuilder().startObject().field("bar", "baz").endObject())
                .setScript("ctx._source.extra = \"foo\"")
                .setFields("_source")
                .execute().actionGet();

        assertThat(updateResponse.getGetResult(), notNullValue());
        assertThat(updateResponse.getGetResult().sourceAsMap().get("bar").toString(), equalTo("baz"));
        assertThat(updateResponse.getGetResult().sourceAsMap().get("extra").toString(), equalTo("foo"));
    }

    @Test
    public void testVersionedUpdate() throws Exception {
        createIndex("test");
        ensureGreen();

        index("test", "type", "1", "text", "value"); // version is now 1

        assertThrows(client().prepareUpdate("test", "type", "1").setScript("ctx._source.text = 'v2'").setVersion(2).execute(),
                VersionConflictEngineException.class);

        client().prepareUpdate("test", "type", "1").setScript("ctx._source.text = 'v2'").setVersion(1).get();
        assertThat(client().prepareGet("test", "type", "1").get().getVersion(), equalTo(2l));

        // and again with a higher version..
        client().prepareUpdate("test", "type", "1").setScript("ctx._source.text = 'v3'").setVersion(2).get();

        assertThat(client().prepareGet("test", "type", "1").get().getVersion(), equalTo(3l));

        // after delete
        client().prepareDelete("test", "type", "1").get();
        assertThrows(client().prepareUpdate("test", "type", "1").setScript("ctx._source.text = 'v2'").setVersion(3).execute(),
                DocumentMissingException.class);

        // external versioning
        client().prepareIndex("test", "type", "2").setSource("text", "value").setVersion(10).setVersionType(VersionType.EXTERNAL).get();
        assertThrows(client().prepareUpdate("test", "type", "2").setScript("ctx._source.text = 'v2'").setVersion(2).setVersionType(VersionType.EXTERNAL).execute(),
                ActionRequestValidationException.class);


        // upserts - the combination with versions is a bit weird. Test are here to ensure we do not change our behavior unintentionally

        // With internal versions, tt means "if object is there with version X, update it or explode. If it is not there, index.
        client().prepareUpdate("test", "type", "3").setScript("ctx._source.text = 'v2'").setVersion(10).setUpsert("{ \"text\": \"v0\" }").get();
        GetResponse get = get("test", "type", "3");
        assertThat(get.getVersion(), equalTo(1l));
        assertThat((String) get.getSource().get("text"), equalTo("v0"));

        // With force version
        client().prepareUpdate("test", "type", "4").setScript("ctx._source.text = 'v2'").
                setVersion(10).setVersionType(VersionType.FORCE).setUpsert("{ \"text\": \"v0\" }").get();
        get = get("test", "type", "4");
        assertThat(get.getVersion(), equalTo(10l));
        assertThat((String) get.getSource().get("text"), equalTo("v0"));


        // retry on conflict is rejected:

        assertThrows(client().prepareUpdate("test", "type", "1").setVersion(10).setRetryOnConflict(5), ActionRequestValidationException.class);

    }

    @Test
    public void testIndexAutoCreation() throws Exception {
        UpdateResponse updateResponse = client().prepareUpdate("test", "type1", "1")
                .setUpsert(XContentFactory.jsonBuilder().startObject().field("bar", "baz").endObject())
                .setScript("ctx._source.extra = \"foo\"")
                .setFields("_source")
                .execute().actionGet();

        assertThat(updateResponse.getGetResult(), notNullValue());
        assertThat(updateResponse.getGetResult().sourceAsMap().get("bar").toString(), equalTo("baz"));
        assertThat(updateResponse.getGetResult().sourceAsMap().get("extra"), nullValue());
    }

    @Test
    public void testUpdate() throws Exception {
        createIndex();
        ensureGreen();

        try {
            client().prepareUpdate("test", "type1", "1").setScript("ctx._source.field++").execute().actionGet();
            fail();
        } catch (DocumentMissingException e) {
            // all is well
        }

        client().prepareIndex("test", "type1", "1").setSource("field", 1).execute().actionGet();

        UpdateResponse updateResponse = client().prepareUpdate("test", "type1", "1").setScript("ctx._source.field += 1").execute().actionGet();
        assertThat(updateResponse.getVersion(), equalTo(2L));
        assertFalse(updateResponse.isCreated());

        for (int i = 0; i < 5; i++) {
            GetResponse getResponse = client().prepareGet("test", "type1", "1").execute().actionGet();
            assertThat(getResponse.getSourceAsMap().get("field").toString(), equalTo("2"));
        }

        updateResponse = client().prepareUpdate("test", "type1", "1").setScript("ctx._source.field += count").addScriptParam("count", 3).execute().actionGet();
        assertThat(updateResponse.getVersion(), equalTo(3L));
        assertFalse(updateResponse.isCreated());

        for (int i = 0; i < 5; i++) {
            GetResponse getResponse = client().prepareGet("test", "type1", "1").execute().actionGet();
            assertThat(getResponse.getSourceAsMap().get("field").toString(), equalTo("5"));
        }

        // check noop
        updateResponse = client().prepareUpdate("test", "type1", "1").setScript("ctx.op = 'none'").execute().actionGet();
        assertThat(updateResponse.getVersion(), equalTo(3L));
        assertFalse(updateResponse.isCreated());

        for (int i = 0; i < 5; i++) {
            GetResponse getResponse = client().prepareGet("test", "type1", "1").execute().actionGet();
            assertThat(getResponse.getSourceAsMap().get("field").toString(), equalTo("5"));
        }

        // check delete
        updateResponse = client().prepareUpdate("test", "type1", "1").setScript("ctx.op = 'delete'").execute().actionGet();
        assertThat(updateResponse.getVersion(), equalTo(4L));
        assertFalse(updateResponse.isCreated());

        for (int i = 0; i < 5; i++) {
            GetResponse getResponse = client().prepareGet("test", "type1", "1").execute().actionGet();
            assertThat(getResponse.isExists(), equalTo(false));
        }

        // check TTL is kept after an update without TTL
        client().prepareIndex("test", "type1", "2").setSource("field", 1).setTTL(86400000L).setRefresh(true).execute().actionGet();
        GetResponse getResponse = client().prepareGet("test", "type1", "2").setFields("_ttl").execute().actionGet();
        long ttl = ((Number) getResponse.getField("_ttl").getValue()).longValue();
        assertThat(ttl, greaterThan(0L));
        client().prepareUpdate("test", "type1", "2").setScript("ctx._source.field += 1").execute().actionGet();
        getResponse = client().prepareGet("test", "type1", "2").setFields("_ttl").execute().actionGet();
        ttl = ((Number) getResponse.getField("_ttl").getValue()).longValue();
        assertThat(ttl, greaterThan(0L));

        // check TTL update
        client().prepareUpdate("test", "type1", "2").setScript("ctx._ttl = 3600000").execute().actionGet();
        getResponse = client().prepareGet("test", "type1", "2").setFields("_ttl").execute().actionGet();
        ttl = ((Number) getResponse.getField("_ttl").getValue()).longValue();
        assertThat(ttl, greaterThan(0L));
        assertThat(ttl, lessThanOrEqualTo(3600000L));

        // check timestamp update
        client().prepareIndex("test", "type1", "3").setSource("field", 1).setRefresh(true).execute().actionGet();
        client().prepareUpdate("test", "type1", "3").setScript("ctx._timestamp = \"2009-11-15T14:12:12\"").execute().actionGet();
        getResponse = client().prepareGet("test", "type1", "3").setFields("_timestamp").execute().actionGet();
        long timestamp = ((Number) getResponse.getField("_timestamp").getValue()).longValue();
        assertThat(timestamp, equalTo(1258294332000L));

        // check fields parameter
        client().prepareIndex("test", "type1", "1").setSource("field", 1).execute().actionGet();
        updateResponse = client().prepareUpdate("test", "type1", "1").setScript("ctx._source.field += 1").setFields("_source", "field").execute().actionGet();
        assertThat(updateResponse.getGetResult(), notNullValue());
        assertThat(updateResponse.getGetResult().sourceRef(), notNullValue());
        assertThat(updateResponse.getGetResult().field("field").getValue(), notNullValue());

        // check updates without script
        // add new field
        client().prepareIndex("test", "type1", "1").setSource("field", 1).execute().actionGet();
        updateResponse = client().prepareUpdate("test", "type1", "1").setDoc(XContentFactory.jsonBuilder().startObject().field("field2", 2).endObject()).execute().actionGet();
        for (int i = 0; i < 5; i++) {
            getResponse = client().prepareGet("test", "type1", "1").execute().actionGet();
            assertThat(getResponse.getSourceAsMap().get("field").toString(), equalTo("1"));
            assertThat(getResponse.getSourceAsMap().get("field2").toString(), equalTo("2"));
        }

        // change existing field
        updateResponse = client().prepareUpdate("test", "type1", "1").setDoc(XContentFactory.jsonBuilder().startObject().field("field", 3).endObject()).execute().actionGet();
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
        updateResponse = client().prepareUpdate("test", "type1", "1").setDoc(XContentFactory.jsonBuilder().startObject().field("map", testMap3).endObject()).execute().actionGet();
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
        createIndex();
        ensureGreen();

        try {
            client().prepareUpdate("test", "type1", "1")
                    .setDoc(XContentFactory.jsonBuilder().startObject().field("field", 1).endObject())
                    .setScript("ctx._source.field += 1")
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
        createIndex();
        ensureGreen();
        try {
            client().prepareUpdate("test", "type1", "1")
                    .setScript("ctx._source.field += 1")
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
    @Slow
    public void testConcurrentUpdateWithRetryOnConflict() throws Exception {
        final boolean useBulkApi = randomBoolean();
        createIndex();
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
                                UpdateRequestBuilder updateRequestBuilder = client().prepareUpdate("test", "type1", Integer.toString(i))
                                        .setScript("ctx._source.field += 1")
                                        .setRetryOnConflict(Integer.MAX_VALUE)
                                        .setUpsert(jsonBuilder().startObject().field("field", 1).endObject());
                                client().prepareBulk().add(updateRequestBuilder).execute().actionGet();
                            } else {
                                client().prepareUpdate("test", "type1", Integer.toString(i)).setScript("ctx._source.field += 1")
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

}
