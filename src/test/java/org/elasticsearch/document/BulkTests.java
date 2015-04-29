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

package org.elasticsearch.document;

import com.google.common.base.Charsets;

import org.elasticsearch.Version;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.count.CountResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateRequestBuilder;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.junit.Test;

import java.util.ArrayList;
import java.util.concurrent.CyclicBarrier;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertExists;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchHits;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class BulkTests extends ElasticsearchIntegrationTest {

    @Test
    public void testBulkUpdate_simple() throws Exception {
        assertAcked(prepareCreate("test").addAlias(new Alias("alias")));
        ensureGreen();

        BulkResponse bulkResponse = client().prepareBulk()
                .add(client().prepareIndex().setIndex(indexOrAlias()).setType("type1").setId("1").setSource("field", 1))
                .add(client().prepareIndex().setIndex(indexOrAlias()).setType("type1").setId("2").setSource("field", 2).setCreate(true))
                .add(client().prepareIndex().setIndex(indexOrAlias()).setType("type1").setId("3").setSource("field", 3))
                .add(client().prepareIndex().setIndex(indexOrAlias()).setType("type1").setId("4").setSource("field", 4))
                .add(client().prepareIndex().setIndex(indexOrAlias()).setType("type1").setId("5").setSource("field", 5))
                .execute().actionGet();

        assertThat(bulkResponse.hasFailures(), equalTo(false));
        assertThat(bulkResponse.getItems().length, equalTo(5));
        for (BulkItemResponse bulkItemResponse : bulkResponse) {
            assertThat(bulkItemResponse.getIndex(), equalTo("test"));
        }

        bulkResponse = client().prepareBulk()
                .add(client().prepareUpdate().setIndex(indexOrAlias()).setType("type1").setId("1")
                        .setScript("ctx._source.field += 1", ScriptService.ScriptType.INLINE))
                .add(client().prepareUpdate().setIndex(indexOrAlias()).setType("type1").setId("2")
                        .setScript("ctx._source.field += 1", ScriptService.ScriptType.INLINE).setRetryOnConflict(3))
                .add(client().prepareUpdate().setIndex(indexOrAlias()).setType("type1").setId("3").setDoc(jsonBuilder().startObject().field("field1", "test").endObject()))
                .execute().actionGet();

        assertThat(bulkResponse.hasFailures(), equalTo(false));
        assertThat(bulkResponse.getItems().length, equalTo(3));
        for (BulkItemResponse bulkItemResponse : bulkResponse) {
            assertThat(bulkItemResponse.getIndex(), equalTo("test"));
        }
        assertThat(((UpdateResponse) bulkResponse.getItems()[0].getResponse()).getId(), equalTo("1"));
        assertThat(((UpdateResponse) bulkResponse.getItems()[0].getResponse()).getVersion(), equalTo(2l));
        assertThat(((UpdateResponse) bulkResponse.getItems()[1].getResponse()).getId(), equalTo("2"));
        assertThat(((UpdateResponse) bulkResponse.getItems()[1].getResponse()).getVersion(), equalTo(2l));
        assertThat(((UpdateResponse) bulkResponse.getItems()[2].getResponse()).getId(), equalTo("3"));
        assertThat(((UpdateResponse) bulkResponse.getItems()[2].getResponse()).getVersion(), equalTo(2l));

        GetResponse getResponse = client().prepareGet().setIndex("test").setType("type1").setId("1").setFields("field").execute().actionGet();
        assertThat(getResponse.isExists(), equalTo(true));
        assertThat(getResponse.getVersion(), equalTo(2l));
        assertThat(((Long) getResponse.getField("field").getValue()), equalTo(2l));

        getResponse = client().prepareGet().setIndex("test").setType("type1").setId("2").setFields("field").execute().actionGet();
        assertThat(getResponse.isExists(), equalTo(true));
        assertThat(getResponse.getVersion(), equalTo(2l));
        assertThat(((Long) getResponse.getField("field").getValue()), equalTo(3l));

        getResponse = client().prepareGet().setIndex("test").setType("type1").setId("3").setFields("field1").execute().actionGet();
        assertThat(getResponse.isExists(), equalTo(true));
        assertThat(getResponse.getVersion(), equalTo(2l));
        assertThat(getResponse.getField("field1").getValue().toString(), equalTo("test"));

        bulkResponse = client().prepareBulk()
                .add(client().prepareUpdate().setIndex(indexOrAlias()).setType("type1").setId("6")
                        .setScript("ctx._source.field += 1", ScriptService.ScriptType.INLINE)
                        .setUpsert(jsonBuilder().startObject().field("field", 0).endObject()))
                .add(client().prepareUpdate().setIndex(indexOrAlias()).setType("type1").setId("7")
                        .setScript("ctx._source.field += 1", ScriptService.ScriptType.INLINE))
                .add(client().prepareUpdate().setIndex(indexOrAlias()).setType("type1").setId("2")
                        .setScript("ctx._source.field += 1", ScriptService.ScriptType.INLINE))
                .execute().actionGet();

        assertThat(bulkResponse.hasFailures(), equalTo(true));
        assertThat(bulkResponse.getItems().length, equalTo(3));
        assertThat(((UpdateResponse) bulkResponse.getItems()[0].getResponse()).getId(), equalTo("6"));
        assertThat(((UpdateResponse) bulkResponse.getItems()[0].getResponse()).getVersion(), equalTo(1l));
        assertThat(bulkResponse.getItems()[1].getResponse(), nullValue());
        assertThat(bulkResponse.getItems()[1].getFailure().getIndex(), equalTo("test"));
        assertThat(bulkResponse.getItems()[1].getFailure().getId(), equalTo("7"));
        assertThat(bulkResponse.getItems()[1].getFailure().getMessage(), containsString("document missing"));
        assertThat(((UpdateResponse) bulkResponse.getItems()[2].getResponse()).getId(), equalTo("2"));
        assertThat(((UpdateResponse) bulkResponse.getItems()[2].getResponse()).getIndex(), equalTo("test"));
        assertThat(((UpdateResponse) bulkResponse.getItems()[2].getResponse()).getVersion(), equalTo(3l));

        getResponse = client().prepareGet().setIndex("test").setType("type1").setId("6").setFields("field").execute().actionGet();
        assertThat(getResponse.isExists(), equalTo(true));
        assertThat(getResponse.getVersion(), equalTo(1l));
        assertThat(((Long) getResponse.getField("field").getValue()), equalTo(0l));

        getResponse = client().prepareGet().setIndex("test").setType("type1").setId("7").setFields("field").execute().actionGet();
        assertThat(getResponse.isExists(), equalTo(false));

        getResponse = client().prepareGet().setIndex("test").setType("type1").setId("2").setFields("field").execute().actionGet();
        assertThat(getResponse.isExists(), equalTo(true));
        assertThat(getResponse.getVersion(), equalTo(3l));
        assertThat(((Long) getResponse.getField("field").getValue()), equalTo(4l));
    }

    @Test
    public void testBulkVersioning() throws Exception {
        createIndex("test");
        ensureGreen();
        BulkResponse bulkResponse = client().prepareBulk()
                .add(client().prepareIndex("test", "type", "1").setCreate(true).setSource("field", "1"))
                .add(client().prepareIndex("test", "type", "2").setCreate(true).setSource("field", "1"))
                .add(client().prepareIndex("test", "type", "1").setSource("field", "2")).get();

        assertTrue(((IndexResponse) bulkResponse.getItems()[0].getResponse()).isCreated());
        assertThat(((IndexResponse) bulkResponse.getItems()[0].getResponse()).getVersion(), equalTo(1l));
        assertTrue(((IndexResponse) bulkResponse.getItems()[1].getResponse()).isCreated());
        assertThat(((IndexResponse) bulkResponse.getItems()[1].getResponse()).getVersion(), equalTo(1l));
        assertFalse(((IndexResponse) bulkResponse.getItems()[2].getResponse()).isCreated());
        assertThat(((IndexResponse) bulkResponse.getItems()[2].getResponse()).getVersion(), equalTo(2l));

        bulkResponse = client().prepareBulk()
                .add(client().prepareUpdate("test", "type", "1").setVersion(4l).setDoc("field", "2"))
                .add(client().prepareUpdate("test", "type", "2").setDoc("field", "2"))
                .add(client().prepareUpdate("test", "type", "1").setVersion(2l).setDoc("field", "3")).get();

        assertThat(bulkResponse.getItems()[0].getFailureMessage(), containsString("version conflict"));
        assertThat(((UpdateResponse) bulkResponse.getItems()[1].getResponse()).getVersion(), equalTo(2l));
        assertThat(((UpdateResponse) bulkResponse.getItems()[2].getResponse()).getVersion(), equalTo(3l));

        bulkResponse = client().prepareBulk()
                .add(client().prepareIndex("test", "type", "e1").setCreate(true).setSource("field", "1").setVersion(10).setVersionType(VersionType.EXTERNAL))
                .add(client().prepareIndex("test", "type", "e2").setCreate(true).setSource("field", "1").setVersion(10).setVersionType(VersionType.EXTERNAL))
                .add(client().prepareIndex("test", "type", "e1").setSource("field", "2").setVersion(12).setVersionType(VersionType.EXTERNAL)).get();

        assertTrue(((IndexResponse) bulkResponse.getItems()[0].getResponse()).isCreated());
        assertThat(((IndexResponse) bulkResponse.getItems()[0].getResponse()).getVersion(), equalTo(10l));
        assertTrue(((IndexResponse) bulkResponse.getItems()[1].getResponse()).isCreated());
        assertThat(((IndexResponse) bulkResponse.getItems()[1].getResponse()).getVersion(), equalTo(10l));
        assertFalse(((IndexResponse) bulkResponse.getItems()[2].getResponse()).isCreated());
        assertThat(((IndexResponse) bulkResponse.getItems()[2].getResponse()).getVersion(), equalTo(12l));

        bulkResponse = client().prepareBulk()
                .add(client().prepareUpdate("test", "type", "e1").setDoc("field", "2").setVersion(10)) // INTERNAL
                .add(client().prepareUpdate("test", "type", "e1").setDoc("field", "3").setVersion(20).setVersionType(VersionType.FORCE))
                .add(client().prepareUpdate("test", "type", "e1").setDoc("field", "3").setVersion(20).setVersionType(VersionType.INTERNAL)).get();

        assertThat(bulkResponse.getItems()[0].getFailureMessage(), containsString("version conflict"));
        assertThat(((UpdateResponse) bulkResponse.getItems()[1].getResponse()).getVersion(), equalTo(20l));
        assertThat(((UpdateResponse) bulkResponse.getItems()[2].getResponse()).getVersion(), equalTo(21l));
    }

    @Test
    public void testBulkUpdate_malformedScripts() throws Exception {

        createIndex("test");
        ensureGreen();

        BulkResponse bulkResponse = client().prepareBulk()
                .add(client().prepareIndex().setIndex("test").setType("type1").setId("1").setSource("field", 1))
                .add(client().prepareIndex().setIndex("test").setType("type1").setId("2").setSource("field", 1))
                .add(client().prepareIndex().setIndex("test").setType("type1").setId("3").setSource("field", 1))
                .execute().actionGet();

        assertThat(bulkResponse.hasFailures(), equalTo(false));
        assertThat(bulkResponse.getItems().length, equalTo(3));

        bulkResponse = client().prepareBulk()
                .add(client().prepareUpdate().setIndex("test").setType("type1").setId("1")
                        .setScript("ctx._source.field += a", ScriptService.ScriptType.INLINE).setFields("field"))
                .add(client().prepareUpdate().setIndex("test").setType("type1").setId("2")
                        .setScript("ctx._source.field += 1", ScriptService.ScriptType.INLINE).setFields("field"))
                .add(client().prepareUpdate().setIndex("test").setType("type1").setId("3")
                        .setScript("ctx._source.field += a", ScriptService.ScriptType.INLINE).setFields("field"))
                .execute().actionGet();

        assertThat(bulkResponse.hasFailures(), equalTo(true));
        assertThat(bulkResponse.getItems().length, equalTo(3));
        assertThat(bulkResponse.getItems()[0].getFailure().getId(), equalTo("1"));
        assertThat(bulkResponse.getItems()[0].getFailure().getMessage(), containsString("failed to execute script"));
        assertThat(bulkResponse.getItems()[0].getResponse(), nullValue());

        assertThat(((UpdateResponse) bulkResponse.getItems()[1].getResponse()).getId(), equalTo("2"));
        assertThat(((UpdateResponse) bulkResponse.getItems()[1].getResponse()).getVersion(), equalTo(2l));
        assertThat(((Integer) ((UpdateResponse) bulkResponse.getItems()[1].getResponse()).getGetResult().field("field").getValue()), equalTo(2));
        assertThat(bulkResponse.getItems()[1].getFailure(), nullValue());

        assertThat(bulkResponse.getItems()[2].getFailure().getId(), equalTo("3"));
        assertThat(bulkResponse.getItems()[2].getFailure().getMessage(), containsString("failed to execute script"));
        assertThat(bulkResponse.getItems()[2].getResponse(), nullValue());
    }

    @Test
    public void testBulkUpdate_largerVolume() throws Exception {
        createIndex("test");
        ensureGreen();

        int numDocs = scaledRandomIntBetween(100, 2000);
        if (numDocs % 2 == 1) {
            numDocs++; // this test needs an even num of docs
        }
        logger.info("Bulk-Indexing {} docs", numDocs);
        BulkRequestBuilder builder = client().prepareBulk();
        for (int i = 0; i < numDocs; i++) {
            builder.add(
                    client().prepareUpdate()
                            .setIndex("test").setType("type1").setId(Integer.toString(i))
                            .setScript("ctx._source.counter += 1", ScriptService.ScriptType.INLINE).setFields("counter")
                            .setUpsert(jsonBuilder().startObject().field("counter", 1).endObject())
            );
        }

        BulkResponse response = builder.execute().actionGet();
        assertThat(response.hasFailures(), equalTo(false));
        assertThat(response.getItems().length, equalTo(numDocs));
        for (int i = 0; i < numDocs; i++) {
            assertThat(response.getItems()[i].getId(), equalTo(Integer.toString(i)));
            assertThat(response.getItems()[i].getVersion(), equalTo(1l));
            assertThat(response.getItems()[i].getIndex(), equalTo("test"));
            assertThat(response.getItems()[i].getType(), equalTo("type1"));
            assertThat(response.getItems()[i].getOpType(), equalTo("update"));
            assertThat(((UpdateResponse) response.getItems()[i].getResponse()).getId(), equalTo(Integer.toString(i)));
            assertThat(((UpdateResponse) response.getItems()[i].getResponse()).getVersion(), equalTo(1l));
            assertThat(((Integer) ((UpdateResponse) response.getItems()[i].getResponse()).getGetResult().field("counter").getValue()), equalTo(1));

            for (int j = 0; j < 5; j++) {
                GetResponse getResponse = client().prepareGet("test", "type1", Integer.toString(i)).setFields("counter").execute().actionGet();
                assertThat(getResponse.isExists(), equalTo(true));
                assertThat(getResponse.getVersion(), equalTo(1l));
                assertThat((Long) getResponse.getField("counter").getValue(), equalTo(1l));
            }
        }

        builder = client().prepareBulk();
        for (int i = 0; i < numDocs; i++) {
            UpdateRequestBuilder updateBuilder = client().prepareUpdate()
                    .setIndex("test").setType("type1").setId(Integer.toString(i)).setFields("counter");
            if (i % 2 == 0) {
                updateBuilder.setScript("ctx._source.counter += 1", ScriptService.ScriptType.INLINE);
            } else {
                updateBuilder.setDoc(jsonBuilder().startObject().field("counter", 2).endObject());
            }
            if (i % 3 == 0) {
                updateBuilder.setRetryOnConflict(3);
            }

            builder.add(updateBuilder);
        }

        response = builder.execute().actionGet();
        assertThat(response.hasFailures(), equalTo(false));
        assertThat(response.getItems().length, equalTo(numDocs));
        for (int i = 0; i < numDocs; i++) {
            assertThat(response.getItems()[i].getId(), equalTo(Integer.toString(i)));
            assertThat(response.getItems()[i].getVersion(), equalTo(2l));
            assertThat(response.getItems()[i].getIndex(), equalTo("test"));
            assertThat(response.getItems()[i].getType(), equalTo("type1"));
            assertThat(response.getItems()[i].getOpType(), equalTo("update"));
            assertThat(((UpdateResponse) response.getItems()[i].getResponse()).getId(), equalTo(Integer.toString(i)));
            assertThat(((UpdateResponse) response.getItems()[i].getResponse()).getVersion(), equalTo(2l));
            assertThat(((Integer) ((UpdateResponse) response.getItems()[i].getResponse()).getGetResult().field("counter").getValue()), equalTo(2));
        }

        builder = client().prepareBulk();
        int maxDocs = numDocs / 2 + numDocs;
        for (int i = (numDocs / 2); i < maxDocs; i++) {
            builder.add(
                    client().prepareUpdate()
                            .setIndex("test").setType("type1").setId(Integer.toString(i)).setScript("ctx._source.counter += 1", ScriptService.ScriptType.INLINE)
            );
        }
        response = builder.execute().actionGet();
        assertThat(response.hasFailures(), equalTo(true));
        assertThat(response.getItems().length, equalTo(numDocs));
        for (int i = 0; i < numDocs; i++) {
            int id = i + (numDocs / 2);
            if (i >= (numDocs / 2)) {
                assertThat(response.getItems()[i].getFailure().getId(), equalTo(Integer.toString(id)));
                assertThat(response.getItems()[i].getFailure().getMessage(), containsString("document missing"));
            } else {
                assertThat(response.getItems()[i].getId(), equalTo(Integer.toString(id)));
                assertThat(response.getItems()[i].getVersion(), equalTo(3l));
                assertThat(response.getItems()[i].getIndex(), equalTo("test"));
                assertThat(response.getItems()[i].getType(), equalTo("type1"));
                assertThat(response.getItems()[i].getOpType(), equalTo("update"));
            }
        }

        builder = client().prepareBulk();
        for (int i = 0; i < numDocs; i++) {
            builder.add(
                    client().prepareUpdate()
                            .setIndex("test").setType("type1").setId(Integer.toString(i))
                            .setScript("ctx.op = \"none\"", ScriptService.ScriptType.INLINE)
            );
        }
        response = builder.execute().actionGet();
        assertThat(response.hasFailures(), equalTo(false));
        assertThat(response.getItems().length, equalTo(numDocs));
        for (int i = 0; i < numDocs; i++) {
            assertThat(response.getItems()[i].getItemId(), equalTo(i));
            assertThat(response.getItems()[i].getId(), equalTo(Integer.toString(i)));
            assertThat(response.getItems()[i].getIndex(), equalTo("test"));
            assertThat(response.getItems()[i].getType(), equalTo("type1"));
            assertThat(response.getItems()[i].getOpType(), equalTo("update"));
        }

        builder = client().prepareBulk();
        for (int i = 0; i < numDocs; i++) {
            builder.add(
                    client().prepareUpdate()
                            .setIndex("test").setType("type1").setId(Integer.toString(i))
                            .setScript("ctx.op = \"delete\"", ScriptService.ScriptType.INLINE)
            );
        }
        response = builder.execute().actionGet();
        assertThat(response.hasFailures(), equalTo(false));
        assertThat(response.getItems().length, equalTo(numDocs));
        for (int i = 0; i < numDocs; i++) {
            assertThat(response.getItems()[i].getItemId(), equalTo(i));
            assertThat(response.getItems()[i].getId(), equalTo(Integer.toString(i)));
            assertThat(response.getItems()[i].getIndex(), equalTo("test"));
            assertThat(response.getItems()[i].getType(), equalTo("type1"));
            assertThat(response.getItems()[i].getOpType(), equalTo("update"));
            for (int j = 0; j < 5; j++) {
                GetResponse getResponse = client().prepareGet("test", "type1", Integer.toString(i)).setFields("counter").execute().actionGet();
                assertThat(getResponse.isExists(), equalTo(false));
            }
        }
    }

    @Test
    public void testBulkIndexingWhileInitializing() throws Exception {

        int replica = randomInt(2);

        internalCluster().ensureAtLeastNumDataNodes(1 + replica);

        assertAcked(prepareCreate("test").setSettings(
                ImmutableSettings.builder()
                        .put(indexSettings())
                        .put("index.number_of_replicas", replica)
        ));

        int numDocs = scaledRandomIntBetween(100, 5000);
        int bulk = scaledRandomIntBetween(1, 99);
        for (int i = 0; i < numDocs; ) {
            final BulkRequestBuilder builder = client().prepareBulk();
            for (int j = 0; j < bulk && i < numDocs; j++, i++) {
                builder.add(client().prepareIndex("test", "type1", Integer.toString(i)).setSource("val", i));
            }
            logger.info("bulk indexing {}-{}", i - bulk, i - 1);
            BulkResponse response = builder.get();
            if (response.hasFailures()) {
                fail(response.buildFailureMessage());
            }
        }

        refresh();

        CountResponse countResponse = client().prepareCount().get();
        assertHitCount(countResponse, numDocs);
    }

    /*
    Test for https://github.com/elasticsearch/elasticsearch/issues/3444
     */
    @Test
    public void testBulkUpdateDocAsUpsertWithParent() throws Exception {
        client().admin().indices().prepareCreate("test")
                .addMapping("parent", "{\"parent\":{}}")
                .addMapping("child", "{\"child\": {\"_parent\": {\"type\": \"parent\"}}}")
                .execute().actionGet();
        ensureGreen();

        BulkRequestBuilder builder = client().prepareBulk();

        byte[] addParent = new BytesArray("{\"index\" : { \"_index\" : \"test\", \"_type\" : \"parent\", \"_id\" : \"parent1\"}}\n" +
                "{\"field1\" : \"value1\"}\n").array();

        byte[] addChild = new BytesArray("{ \"update\" : { \"_index\" : \"test\", \"_type\" : \"child\", \"_id\" : \"child1\", \"parent\" : \"parent1\"}}\n" +
                "{\"doc\" : { \"field1\" : \"value1\"}, \"doc_as_upsert\" : \"true\"}\n").array();

        builder.add(addParent, 0, addParent.length);
        builder.add(addChild, 0, addChild.length);

        BulkResponse bulkResponse = builder.get();
        assertThat(bulkResponse.getItems().length, equalTo(2));
        assertThat(bulkResponse.getItems()[0].isFailed(), equalTo(false));
        assertThat(bulkResponse.getItems()[1].isFailed(), equalTo(false));

        client().admin().indices().prepareRefresh("test").get();

        //we check that the _parent field was set on the child document by using the has parent query
        SearchResponse searchResponse = client().prepareSearch("test")
                .setQuery(QueryBuilders.hasParentQuery("parent", QueryBuilders.matchAllQuery()))
                .get();

        assertNoFailures(searchResponse);
        assertSearchHits(searchResponse, "child1");
    }

    /*
    Test for https://github.com/elasticsearch/elasticsearch/issues/3444
     */
    @Test
    public void testBulkUpdateUpsertWithParent() throws Exception {
        assertAcked(prepareCreate("test")
                .addMapping("parent", "{\"parent\":{}}")
                .addMapping("child", "{\"child\": {\"_parent\": {\"type\": \"parent\"}}}"));
        ensureGreen();

        BulkRequestBuilder builder = client().prepareBulk();

        byte[] addParent = new BytesArray("{\"index\" : { \"_index\" : \"test\", \"_type\" : \"parent\", \"_id\" : \"parent1\"}}\n" +
                "{\"field1\" : \"value1\"}\n").array();

        byte[] addChild = new BytesArray("{\"update\" : { \"_id\" : \"child1\", \"_type\" : \"child\", \"_index\" : \"test\", \"parent\" : \"parent1\"} }\n" +
                "{ \"script\" : \"ctx._source.field2 = 'value2'\", \"upsert\" : {\"field1\" : \"value1\"}}\n").array();

        builder.add(addParent, 0, addParent.length);
        builder.add(addChild, 0, addChild.length);

        BulkResponse bulkResponse = builder.get();
        assertThat(bulkResponse.getItems().length, equalTo(2));
        assertThat(bulkResponse.getItems()[0].isFailed(), equalTo(false));
        assertThat(bulkResponse.getItems()[1].isFailed(), equalTo(false));

        client().admin().indices().prepareRefresh("test").get();

        SearchResponse searchResponse = client().prepareSearch("test")
                .setQuery(QueryBuilders.hasParentQuery("parent", QueryBuilders.matchAllQuery()))
                .get();

        assertSearchHits(searchResponse, "child1");
    }

    /*
     * Test for https://github.com/elasticsearch/elasticsearch/issues/8365
     */
    @Test
    public void testBulkUpdateChildMissingParentRouting() throws Exception {
        assertAcked(prepareCreate("test").addMapping("parent", "{\"parent\":{}}").addMapping("child",
                "{\"child\": {\"_parent\": {\"type\": \"parent\"}}}"));
        ensureGreen();

        BulkRequestBuilder builder = client().prepareBulk();

        byte[] addParent = new BytesArray("{\"index\" : { \"_index\" : \"test\", \"_type\" : \"parent\", \"_id\" : \"parent1\"}}\n"
                + "{\"field1\" : \"value1\"}\n").array();

        byte[] addChildOK = new BytesArray(
                "{\"index\" : { \"_id\" : \"child1\", \"_type\" : \"child\", \"_index\" : \"test\", \"parent\" : \"parent1\"} }\n"
                        + "{ \"field1\" : \"value1\"}\n").array();
        byte[] addChildMissingRouting = new BytesArray(
                "{\"index\" : { \"_id\" : \"child2\", \"_type\" : \"child\", \"_index\" : \"test\"} }\n" + "{ \"field1\" : \"value1\"}\n")
                .array();

        builder.add(addParent, 0, addParent.length);
        builder.add(addChildOK, 0, addChildOK.length);
        builder.add(addChildMissingRouting, 0, addChildMissingRouting.length);
        builder.add(addChildOK, 0, addChildOK.length);

        BulkResponse bulkResponse = builder.get();
        assertThat(bulkResponse.getItems().length, equalTo(4));
        assertThat(bulkResponse.getItems()[0].isFailed(), equalTo(false));
        assertThat(bulkResponse.getItems()[1].isFailed(), equalTo(false));
        assertThat(bulkResponse.getItems()[2].isFailed(), equalTo(true));
        assertThat(bulkResponse.getItems()[3].isFailed(), equalTo(false));
    }

    @Test
    public void testFailingVersionedUpdatedOnBulk() throws Exception {
        createIndex("test");
        index("test", "type", "1", "field", "1");
        final BulkResponse[] responses = new BulkResponse[30];
        final CyclicBarrier cyclicBarrier = new CyclicBarrier(responses.length);
        Thread[] threads = new Thread[responses.length];


        for (int i = 0; i < responses.length; i++) {
            final int threadID = i;
            threads[threadID] = new Thread(new Runnable() {
                @Override
                public void run() {
                    try {
                        cyclicBarrier.await();
                    } catch (Exception e) {
                        return;
                    }
                    BulkRequestBuilder requestBuilder = client().prepareBulk();
                    requestBuilder.add(client().prepareUpdate("test", "type", "1").setVersion(1).setDoc("field", threadID));
                    responses[threadID] = requestBuilder.get();

                }
            });
            threads[threadID].start();

        }

        for (int i = 0; i < threads.length; i++) {
            threads[i].join();
        }

        int successes = 0;
        for (BulkResponse response : responses) {
            if (!response.hasFailures()) {
                successes++;
            }
        }

        assertThat(successes, equalTo(1));
    }

    @Test // issue 4745
    public void preParsingSourceDueToMappingShouldNotBreakCompleteBulkRequest() throws Exception {
        XContentBuilder builder = jsonBuilder().startObject()
                    .startObject("type")
                        .startObject("_timestamp")
                            .field("enabled", true)
                            .field("path", "last_modified")
                        .endObject()
                    .endObject()
                .endObject();
        assertAcked(prepareCreate("test").addMapping("type", builder));

        String brokenBuildRequestData = "{\"index\": {\"_id\": \"1\"}}\n" +
                "{\"name\": \"Malformed}\n" +
                "{\"index\": {\"_id\": \"2\"}}\n" +
                "{\"name\": \"Good\", \"last_modified\" : \"2013-04-05\"}\n";

        BulkResponse bulkResponse = client().prepareBulk().add(brokenBuildRequestData.getBytes(Charsets.UTF_8), 0, brokenBuildRequestData.length(), "test", "type").setRefresh(true).get();
        assertThat(bulkResponse.getItems().length, is(2));
        assertThat(bulkResponse.getItems()[0].isFailed(), is(true));
        assertThat(bulkResponse.getItems()[1].isFailed(), is(false));

        assertExists(get("test", "type", "2"));
    }

    @Test // issue 4745
    public void preParsingSourceDueToRoutingShouldNotBreakCompleteBulkRequest() throws Exception {
        XContentBuilder builder = jsonBuilder().startObject()
                    .startObject("type")
                        .startObject("_routing")
                            .field("required", true)
                            .field("path", "my_routing")
                        .endObject()
                    .endObject()
                .endObject();
        assertAcked(prepareCreate("test").addMapping("type", builder)
            .setSettings(IndexMetaData.SETTING_VERSION_CREATED, Version.V_1_4_2_ID));
        ensureYellow("test");

        String brokenBuildRequestData = "{\"index\": {} }\n" +
                "{\"name\": \"Malformed}\n" +
                "{\"index\": { \"_id\" : \"24000\" } }\n" +
                "{\"name\": \"Good\", \"my_routing\" : \"48000\"}\n";

        BulkResponse bulkResponse = client().prepareBulk().add(brokenBuildRequestData.getBytes(Charsets.UTF_8), 0, brokenBuildRequestData.length(), "test", "type").setRefresh(true).get();
        assertThat(bulkResponse.getItems().length, is(2));
        assertThat(bulkResponse.getItems()[0].isFailed(), is(true));
        assertThat(bulkResponse.getItems()[1].isFailed(), is(false));

        assertExists(client().prepareGet("test", "type", "24000").setRouting("48000").get());
    }


    @Test // issue 4745
    public void preParsingSourceDueToIdShouldNotBreakCompleteBulkRequest() throws Exception {
        XContentBuilder builder = jsonBuilder().startObject()
                    .startObject("type")
                        .startObject("_id")
                            .field("path", "my_id")
                        .endObject()
                    .endObject()
                .endObject();
        assertAcked(prepareCreate("test").addMapping("type", builder)
            .setSettings(IndexMetaData.SETTING_VERSION_CREATED, Version.V_1_4_2_ID));
        ensureYellow("test");

        String brokenBuildRequestData = "{\"index\": {} }\n" +
                "{\"name\": \"Malformed}\n" +
                "{\"index\": {} }\n" +
                "{\"name\": \"Good\", \"my_id\" : \"48\"}\n";

        BulkResponse bulkResponse = client().prepareBulk().add(brokenBuildRequestData.getBytes(Charsets.UTF_8), 0, brokenBuildRequestData.length(), "test", "type").setRefresh(true).get();
        assertThat(bulkResponse.getItems().length, is(2));
        assertThat(bulkResponse.getItems()[0].isFailed(), is(true));
        assertThat(bulkResponse.getItems()[1].isFailed(), is(false));

        assertExists(get("test", "type", "48"));
    }

    @Test // issue 4987
    public void testThatInvalidIndexNamesShouldNotBreakCompleteBulkRequest() {
        int bulkEntryCount = randomIntBetween(10, 50);
        BulkRequestBuilder builder = client().prepareBulk();
        boolean[] expectedFailures = new boolean[bulkEntryCount];
        ArrayList<String> badIndexNames = new ArrayList<>();
        for (int i = randomIntBetween(1, 5); i > 0; i--) {
            badIndexNames.add("INVALID.NAME" + i);
        }
        boolean expectFailure = false;
        for (int i = 0; i < bulkEntryCount; i++) {
            expectFailure |= expectedFailures[i] = randomBoolean();
            String name;
            if (expectedFailures[i]) {
                name = randomFrom(badIndexNames);
            } else {
                name = "test";
            }
            builder.add(client().prepareIndex().setIndex(name).setType("type1").setId("1").setSource("field", 1));
        }
        BulkResponse bulkResponse = builder.get();
        assertThat(bulkResponse.hasFailures(), is(expectFailure));
        assertThat(bulkResponse.getItems().length, is(bulkEntryCount));
        for (int i = 0; i < bulkEntryCount; i++) {
            assertThat(bulkResponse.getItems()[i].isFailed(), is(expectedFailures[i]));
        }
    }

    @Test // issue 6630
    public void testThatFailedUpdateRequestReturnsCorrectType() throws Exception {
        BulkResponse indexBulkItemResponse = client().prepareBulk()
                .add(new IndexRequest("test", "type", "3").source("{ \"title\" : \"Great Title of doc 3\" }"))
                .add(new IndexRequest("test", "type", "4").source("{ \"title\" : \"Great Title of doc 4\" }"))
                .add(new IndexRequest("test", "type", "5").source("{ \"title\" : \"Great Title of doc 5\" }"))
                .add(new IndexRequest("test", "type", "6").source("{ \"title\" : \"Great Title of doc 6\" }"))
                .setRefresh(true)
                .get();
        assertNoFailures(indexBulkItemResponse);

        BulkResponse bulkItemResponse = client().prepareBulk()
                .add(new IndexRequest("test", "type", "1").source("{ \"title\" : \"Great Title of doc 1\" }"))
                .add(new IndexRequest("test", "type", "2").source("{ \"title\" : \"Great Title of doc 2\" }"))
                .add(new UpdateRequest("test", "type", "3").doc("{ \"date\" : \"2014-01-30T23:59:57\"}"))
                .add(new UpdateRequest("test", "type", "4").doc("{ \"date\" : \"2014-13-30T23:59:57\"}"))
                .add(new DeleteRequest("test", "type", "5"))
                .add(new DeleteRequest("test", "type", "6"))
        .get();

        assertNoFailures(indexBulkItemResponse);
        assertThat(bulkItemResponse.getItems().length, is(6));
        assertThat(bulkItemResponse.getItems()[0].getOpType(), is("index"));
        assertThat(bulkItemResponse.getItems()[1].getOpType(), is("index"));
        assertThat(bulkItemResponse.getItems()[2].getOpType(), is("update"));
        assertThat(bulkItemResponse.getItems()[3].getOpType(), is("update"));
        assertThat(bulkItemResponse.getItems()[4].getOpType(), is("delete"));
        assertThat(bulkItemResponse.getItems()[5].getOpType(), is("delete"));
    }


    private static String indexOrAlias() {
        return randomBoolean() ? "test" : "alias";
    }

    @Test // issue 6410
    public void testThatMissingIndexDoesNotAbortFullBulkRequest() throws Exception{
        createIndex("bulkindex1", "bulkindex2");
        ensureYellow();
        BulkRequest bulkRequest = new BulkRequest();
        bulkRequest.add(new IndexRequest("bulkindex1", "index1_type", "1").source("text", "hallo1"))
                   .add(new IndexRequest("bulkindex2", "index2_type", "1").source("text", "hallo2"))
                   .add(new IndexRequest("bulkindex2", "index2_type").source("text", "hallo2"))
                   .add(new UpdateRequest("bulkindex2", "index2_type", "2").doc("foo", "bar"))
                   .add(new DeleteRequest("bulkindex2", "index2_type", "3"))
                   .refresh(true);

        client().bulk(bulkRequest).get();
        SearchResponse searchResponse = client().prepareSearch("bulkindex*").get();
        assertHitCount(searchResponse, 3);

        assertAcked(client().admin().indices().prepareClose("bulkindex2"));

        BulkResponse bulkResponse = client().bulk(bulkRequest).get();
        assertThat(bulkResponse.hasFailures(), is(true));
        assertThat(bulkResponse.getItems().length, is(5));
    }
}

