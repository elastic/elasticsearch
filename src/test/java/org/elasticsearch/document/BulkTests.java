package org.elasticsearch.document;

import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.count.CountResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequestBuilder;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.junit.Test;

import java.util.concurrent.CyclicBarrier;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.*;
import static org.hamcrest.Matchers.*;

/**
 */
public class BulkTests extends ElasticsearchIntegrationTest {


    @Test
    public void testBulkUpdate_simple() throws Exception {
        client().admin().indices().prepareCreate("test")
                .setSettings(
                        ImmutableSettings.settingsBuilder()
                                .put("index.number_of_shards", 2)
                                .put("index.number_of_replicas", 0)
                ).execute().actionGet();
        client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForGreenStatus().execute().actionGet();

        BulkResponse bulkResponse = client().prepareBulk()
                .add(client().prepareIndex().setIndex("test").setType("type1").setId("1").setSource("field", 1))
                .add(client().prepareIndex().setIndex("test").setType("type1").setId("2").setSource("field", 2).setCreate(true))
                .add(client().prepareIndex().setIndex("test").setType("type1").setId("3").setSource("field", 3))
                .add(client().prepareIndex().setIndex("test").setType("type1").setId("4").setSource("field", 4))
                .add(client().prepareIndex().setIndex("test").setType("type1").setId("5").setSource("field", 5))
                .execute().actionGet();

        assertThat(bulkResponse.hasFailures(), equalTo(false));
        assertThat(bulkResponse.getItems().length, equalTo(5));

        bulkResponse = client().prepareBulk()
                .add(client().prepareUpdate().setIndex("test").setType("type1").setId("1").setScript("ctx._source.field += 1"))
                .add(client().prepareUpdate().setIndex("test").setType("type1").setId("2").setScript("ctx._source.field += 1").setRetryOnConflict(3))
                .add(client().prepareUpdate().setIndex("test").setType("type1").setId("3").setDoc(jsonBuilder().startObject().field("field1", "test").endObject()))
                .execute().actionGet();

        assertThat(bulkResponse.hasFailures(), equalTo(false));
        assertThat(bulkResponse.getItems().length, equalTo(3));
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
                .add(client().prepareUpdate().setIndex("test").setType("type1").setId("6").setScript("ctx._source.field += 1")
                        .setUpsert(jsonBuilder().startObject().field("field", 0).endObject()))
                .add(client().prepareUpdate().setIndex("test").setType("type1").setId("7").setScript("ctx._source.field += 1"))
                .add(client().prepareUpdate().setIndex("test").setType("type1").setId("2").setScript("ctx._source.field += 1"))
                .execute().actionGet();

        assertThat(bulkResponse.hasFailures(), equalTo(true));
        assertThat(bulkResponse.getItems().length, equalTo(3));
        assertThat(((UpdateResponse) bulkResponse.getItems()[0].getResponse()).getId(), equalTo("6"));
        assertThat(((UpdateResponse) bulkResponse.getItems()[0].getResponse()).getVersion(), equalTo(1l));
        assertThat(bulkResponse.getItems()[1].getResponse(), nullValue());
        assertThat(bulkResponse.getItems()[1].getFailure().getId(), equalTo("7"));
        assertThat(bulkResponse.getItems()[1].getFailure().getMessage(), containsString("DocumentMissingException"));
        assertThat(((UpdateResponse) bulkResponse.getItems()[2].getResponse()).getId(), equalTo("2"));
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
    public void testBulkUpdate_malformedScripts() throws Exception {

        client().admin().indices().prepareCreate("test")
                .setSettings(
                        ImmutableSettings.settingsBuilder()
                                .put("index.number_of_shards", 2)
                                .put("index.number_of_replicas", 0)
                ).execute().actionGet();
        client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForGreenStatus().execute().actionGet();

        BulkResponse bulkResponse = client().prepareBulk()
                .add(client().prepareIndex().setIndex("test").setType("type1").setId("1").setSource("field", 1))
                .add(client().prepareIndex().setIndex("test").setType("type1").setId("2").setSource("field", 1))
                .add(client().prepareIndex().setIndex("test").setType("type1").setId("3").setSource("field", 1))
                .execute().actionGet();

        assertThat(bulkResponse.hasFailures(), equalTo(false));
        assertThat(bulkResponse.getItems().length, equalTo(3));

        bulkResponse = client().prepareBulk()
                .add(client().prepareUpdate().setIndex("test").setType("type1").setId("1").setScript("ctx._source.field += a").setFields("field"))
                .add(client().prepareUpdate().setIndex("test").setType("type1").setId("2").setScript("ctx._source.field += 1").setFields("field"))
                .add(client().prepareUpdate().setIndex("test").setType("type1").setId("3").setScript("ctx._source.field += a").setFields("field"))
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
        client().admin().indices().prepareCreate("test")
                .setSettings(
                        ImmutableSettings.settingsBuilder()
                                .put("index.number_of_shards", 2)
                                .put("index.number_of_replicas", 1)
                ).execute().actionGet();
        client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForGreenStatus().execute().actionGet();

        int numDocs = 2000;
        BulkRequestBuilder builder = client().prepareBulk();
        for (int i = 0; i < numDocs; i++) {
            builder.add(
                    client().prepareUpdate()
                            .setIndex("test").setType("type1").setId(Integer.toString(i))
                            .setScript("ctx._source.counter += 1").setFields("counter")
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
                updateBuilder.setScript("ctx._source.counter += 1");
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
                            .setIndex("test").setType("type1").setId(Integer.toString(i)).setScript("ctx._source.counter += 1")
            );
        }
        response = builder.execute().actionGet();
        assertThat(response.hasFailures(), equalTo(true));
        assertThat(response.getItems().length, equalTo(numDocs));
        for (int i = 0; i < numDocs; i++) {
            int id = i + (numDocs / 2);
            if (i >= (numDocs / 2)) {
                assertThat(response.getItems()[i].getFailure().getId(), equalTo(Integer.toString(id)));
                assertThat(response.getItems()[i].getFailure().getMessage(), containsString("DocumentMissingException"));
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
                            .setIndex("test").setType("type1").setId(Integer.toString(i)).setScript("ctx.op = \"none\"")
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
                            .setIndex("test").setType("type1").setId(Integer.toString(i)).setScript("ctx.op = \"delete\"")
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

        int shards = 1 + randomInt(10);
        int replica = randomInt(2);

        cluster().ensureAtLeastNumNodes(1 + replica);


        client().admin().indices().prepareCreate("test")
                .setSettings(
                        ImmutableSettings.settingsBuilder()
                                .put("index.number_of_shards", shards)
                                .put("index.number_of_replicas", replica)
                ).execute().actionGet();

        int numDocs = 5000;
        int bulk = 50;
        for (int i = 0; i < numDocs; ) {
            BulkRequestBuilder builder = client().prepareBulk();
            for (int j = 0; j < bulk; j++, i++) {
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
        client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForGreenStatus().execute().actionGet();

        BulkRequestBuilder builder = client().prepareBulk();

        byte[] addParent = new BytesArray("{\"index\" : { \"_index\" : \"test\", \"_type\" : \"parent\", \"_id\" : \"parent1\"}}\n" +
                "{\"field1\" : \"value1\"}\n").array();

        byte[] addChild = new BytesArray("{ \"update\" : { \"_index\" : \"test\", \"_type\" : \"child\", \"_id\" : \"child1\", \"parent\" : \"parent1\"}}\n" +
                "{\"doc\" : { \"field1\" : \"value1\"}, \"doc_as_upsert\" : \"true\"}\n").array();

        builder.add(addParent, 0, addParent.length, false);
        builder.add(addChild, 0, addChild.length, false);

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
        client().admin().indices().prepareCreate("test")
                .addMapping("parent", "{\"parent\":{}}")
                .addMapping("child", "{\"child\": {\"_parent\": {\"type\": \"parent\"}}}")
                .execute().actionGet();
        client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForGreenStatus().execute().actionGet();

        BulkRequestBuilder builder = client().prepareBulk();

        byte[] addParent = new BytesArray("{\"index\" : { \"_index\" : \"test\", \"_type\" : \"parent\", \"_id\" : \"parent1\"}}\n" +
                "{\"field1\" : \"value1\"}\n").array();

        byte[] addChild = new BytesArray("{\"update\" : { \"_id\" : \"child1\", \"_type\" : \"child\", \"_index\" : \"test\", \"parent\" : \"parent1\"} }\n" +
                "{ \"script\" : \"ctx._source.field2 = 'value2'\", \"upsert\" : {\"field1\" : \"value1\"}}\n").array();

        builder.add(addParent, 0, addParent.length, false);
        builder.add(addChild, 0, addChild.length, false);

        BulkResponse bulkResponse = builder.get();
        assertThat(bulkResponse.getItems().length, equalTo(2));
        assertThat(bulkResponse.getItems()[0].isFailed(), equalTo(false));
        assertThat(bulkResponse.getItems()[1].isFailed(), equalTo(false));

        client().admin().indices().prepareRefresh("test").get();

        SearchResponse searchResponse = client().prepareSearch("test")
                .setQuery(QueryBuilders.hasParentQuery("parent", QueryBuilders.matchAllQuery()))
                .get();

        assertNoFailures(searchResponse);
        assertSearchHits(searchResponse, "child1");
    }

    @Test
    public void testFailingVersionedUpdatedOnBulk() throws Exception {
        // This test tries to induce a version conflict during a bulk update.
        // from version 1.0.0 on, ES supports settings a version on those update which makes it easy
        // to create. Here we do a best effort by running 30 concurrent updates, 1000 iterations.
        createIndex("test");
        client().prepareIndex("test", "type", "1").setSource("field", "1").get();
        final BulkResponse[] responses = new BulkResponse[30];
        Thread[] threads = new Thread[responses.length];

        for (int j = 0; j < 1000; j++) {
            final CyclicBarrier cyclicBarrier = new CyclicBarrier(responses.length);

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
                        requestBuilder.add(client().prepareUpdate("test", "type", "1").setDoc("field", threadID));
                        responses[threadID] = requestBuilder.get();

                    }
                });
                threads[threadID].start();

            }

            for (int i = 0; i < threads.length; i++) {
                threads[i].join();
            }

            for (BulkResponse response : responses) {
                for (BulkItemResponse itemResponse : response) {
                    if (itemResponse.isFailed()) {
                        assertThat("\"" + itemResponse.getFailureMessage() + "\" is not a version issue",
                                itemResponse.getFailureMessage().contains("version conflict"), equalTo(true));

                        return; // found what we wanted see Cases #3448
                    }

                }
            }
        }

        logger.info("failed to induce a version conflict.");
    }

}
