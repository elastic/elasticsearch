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

import org.elasticsearch.action.admin.indices.cache.clear.ClearIndicesCacheResponse;
import org.elasticsearch.action.admin.indices.flush.FlushResponse;
import org.elasticsearch.action.admin.indices.forcemerge.ForceMergeResponse;
import org.elasticsearch.action.admin.indices.refresh.RefreshResponse;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.WriteRequest.RefreshPolicy;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.hamcrest.ElasticsearchAssertions;

import java.io.IOException;

import static org.elasticsearch.action.DocWriteRequest.OpType;
import static org.elasticsearch.client.Requests.clearIndicesCacheRequest;
import static org.elasticsearch.client.Requests.getRequest;
import static org.elasticsearch.client.Requests.indexRequest;
import static org.elasticsearch.client.Requests.refreshRequest;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

/**
 * Integration test for document action like index, bulk, and get. It has a very long history: it was in the second commit of Elasticsearch.
 */
public class DocumentActionsIT extends ESIntegTestCase {
    protected void createIndex() {
        ElasticsearchAssertions.assertAcked(prepareCreate(getConcreteIndexName()).addMapping("type1", "name", "type=keyword,store=true"));
    }

    protected String getConcreteIndexName() {
        return "test";
    }

    public void testIndexActions() throws Exception {
        createIndex();
        NumShards numShards = getNumShards(getConcreteIndexName());
        logger.info("Running Cluster Health");
        ensureGreen();
        logger.info("Indexing [type1/1]");
        IndexResponse indexResponse = client().prepareIndex().setIndex("test").setType("type1").setId("1").setSource(source("1", "test"))
                .setRefreshPolicy(RefreshPolicy.IMMEDIATE).get();
        assertThat(indexResponse.getIndex(), equalTo(getConcreteIndexName()));
        assertThat(indexResponse.getId(), equalTo("1"));
        assertThat(indexResponse.getType(), equalTo("type1"));
        logger.info("Refreshing");
        RefreshResponse refreshResponse = refresh();
        assertThat(refreshResponse.getSuccessfulShards(), equalTo(numShards.totalNumShards));

        logger.info("--> index exists?");
        assertThat(indexExists(getConcreteIndexName()), equalTo(true));
        logger.info("--> index exists?, fake index");
        assertThat(indexExists("test1234565"), equalTo(false));

        logger.info("Clearing cache");
        ClearIndicesCacheResponse clearIndicesCacheResponse = client().admin().indices().clearCache(clearIndicesCacheRequest("test")
            .fieldDataCache(true).queryCache(true)).actionGet();
        assertNoFailures(clearIndicesCacheResponse);
        assertThat(clearIndicesCacheResponse.getSuccessfulShards(), equalTo(numShards.totalNumShards));

        logger.info("Force Merging");
        waitForRelocation(ClusterHealthStatus.GREEN);
        ForceMergeResponse mergeResponse = forceMerge();
        assertThat(mergeResponse.getSuccessfulShards(), equalTo(numShards.totalNumShards));

        GetResponse getResult;

        logger.info("Get [type1/1]");
        for (int i = 0; i < 5; i++) {
            getResult = client().prepareGet("test", "type1", "1").execute().actionGet();
            assertThat(getResult.getIndex(), equalTo(getConcreteIndexName()));
            assertThat("cycle #" + i, getResult.getSourceAsString(), equalTo(Strings.toString(source("1", "test"))));
            assertThat("cycle(map) #" + i, (String) getResult.getSourceAsMap().get("name"), equalTo("test"));
            getResult = client().get(getRequest("test").type("type1").id("1")).actionGet();
            assertThat("cycle #" + i, getResult.getSourceAsString(), equalTo(Strings.toString(source("1", "test"))));
            assertThat(getResult.getIndex(), equalTo(getConcreteIndexName()));
        }

        logger.info("Get [type1/1] with script");
        for (int i = 0; i < 5; i++) {
            getResult = client().prepareGet("test", "type1", "1").setStoredFields("name").execute().actionGet();
            assertThat(getResult.getIndex(), equalTo(getConcreteIndexName()));
            assertThat(getResult.isExists(), equalTo(true));
            assertThat(getResult.getSourceAsBytes(), nullValue());
            assertThat(getResult.getField("name").getValues().get(0).toString(), equalTo("test"));
        }

        logger.info("Get [type1/2] (should be empty)");
        for (int i = 0; i < 5; i++) {
            getResult = client().get(getRequest("test").type("type1").id("2")).actionGet();
            assertThat(getResult.isExists(), equalTo(false));
        }

        logger.info("Delete [type1/1]");
        DeleteResponse deleteResponse = client().prepareDelete("test", "type1", "1").execute().actionGet();
        assertThat(deleteResponse.getIndex(), equalTo(getConcreteIndexName()));
        assertThat(deleteResponse.getId(), equalTo("1"));
        assertThat(deleteResponse.getType(), equalTo("type1"));
        logger.info("Refreshing");
        client().admin().indices().refresh(refreshRequest("test")).actionGet();

        logger.info("Get [type1/1] (should be empty)");
        for (int i = 0; i < 5; i++) {
            getResult = client().get(getRequest("test").type("type1").id("1")).actionGet();
            assertThat(getResult.isExists(), equalTo(false));
        }

        logger.info("Index [type1/1]");
        client().index(indexRequest("test").type("type1").id("1").source(source("1", "test"))).actionGet();
        logger.info("Index [type1/2]");
        client().index(indexRequest("test").type("type1").id("2").source(source("2", "test2"))).actionGet();

        logger.info("Flushing");
        FlushResponse flushResult = client().admin().indices().prepareFlush("test").execute().actionGet();
        assertThat(flushResult.getSuccessfulShards(), equalTo(numShards.totalNumShards));
        assertThat(flushResult.getFailedShards(), equalTo(0));
        logger.info("Refreshing");
        client().admin().indices().refresh(refreshRequest("test")).actionGet();

        logger.info("Get [type1/1] and [type1/2]");
        for (int i = 0; i < 5; i++) {
            getResult = client().get(getRequest("test").type("type1").id("1")).actionGet();
            assertThat(getResult.getIndex(), equalTo(getConcreteIndexName()));
            assertThat("cycle #" + i, getResult.getSourceAsString(), equalTo(Strings.toString(source("1", "test"))));
            getResult = client().get(getRequest("test").type("type1").id("2")).actionGet();
            String ste1 = getResult.getSourceAsString();
            String ste2 = Strings.toString(source("2", "test2"));
            assertThat("cycle #" + i, ste1, equalTo(ste2));
            assertThat(getResult.getIndex(), equalTo(getConcreteIndexName()));
        }

        logger.info("Count");
        // check count
        for (int i = 0; i < 5; i++) {
            // test successful
            SearchResponse countResponse = client().prepareSearch("test").setSize(0).setQuery(termQuery("_type", "type1"))
                .execute().actionGet();
            assertNoFailures(countResponse);
            assertThat(countResponse.getHits().getTotalHits().value, equalTo(2L));
            assertThat(countResponse.getSuccessfulShards(), equalTo(numShards.numPrimaries));
            assertThat(countResponse.getFailedShards(), equalTo(0));

            // count with no query is a match all one
            countResponse = client().prepareSearch("test").setSize(0).execute().actionGet();
            assertThat("Failures " + countResponse.getShardFailures(), countResponse.getShardFailures() == null ? 0
                : countResponse.getShardFailures().length, equalTo(0));
            assertThat(countResponse.getHits().getTotalHits().value, equalTo(2L));
            assertThat(countResponse.getSuccessfulShards(), equalTo(numShards.numPrimaries));
            assertThat(countResponse.getFailedShards(), equalTo(0));
        }
    }

    public void testBulk() throws Exception {
        createIndex();
        NumShards numShards = getNumShards(getConcreteIndexName());
        logger.info("-> running Cluster Health");
        ensureGreen();

        BulkResponse bulkResponse = client().prepareBulk()
                .add(client().prepareIndex().setIndex("test").setType("type1").setId("1").setSource(source("1", "test")))
                .add(client().prepareIndex().setIndex("test").setType("type1").setId("2").setSource(source("2", "test")).setCreate(true))
                .add(client().prepareIndex().setIndex("test").setType("type1").setSource(source("3", "test")))
                .add(client().prepareDelete().setIndex("test").setType("type1").setId("1"))
                .add(client().prepareIndex().setIndex("test").setType("type1").setSource("{ xxx }", XContentType.JSON)) // failure
                .execute().actionGet();

        assertThat(bulkResponse.hasFailures(), equalTo(true));
        assertThat(bulkResponse.getItems().length, equalTo(5));

        assertThat(bulkResponse.getItems()[0].isFailed(), equalTo(false));
        assertThat(bulkResponse.getItems()[0].getOpType(), equalTo(OpType.INDEX));
        assertThat(bulkResponse.getItems()[0].getIndex(), equalTo(getConcreteIndexName()));
        assertThat(bulkResponse.getItems()[0].getType(), equalTo("type1"));
        assertThat(bulkResponse.getItems()[0].getId(), equalTo("1"));

        assertThat(bulkResponse.getItems()[1].isFailed(), equalTo(false));
        assertThat(bulkResponse.getItems()[1].getOpType(), equalTo(OpType.CREATE));
        assertThat(bulkResponse.getItems()[1].getIndex(), equalTo(getConcreteIndexName()));
        assertThat(bulkResponse.getItems()[1].getType(), equalTo("type1"));
        assertThat(bulkResponse.getItems()[1].getId(), equalTo("2"));

        assertThat(bulkResponse.getItems()[2].isFailed(), equalTo(false));
        assertThat(bulkResponse.getItems()[2].getOpType(), equalTo(OpType.INDEX));
        assertThat(bulkResponse.getItems()[2].getIndex(), equalTo(getConcreteIndexName()));
        assertThat(bulkResponse.getItems()[2].getType(), equalTo("type1"));
        String generatedId3 = bulkResponse.getItems()[2].getId();

        assertThat(bulkResponse.getItems()[3].isFailed(), equalTo(false));
        assertThat(bulkResponse.getItems()[3].getOpType(), equalTo(OpType.DELETE));
        assertThat(bulkResponse.getItems()[3].getIndex(), equalTo(getConcreteIndexName()));
        assertThat(bulkResponse.getItems()[3].getType(), equalTo("type1"));
        assertThat(bulkResponse.getItems()[3].getId(), equalTo("1"));

        assertThat(bulkResponse.getItems()[4].isFailed(), equalTo(true));
        assertThat(bulkResponse.getItems()[4].getOpType(), equalTo(OpType.INDEX));
        assertThat(bulkResponse.getItems()[4].getIndex(), equalTo(getConcreteIndexName()));
        assertThat(bulkResponse.getItems()[4].getType(), equalTo("type1"));

        waitForRelocation(ClusterHealthStatus.GREEN);
        RefreshResponse refreshResponse = client().admin().indices().prepareRefresh("test").execute().actionGet();
        assertNoFailures(refreshResponse);
        assertThat(refreshResponse.getSuccessfulShards(), equalTo(numShards.totalNumShards));


        for (int i = 0; i < 5; i++) {
            GetResponse getResult = client().get(getRequest("test").type("type1").id("1")).actionGet();
            assertThat(getResult.getIndex(), equalTo(getConcreteIndexName()));
            assertThat("cycle #" + i, getResult.isExists(), equalTo(false));

            getResult = client().get(getRequest("test").type("type1").id("2")).actionGet();
            assertThat("cycle #" + i, getResult.getSourceAsString(), equalTo(Strings.toString(source("2", "test"))));
            assertThat(getResult.getIndex(), equalTo(getConcreteIndexName()));

            getResult = client().get(getRequest("test").type("type1").id(generatedId3)).actionGet();
            assertThat("cycle #" + i, getResult.getSourceAsString(), equalTo(Strings.toString(source("3", "test"))));
            assertThat(getResult.getIndex(), equalTo(getConcreteIndexName()));
        }
    }

    private XContentBuilder source(String id, String nameValue) throws IOException {
        return XContentFactory.jsonBuilder().startObject().field("id", id).field("name", nameValue).endObject();
    }
}
