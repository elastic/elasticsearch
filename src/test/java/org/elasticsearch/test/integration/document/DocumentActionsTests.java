/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.test.integration.document;

import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthStatus;
import org.elasticsearch.action.admin.indices.cache.clear.ClearIndicesCacheResponse;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.admin.indices.flush.FlushResponse;
import org.elasticsearch.action.admin.indices.optimize.OptimizeResponse;
import org.elasticsearch.action.admin.indices.refresh.RefreshResponse;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.count.CountResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.deletebyquery.DeleteByQueryResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.broadcast.BroadcastOperationThreading;
import org.elasticsearch.action.support.replication.ReplicationType;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.Unicode;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.test.integration.AbstractNodesTests;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.Map;

import static org.elasticsearch.client.Requests.*;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

/**
 *
 */
public class DocumentActionsTests extends AbstractNodesTests {

    protected Client client1;
    protected Client client2;

    @BeforeClass
    public void startNodes() {
        startNode("server1", nodeSettings());
        startNode("server2", nodeSettings());
        client1 = getClient1();
        client2 = getClient2();

        // no indices, check that simple operations fail
        try {
            client1.prepareCount("test").setQuery(termQuery("_type", "type1")).setOperationThreading(BroadcastOperationThreading.NO_THREADS).execute().actionGet();
            assert false : "should fail";
        } catch (Exception e) {
            // all is well
        }
        client1.prepareCount().setQuery(termQuery("_type", "type1")).setOperationThreading(BroadcastOperationThreading.NO_THREADS).execute().actionGet();
    }

    protected void createIndex() {
        try {
            client1.admin().indices().prepareDelete("test").execute().actionGet();
        } catch (Exception e) {
            // ignore
        }
        logger.info("--> creating index test");
        client1.admin().indices().create(createIndexRequest("test")).actionGet();
    }

    protected Settings nodeSettings() {
        return ImmutableSettings.Builder.EMPTY_SETTINGS;
    }

    protected String getConcreteIndexName() {
        return "test";
    }

    @AfterClass
    public void closeNodes() {
        client1.close();
        client2.close();
        closeAllNodes();
    }

    protected Client getClient1() {
        return client("server1");
    }

    protected Client getClient2() {
        return client("server2");
    }

    @Test
    public void testIndexActions() throws Exception {
        createIndex();
        logger.info("Running Cluster Health");
        ClusterHealthResponse clusterHealth = client1.admin().cluster().health(clusterHealthRequest().setWaitForGreenStatus()).actionGet();
        logger.info("Done Cluster Health, status " + clusterHealth.getStatus());
        assertThat(clusterHealth.isTimedOut(), equalTo(false));
        assertThat(clusterHealth.getStatus(), equalTo(ClusterHealthStatus.GREEN));

        logger.info("Indexing [type1/1]");
        IndexResponse indexResponse = client1.prepareIndex().setIndex("test").setType("type1").setId("1").setSource(source("1", "test")).setRefresh(true).execute().actionGet();
        assertThat(indexResponse.getIndex(), equalTo(getConcreteIndexName()));
        assertThat(indexResponse.getId(), equalTo("1"));
        assertThat(indexResponse.getType(), equalTo("type1"));
        logger.info("Refreshing");
        RefreshResponse refreshResponse = client1.admin().indices().prepareRefresh("test").execute().actionGet();
        assertThat(refreshResponse.getSuccessfulShards(), equalTo(10));
        assertThat(refreshResponse.getFailedShards(), equalTo(0));

        logger.info("--> index exists?");
        IndicesExistsResponse indicesExistsResponse = client1.admin().indices().prepareExists(getConcreteIndexName()).execute().actionGet();
        assertThat(indicesExistsResponse.isExists(), equalTo(true));

        logger.info("--> index exists?, fake index");
        indicesExistsResponse = client1.admin().indices().prepareExists("test1234565").execute().actionGet();
        assertThat(indicesExistsResponse.isExists(), equalTo(false));

        logger.info("Clearing cache");
        ClearIndicesCacheResponse clearIndicesCacheResponse = client1.admin().indices().clearCache(clearIndicesCacheRequest("test")).actionGet();
        assertThat(clearIndicesCacheResponse.getSuccessfulShards(), equalTo(10));
        assertThat(clearIndicesCacheResponse.getFailedShards(), equalTo(0));

        logger.info("Optimizing");
        OptimizeResponse optimizeResponse = client1.admin().indices().prepareOptimize("test").execute().actionGet();
        assertThat(optimizeResponse.getSuccessfulShards(), equalTo(10));
        assertThat(optimizeResponse.getFailedShards(), equalTo(0));

        GetResponse getResult;

        logger.info("Get [type1/1]");
        for (int i = 0; i < 5; i++) {
            getResult = client1.prepareGet("test", "type1", "1").setOperationThreaded(false).execute().actionGet();
            assertThat(getResult.getIndex(), equalTo(getConcreteIndexName()));
            assertThat("cycle #" + i, getResult.getSourceAsString(), equalTo(source("1", "test").string()));
            assertThat("cycle(map) #" + i, (String) ((Map) getResult.getSourceAsMap().get("type1")).get("name"), equalTo("test"));
            getResult = client1.get(getRequest("test").setType("type1").setId("1").setOperationThreaded(true)).actionGet();
            assertThat("cycle #" + i, getResult.getSourceAsString(), equalTo(source("1", "test").string()));
            assertThat(getResult.getIndex(), equalTo(getConcreteIndexName()));
        }

        logger.info("Get [type1/1] with script");
        for (int i = 0; i < 5; i++) {
            getResult = client1.prepareGet("test", "type1", "1").setFields("_source.type1.name").execute().actionGet();
            assertThat(getResult.getIndex(), equalTo(getConcreteIndexName()));
            assertThat(getResult.isExists(), equalTo(true));
            assertThat(getResult.getSourceAsBytes(), nullValue());
            assertThat(getResult.getField("_source.type1.name").getValues().get(0).toString(), equalTo("test"));
        }

        logger.info("Get [type1/2] (should be empty)");
        for (int i = 0; i < 5; i++) {
            getResult = client1.get(getRequest("test").setType("type1").setId("2")).actionGet();
            assertThat(getResult.isExists(), equalTo(false));
        }

        logger.info("Delete [type1/1]");
        DeleteResponse deleteResponse = client1.prepareDelete("test", "type1", "1").setReplicationType(ReplicationType.SYNC).execute().actionGet();
        assertThat(deleteResponse.getIndex(), equalTo(getConcreteIndexName()));
        assertThat(deleteResponse.getId(), equalTo("1"));
        assertThat(deleteResponse.getType(), equalTo("type1"));
        logger.info("Refreshing");
        client1.admin().indices().refresh(refreshRequest("test")).actionGet();

        logger.info("Get [type1/1] (should be empty)");
        for (int i = 0; i < 5; i++) {
            getResult = client1.get(getRequest("test").setType("type1").setId("1")).actionGet();
            assertThat(getResult.isExists(), equalTo(false));
        }

        logger.info("Index [type1/1]");
        client1.index(indexRequest("test").setType("type1").setId("1").setSource(source("1", "test"))).actionGet();
        logger.info("Index [type1/2]");
        client1.index(indexRequest("test").setType("type1").setId("2").setSource(source("2", "test2"))).actionGet();

        logger.info("Flushing");
        FlushResponse flushResult = client1.admin().indices().prepareFlush("test").execute().actionGet();
        assertThat(flushResult.getSuccessfulShards(), equalTo(10));
        assertThat(flushResult.getFailedShards(), equalTo(0));
        logger.info("Refreshing");
        client1.admin().indices().refresh(refreshRequest("test")).actionGet();

        logger.info("Get [type1/1] and [type1/2]");
        for (int i = 0; i < 5; i++) {
            getResult = client1.get(getRequest("test").setType("type1").setId("1")).actionGet();
            assertThat(getResult.getIndex(), equalTo(getConcreteIndexName()));
            assertThat("cycle #" + i, getResult.getSourceAsString(), equalTo(source("1", "test").string()));
            getResult = client1.get(getRequest("test").setType("type1").setId("2")).actionGet();
            String ste1 = getResult.getSourceAsString();
            String ste2 = source("2", "test2").string();
            assertThat("cycle #" + i, ste1, equalTo(ste2));
            assertThat(getResult.getIndex(), equalTo(getConcreteIndexName()));
        }

        logger.info("Count");
        // check count
        for (int i = 0; i < 5; i++) {
            // test successful
            CountResponse countResponse = client1.prepareCount("test").setQuery(termQuery("_type", "type1")).setOperationThreading(BroadcastOperationThreading.NO_THREADS).execute().actionGet();
            assertThat("Failures " + countResponse.getShardFailures(), countResponse.getShardFailures().size(), equalTo(0));
            assertThat(countResponse.getCount(), equalTo(2l));
            assertThat(countResponse.getSuccessfulShards(), equalTo(5));
            assertThat(countResponse.getFailedShards(), equalTo(0));

            countResponse = client1.count(countRequest("test").setQuery(termQuery("_type", "type1")).setOperationThreading(BroadcastOperationThreading.SINGLE_THREAD)).actionGet();
            assertThat(countResponse.getCount(), equalTo(2l));
            assertThat(countResponse.getSuccessfulShards(), equalTo(5));
            assertThat(countResponse.getFailedShards(), equalTo(0));

            countResponse = client1.count(countRequest("test").setQuery(termQuery("_type", "type1")).setOperationThreading(BroadcastOperationThreading.THREAD_PER_SHARD)).actionGet();
            assertThat(countResponse.getCount(), equalTo(2l));
            assertThat(countResponse.getSuccessfulShards(), equalTo(5));
            assertThat(countResponse.getFailedShards(), equalTo(0));

            // test failed (simply query that can't be parsed)
            countResponse = client1.count(countRequest("test").setQuery(Unicode.fromStringAsBytes("{ term : { _type : \"type1 } }"))).actionGet();

            assertThat(countResponse.getCount(), equalTo(0l));
            assertThat(countResponse.getSuccessfulShards(), equalTo(0));
            assertThat(countResponse.getFailedShards(), equalTo(5));

            // count with no query is a match all one
            countResponse = client1.prepareCount("test").execute().actionGet();
            assertThat("Failures " + countResponse.getShardFailures(), countResponse.getShardFailures().size(), equalTo(0));
            assertThat(countResponse.getCount(), equalTo(2l));
            assertThat(countResponse.getSuccessfulShards(), equalTo(5));
            assertThat(countResponse.getFailedShards(), equalTo(0));
        }

        logger.info("Delete by query");
        DeleteByQueryResponse queryResponse = client2.prepareDeleteByQuery().setIndices("test").setQuery(termQuery("name", "test2")).execute().actionGet();
        assertThat(queryResponse.getIndex(getConcreteIndexName()).getSuccessfulShards(), equalTo(5));
        assertThat(queryResponse.getIndex(getConcreteIndexName()).getFailedShards(), equalTo(0));
        client1.admin().indices().refresh(refreshRequest("test")).actionGet();

        logger.info("Get [type1/1] and [type1/2], should be empty");
        for (int i = 0; i < 5; i++) {
            getResult = client1.get(getRequest("test").setType("type1").setId("1")).actionGet();
            assertThat(getResult.getIndex(), equalTo(getConcreteIndexName()));
            assertThat("cycle #" + i, getResult.getSourceAsString(), equalTo(source("1", "test").string()));
            getResult = client1.get(getRequest("test").setType("type1").setId("2")).actionGet();
            assertThat("cycle #" + i, getResult.isExists(), equalTo(false));
            assertThat(getResult.getIndex(), equalTo(getConcreteIndexName()));
        }
    }

    @Test
    public void testBulk() throws Exception {
        createIndex();
        logger.info("-> running Cluster Health");
        ClusterHealthResponse clusterHealth = client1.admin().cluster().health(clusterHealthRequest().setWaitForGreenStatus()).actionGet();
        logger.info("Done Cluster Health, status " + clusterHealth.getStatus());
        assertThat(clusterHealth.isTimedOut(), equalTo(false));
        assertThat(clusterHealth.getStatus(), equalTo(ClusterHealthStatus.GREEN));

        BulkResponse bulkResponse = client1.prepareBulk()
                .add(client1.prepareIndex().setIndex("test").setType("type1").setId("1").setSource(source("1", "test")))
                .add(client1.prepareIndex().setIndex("test").setType("type1").setId("2").setSource(source("2", "test")).setCreate(true))
                .add(client1.prepareIndex().setIndex("test").setType("type1").setSource(source("3", "test")))
                .add(client1.prepareDelete().setIndex("test").setType("type1").setId("1"))
                .add(client1.prepareIndex().setIndex("test").setType("type1").setSource("{ xxx }")) // failure
                .execute().actionGet();

        assertThat(bulkResponse.hasFailures(), equalTo(true));
        assertThat(bulkResponse.getItems().length, equalTo(5));

        assertThat(bulkResponse.getItems()[0].isFailed(), equalTo(false));
        assertThat(bulkResponse.getItems()[0].getOpType(), equalTo("index"));
        assertThat(bulkResponse.getItems()[0].getIndex(), equalTo(getConcreteIndexName()));
        assertThat(bulkResponse.getItems()[0].getType(), equalTo("type1"));
        assertThat(bulkResponse.getItems()[0].getId(), equalTo("1"));

        assertThat(bulkResponse.getItems()[1].isFailed(), equalTo(false));
        assertThat(bulkResponse.getItems()[1].getOpType(), equalTo("create"));
        assertThat(bulkResponse.getItems()[1].getIndex(), equalTo(getConcreteIndexName()));
        assertThat(bulkResponse.getItems()[1].getType(), equalTo("type1"));
        assertThat(bulkResponse.getItems()[1].getId(), equalTo("2"));

        assertThat(bulkResponse.getItems()[2].isFailed(), equalTo(false));
        assertThat(bulkResponse.getItems()[2].getOpType(), equalTo("create"));
        assertThat(bulkResponse.getItems()[2].getIndex(), equalTo(getConcreteIndexName()));
        assertThat(bulkResponse.getItems()[2].getType(), equalTo("type1"));
        String generatedId3 = bulkResponse.getItems()[2].getId();

        assertThat(bulkResponse.getItems()[3].isFailed(), equalTo(false));
        assertThat(bulkResponse.getItems()[3].getOpType(), equalTo("delete"));
        assertThat(bulkResponse.getItems()[3].getIndex(), equalTo(getConcreteIndexName()));
        assertThat(bulkResponse.getItems()[3].getType(), equalTo("type1"));
        assertThat(bulkResponse.getItems()[3].getId(), equalTo("1"));

        assertThat(bulkResponse.getItems()[4].isFailed(), equalTo(true));
        assertThat(bulkResponse.getItems()[4].getOpType(), equalTo("create"));
        assertThat(bulkResponse.getItems()[4].getIndex(), equalTo(getConcreteIndexName()));
        assertThat(bulkResponse.getItems()[4].getType(), equalTo("type1"));

        RefreshResponse refreshResponse = client1.admin().indices().prepareRefresh("test").execute().actionGet();
        assertThat(refreshResponse.getSuccessfulShards(), equalTo(10));
        assertThat(refreshResponse.getFailedShards(), equalTo(0));


        for (int i = 0; i < 5; i++) {
            GetResponse getResult = client1.get(getRequest("test").setType("type1").setId("1")).actionGet();
            assertThat(getResult.getIndex(), equalTo(getConcreteIndexName()));
            assertThat("cycle #" + i, getResult.isExists(), equalTo(false));

            getResult = client1.get(getRequest("test").setType("type1").setId("2")).actionGet();
            assertThat("cycle #" + i, getResult.getSourceAsString(), equalTo(source("2", "test").string()));
            assertThat(getResult.getIndex(), equalTo(getConcreteIndexName()));

            getResult = client1.get(getRequest("test").setType("type1").setId(generatedId3)).actionGet();
            assertThat("cycle #" + i, getResult.getSourceAsString(), equalTo(source("3", "test").string()));
            assertThat(getResult.getIndex(), equalTo(getConcreteIndexName()));
        }
    }

    private XContentBuilder source(String id, String nameValue) throws IOException {
        return XContentFactory.jsonBuilder().startObject().startObject("type1").field("id", id).field("name", nameValue).endObject().endObject();
    }
}
