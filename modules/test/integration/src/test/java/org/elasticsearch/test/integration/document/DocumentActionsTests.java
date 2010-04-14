/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
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
import org.elasticsearch.action.admin.indices.flush.FlushResponse;
import org.elasticsearch.action.admin.indices.optimize.OptimizeResponse;
import org.elasticsearch.action.admin.indices.refresh.RefreshResponse;
import org.elasticsearch.action.count.CountResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.deletebyquery.DeleteByQueryResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.broadcast.BroadcastOperationThreading;
import org.elasticsearch.client.Client;
import org.elasticsearch.test.integration.AbstractNodesTests;
import org.elasticsearch.util.Unicode;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Map;

import static org.elasticsearch.client.Requests.*;
import static org.elasticsearch.index.query.json.JsonQueryBuilders.*;
import static org.hamcrest.MatcherAssert.*;
import static org.hamcrest.Matchers.*;

/**
 * @author kimchy (shay.banon)
 */
public class DocumentActionsTests extends AbstractNodesTests {

    protected Client client1;
    protected Client client2;

    @BeforeMethod public void startNodes() {
        startNode("server1");
        startNode("server2");
        client1 = getClient1();
        client2 = getClient2();
        createIndex();
    }

    protected void createIndex() {
        logger.info("Creating index test");
        client1.admin().indices().create(createIndexRequest("test")).actionGet();
    }

    protected String getConcreteIndexName() {
        return "test";
    }

    @AfterMethod public void closeNodes() {
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

    @Test public void testIndexActions() throws Exception {
        logger.info("Running Cluster Health");
        ClusterHealthResponse clusterHealth = client1.admin().cluster().health(clusterHealth().waitForGreenStatus()).actionGet();
        logger.info("Done Cluster Health, status " + clusterHealth.status());
        assertThat(clusterHealth.timedOut(), equalTo(false));
        assertThat(clusterHealth.status(), equalTo(ClusterHealthStatus.GREEN));

        logger.info("Indexing [type1/1]");
        IndexResponse indexResponse = client1.index(indexRequest("test").type("type1").id("1").source(source("1", "test"))).actionGet();
        assertThat(indexResponse.index(), equalTo(getConcreteIndexName()));
        assertThat(indexResponse.id(), equalTo("1"));
        assertThat(indexResponse.type(), equalTo("type1"));
        logger.info("Refreshing");
        RefreshResponse refreshResponse = client1.admin().indices().refresh(refreshRequest("test")).actionGet();
        assertThat(refreshResponse.successfulShards(), equalTo(10));
        assertThat(refreshResponse.failedShards(), equalTo(0));

        logger.info("Clearing cache");
        ClearIndicesCacheResponse clearIndicesCacheResponse = client1.admin().indices().clearCache(clearIndicesCache("test")).actionGet();
        assertThat(clearIndicesCacheResponse.successfulShards(), equalTo(10));
        assertThat(clearIndicesCacheResponse.failedShards(), equalTo(0));

        logger.info("Optimizing");
        OptimizeResponse optimizeResponse = client1.admin().indices().optimize(optimizeRequest("test")).actionGet();
        assertThat(optimizeResponse.successfulShards(), equalTo(10));
        assertThat(optimizeResponse.failedShards(), equalTo(0));

        GetResponse getResult;

        logger.info("Get [type1/1]");
        for (int i = 0; i < 5; i++) {
            getResult = client1.get(getRequest("test").type("type1").id("1").operationThreaded(false)).actionGet();
            assertThat(getResult.index(), equalTo(getConcreteIndexName()));
            assertThat("cycle #" + i, getResult.sourceAsString(), equalTo(source("1", "test")));
            assertThat("cycle(map) #" + i, (String) ((Map) getResult.sourceAsMap().get("type1")).get("name"), equalTo("test"));
            getResult = client1.get(getRequest("test").type("type1").id("1").operationThreaded(true)).actionGet();
            assertThat("cycle #" + i, getResult.sourceAsString(), equalTo(source("1", "test")));
            assertThat(getResult.index(), equalTo(getConcreteIndexName()));
        }

        logger.info("Get [type1/2] (should be empty)");
        for (int i = 0; i < 5; i++) {
            getResult = client1.get(getRequest("test").type("type1").id("2")).actionGet();
            assertThat(getResult.exists(), equalTo(false));
        }

        logger.info("Delete [type1/1]");
        DeleteResponse deleteResponse = client1.delete(deleteRequest("test").type("type1").id("1")).actionGet();
        assertThat(deleteResponse.index(), equalTo(getConcreteIndexName()));
        assertThat(deleteResponse.id(), equalTo("1"));
        assertThat(deleteResponse.type(), equalTo("type1"));
        logger.info("Refreshing");
        client1.admin().indices().refresh(refreshRequest("test")).actionGet();

        logger.info("Get [type1/1] (should be empty)");
        for (int i = 0; i < 5; i++) {
            getResult = client1.get(getRequest("test").type("type1").id("1")).actionGet();
            assertThat(getResult.exists(), equalTo(false));
        }

        logger.info("Index [type1/1]");
        client1.index(indexRequest("test").type("type1").id("1").source(source("1", "test"))).actionGet();
        logger.info("Index [type1/2]");
        client1.index(indexRequest("test").type("type1").id("2").source(source("2", "test2"))).actionGet();

        logger.info("Flushing");
        FlushResponse flushResult = client1.admin().indices().flush(flushRequest("test")).actionGet();
        assertThat(flushResult.successfulShards(), equalTo(10));
        assertThat(flushResult.failedShards(), equalTo(0));
        logger.info("Refreshing");
        client1.admin().indices().refresh(refreshRequest("test")).actionGet();

        logger.info("Get [type1/1] and [type1/2]");
        for (int i = 0; i < 5; i++) {
            getResult = client1.get(getRequest("test").type("type1").id("1")).actionGet();
            assertThat(getResult.index(), equalTo(getConcreteIndexName()));
            assertThat("cycle #" + i, getResult.sourceAsString(), equalTo(source("1", "test")));
            getResult = client1.get(getRequest("test").type("type1").id("2")).actionGet();
            assertThat("cycle #" + i, getResult.sourceAsString(), equalTo(source("2", "test2")));
            assertThat(getResult.index(), equalTo(getConcreteIndexName()));
        }

        logger.info("Count");
        // check count
        for (int i = 0; i < 5; i++) {
            // test successful
            CountResponse countResponse = client1.count(countRequest("test").query(termQuery("_type", "type1")).operationThreading(BroadcastOperationThreading.NO_THREADS)).actionGet();
            assertThat(countResponse.count(), equalTo(2l));
            assertThat(countResponse.successfulShards(), equalTo(5));
            assertThat(countResponse.failedShards(), equalTo(0));

            countResponse = client1.count(countRequest("test").query(termQuery("_type", "type1")).operationThreading(BroadcastOperationThreading.SINGLE_THREAD)).actionGet();
            assertThat(countResponse.count(), equalTo(2l));
            assertThat(countResponse.successfulShards(), equalTo(5));
            assertThat(countResponse.failedShards(), equalTo(0));

            countResponse = client1.count(countRequest("test").query(termQuery("_type", "type1")).operationThreading(BroadcastOperationThreading.THREAD_PER_SHARD)).actionGet();
            assertThat(countResponse.count(), equalTo(2l));
            assertThat(countResponse.successfulShards(), equalTo(5));
            assertThat(countResponse.failedShards(), equalTo(0));

            // test failed (simply query that can't be parsed)
            countResponse = client1.count(countRequest("test").query(Unicode.fromStringAsBytes("{ term : { _type : \"type1 } }"))).actionGet();

            assertThat(countResponse.count(), equalTo(0l));
            assertThat(countResponse.successfulShards(), equalTo(0));
            assertThat(countResponse.failedShards(), equalTo(5));
        }

        logger.info("Delete by query");
        DeleteByQueryResponse queryResponse = client2.deleteByQuery(deleteByQueryRequest("test").query(termQuery("name", "test2"))).actionGet();
        assertThat(queryResponse.index(getConcreteIndexName()).successfulShards(), equalTo(5));
        assertThat(queryResponse.index(getConcreteIndexName()).failedShards(), equalTo(0));
        client1.admin().indices().refresh(refreshRequest("test")).actionGet();

        logger.info("Get [type1/1] and [type1/2], should be empty");
        for (int i = 0; i < 5; i++) {
            getResult = client1.get(getRequest("test").type("type1").id("1")).actionGet();
            assertThat(getResult.index(), equalTo(getConcreteIndexName()));
            assertThat("cycle #" + i, getResult.sourceAsString(), equalTo(source("1", "test")));
            getResult = client1.get(getRequest("test").type("type1").id("2")).actionGet();
            assertThat("cycle #" + i, getResult.exists(), equalTo(false));
            assertThat(getResult.index(), equalTo(getConcreteIndexName()));
        }
    }

    private String source(String id, String nameValue) {
        return "{ type1 : { \"id\" : \"" + id + "\", \"name\" : \"" + nameValue + "\" } }";
    }
}
