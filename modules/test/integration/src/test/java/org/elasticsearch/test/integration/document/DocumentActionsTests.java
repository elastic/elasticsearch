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

import org.elasticsearch.action.admin.indices.flush.FlushResponse;
import org.elasticsearch.action.admin.indices.refresh.RefreshResponse;
import org.elasticsearch.action.count.CountResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.deletebyquery.DeleteByQueryResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.broadcast.BroadcastOperationThreading;
import org.elasticsearch.test.integration.AbstractServersTests;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import static org.elasticsearch.client.Requests.*;
import static org.elasticsearch.index.query.json.JsonQueryBuilders.*;
import static org.hamcrest.MatcherAssert.*;
import static org.hamcrest.Matchers.*;

/**
 * @author kimchy (Shay Banon)
 */
public class DocumentActionsTests extends AbstractServersTests {

    @AfterMethod public void closeServers() {
        closeAllServers();
    }

    @Test public void testIndexActions() throws Exception {
        startServer("server1");
        startServer("server2");

        logger.info("Creating index test");
        client("server1").admin().indices().create(createIndexRequest("test")).actionGet();

        logger.info("Indexing [type1/1]");
        IndexResponse indexResponse = client("server1").index(indexRequest("test").type("type1").id("1").source(source("1", "test"))).actionGet();
        assertThat(indexResponse.id(), equalTo("1"));
        assertThat(indexResponse.type(), equalTo("type1"));
        logger.info("Refreshing");
        RefreshResponse refreshResult = client("server1").admin().indices().refresh(refreshRequest("test")).actionGet();
        assertThat(refreshResult.index("test").successfulShards(), equalTo(5));
        assertThat(refreshResult.index("test").failedShards(), equalTo(0));

        GetResponse getResult;

        logger.info("Get [type1/1]");
        for (int i = 0; i < 5; i++) {
            getResult = client("server1").get(getRequest("test").type("type1").id("1").threadedOperation(false)).actionGet();
            assertThat("cycle #" + i, getResult.source(), equalTo(source("1", "test")));
            getResult = client("server1").get(getRequest("test").type("type1").id("1").threadedOperation(true)).actionGet();
            assertThat("cycle #" + i, getResult.source(), equalTo(source("1", "test")));
        }

        logger.info("Get [type1/2] (should be empty)");
        for (int i = 0; i < 5; i++) {
            getResult = client("server1").get(getRequest("test").type("type1").id("2")).actionGet();
            assertThat(getResult.empty(), equalTo(true));
        }

        logger.info("Delete [type1/1]");
        DeleteResponse deleteResponse = client("server1").delete(deleteRequest("test").type("type1").id("1")).actionGet();
        assertThat(deleteResponse.id(), equalTo("1"));
        assertThat(deleteResponse.type(), equalTo("type1"));
        logger.info("Refreshing");
        client("server1").admin().indices().refresh(refreshRequest("test")).actionGet();

        logger.info("Get [type1/1] (should be empty)");
        for (int i = 0; i < 5; i++) {
            getResult = client("server1").get(getRequest("test").type("type1").id("1")).actionGet();
            assertThat(getResult.empty(), equalTo(true));
        }

        logger.info("Index [type1/1]");
        client("server1").index(indexRequest("test").type("type1").id("1").source(source("1", "test"))).actionGet();
        logger.info("Index [type1/2]");
        client("server1").index(indexRequest("test").type("type1").id("2").source(source("2", "test"))).actionGet();

        logger.info("Flushing");
        FlushResponse flushResult = client("server1").admin().indices().flush(flushRequest("test")).actionGet();
        assertThat(flushResult.index("test").successfulShards(), equalTo(5));
        assertThat(flushResult.index("test").failedShards(), equalTo(0));
        logger.info("Refreshing");
        client("server1").admin().indices().refresh(refreshRequest("test")).actionGet();

        logger.info("Get [type1/1] and [type1/2]");
        for (int i = 0; i < 5; i++) {
            getResult = client("server1").get(getRequest("test").type("type1").id("1")).actionGet();
            assertThat("cycle #" + i, getResult.source(), equalTo(source("1", "test")));
            getResult = client("server1").get(getRequest("test").type("type1").id("2")).actionGet();
            assertThat("cycle #" + i, getResult.source(), equalTo(source("2", "test")));
        }

        logger.info("Count");
        // check count
        for (int i = 0; i < 5; i++) {
            // test successful
            CountResponse countResponse = client("server1").count(countRequest("test").querySource(termQuery("_type", "type1")).operationThreading(BroadcastOperationThreading.NO_THREADS)).actionGet();
            assertThat(countResponse.count(), equalTo(2l));
            assertThat(countResponse.successfulShards(), equalTo(5));
            assertThat(countResponse.failedShards(), equalTo(0));

            countResponse = client("server1").count(countRequest("test").querySource(termQuery("_type", "type1")).operationThreading(BroadcastOperationThreading.SINGLE_THREAD)).actionGet();
            assertThat(countResponse.count(), equalTo(2l));
            assertThat(countResponse.successfulShards(), equalTo(5));
            assertThat(countResponse.failedShards(), equalTo(0));

            countResponse = client("server1").count(countRequest("test").querySource(termQuery("_type", "type1")).operationThreading(BroadcastOperationThreading.THREAD_PER_SHARD)).actionGet();
            assertThat(countResponse.count(), equalTo(2l));
            assertThat(countResponse.successfulShards(), equalTo(5));
            assertThat(countResponse.failedShards(), equalTo(0));

            // test failed (simply query that can't be parsed)
            countResponse = client("server1").count(countRequest("test").querySource("{ term : { _type : \"type1 } }")).actionGet();

            assertThat(countResponse.count(), equalTo(0l));
            assertThat(countResponse.successfulShards(), equalTo(0));
            assertThat(countResponse.failedShards(), equalTo(5));
        }

        logger.info("Delete by query");
        DeleteByQueryResponse queryResponse = client("server2").deleteByQuery(deleteByQueryRequest("test").querySource(termQuery("name", "test2"))).actionGet();
        assertThat(queryResponse.index("test").successfulShards(), equalTo(5));
        assertThat(queryResponse.index("test").failedShards(), equalTo(0));
        client("server1").admin().indices().refresh(refreshRequest("test")).actionGet();

        logger.info("Get [type1/1] and [type1/2], should be empty");
        for (int i = 0; i < 5; i++) {
            getResult = client("server1").get(getRequest("test").type("type1").id("1")).actionGet();
            assertThat("cycle #" + i, getResult.source(), equalTo(source("1", "test")));
            getResult = client("server1").get(getRequest("test").type("type1").id("2")).actionGet();
            assertThat("cycle #" + i, getResult.empty(), equalTo(false));
        }
    }

    private String source(String id, String nameValue) {
        return "{ type1 : { \"id\" : \"" + id + "\", \"name\" : \"" + nameValue + "\" } }";
    }
}
