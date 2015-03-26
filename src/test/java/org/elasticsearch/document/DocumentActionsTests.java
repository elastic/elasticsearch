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
import org.elasticsearch.action.admin.cluster.health.ClusterHealthStatus;
import org.elasticsearch.action.admin.indices.cache.clear.ClearIndicesCacheResponse;
import org.elasticsearch.action.admin.indices.flush.FlushResponse;
import org.elasticsearch.action.admin.indices.optimize.OptimizeResponse;
import org.elasticsearch.action.admin.indices.refresh.RefreshResponse;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.count.CountResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.deletebyquery.DeleteByQueryResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.junit.Test;

import java.io.IOException;

import static org.elasticsearch.client.Requests.*;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.hamcrest.Matchers.*;

/**
 *
 */
public class DocumentActionsTests extends ElasticsearchIntegrationTest {

    protected void createIndex() {
        createIndex(getConcreteIndexName());
    }


    protected String getConcreteIndexName() {
        return "test";
    }

    @Test
    public void testIndexActions() throws Exception {
        createIndex();
        NumShards numShards = getNumShards(getConcreteIndexName());
        logger.info("Running Cluster Health");
        ensureGreen();
        logger.info("Indexing [type1/1]");
        IndexResponse indexResponse = client().prepareIndex().setIndex("test").setType("type1").setId("1").setSource(source("1", "test")).setRefresh(true).execute().actionGet();
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
        ClearIndicesCacheResponse clearIndicesCacheResponse = client().admin().indices().clearCache(clearIndicesCacheRequest("test").recycler(true).fieldDataCache(true).filterCache(true).idCache(true)).actionGet();
        assertNoFailures(clearIndicesCacheResponse);
        assertThat(clearIndicesCacheResponse.getSuccessfulShards(), equalTo(numShards.totalNumShards));

        logger.info("Optimizing");
        waitForRelocation(ClusterHealthStatus.GREEN);
        OptimizeResponse optimizeResponse = optimize();
        assertThat(optimizeResponse.getSuccessfulShards(), equalTo(numShards.totalNumShards));

        GetResponse getResult;

        logger.info("Get [type1/1]");
        for (int i = 0; i < 5; i++) {
            getResult = client().prepareGet("test", "type1", "1").setOperationThreaded(false).execute().actionGet();
            assertThat(getResult.getIndex(), equalTo(getConcreteIndexName()));
            assertThat("cycle #" + i, getResult.getSourceAsString(), equalTo(source("1", "test").string()));
            assertThat("cycle(map) #" + i, (String) getResult.getSourceAsMap().get("name"), equalTo("test"));
            getResult = client().get(getRequest("test").type("type1").id("1").operationThreaded(true)).actionGet();
            assertThat("cycle #" + i, getResult.getSourceAsString(), equalTo(source("1", "test").string()));
            assertThat(getResult.getIndex(), equalTo(getConcreteIndexName()));
        }

        logger.info("Get [type1/1] with script");
        for (int i = 0; i < 5; i++) {
            getResult = client().prepareGet("test", "type1", "1").setFields("name").execute().actionGet();
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
            assertThat("cycle #" + i, getResult.getSourceAsString(), equalTo(source("1", "test").string()));
            getResult = client().get(getRequest("test").type("type1").id("2")).actionGet();
            String ste1 = getResult.getSourceAsString();
            String ste2 = source("2", "test2").string();
            assertThat("cycle #" + i, ste1, equalTo(ste2));
            assertThat(getResult.getIndex(), equalTo(getConcreteIndexName()));
        }

        logger.info("Count");
        // check count
        for (int i = 0; i < 5; i++) {
            // test successful
            CountResponse countResponse = client().prepareCount("test").setQuery(termQuery("_type", "type1")).execute().actionGet();
            assertNoFailures(countResponse);
            assertThat(countResponse.getCount(), equalTo(2l));
            assertThat(countResponse.getSuccessfulShards(), equalTo(numShards.numPrimaries));
            assertThat(countResponse.getFailedShards(), equalTo(0));

            // test failed (simply query that can't be parsed)
            countResponse = client().count(countRequest("test").source("{ term : { _type : \"type1 } }".getBytes(Charsets.UTF_8))).actionGet();

            assertThat(countResponse.getCount(), equalTo(0l));
            assertThat(countResponse.getSuccessfulShards(), equalTo(0));
            assertThat(countResponse.getFailedShards(), equalTo(numShards.numPrimaries));

            // count with no query is a match all one
            countResponse = client().prepareCount("test").execute().actionGet();
            assertThat("Failures " + countResponse.getShardFailures(), countResponse.getShardFailures() == null ? 0 : countResponse.getShardFailures().length, equalTo(0));
            assertThat(countResponse.getCount(), equalTo(2l));
            assertThat(countResponse.getSuccessfulShards(), equalTo(numShards.numPrimaries));
            assertThat(countResponse.getFailedShards(), equalTo(0));
        }

        logger.info("Delete by query");
        DeleteByQueryResponse queryResponse = client().prepareDeleteByQuery().setIndices("test").setQuery(termQuery("name", "test2")).execute().actionGet();
        assertThat(queryResponse.getIndex(getConcreteIndexName()).getShardInfo().getTotal(), greaterThanOrEqualTo(numShards.totalNumShards));
        assertThat(queryResponse.getIndex(getConcreteIndexName()).getShardInfo().getSuccessful(), greaterThanOrEqualTo(numShards.totalNumShards));
        assertThat(queryResponse.getIndex(getConcreteIndexName()).getShardInfo().getFailures().length, equalTo(0));
        client().admin().indices().refresh(refreshRequest("test")).actionGet();

        logger.info("Get [type1/1] and [type1/2], should be empty");
        for (int i = 0; i < 5; i++) {
            getResult = client().get(getRequest("test").type("type1").id("1")).actionGet();
            assertThat(getResult.getIndex(), equalTo(getConcreteIndexName()));
            assertThat("cycle #" + i, getResult.getSourceAsString(), equalTo(source("1", "test").string()));
            getResult = client().get(getRequest("test").type("type1").id("2")).actionGet();
            assertThat("cycle #" + i, getResult.isExists(), equalTo(false));
            assertThat(getResult.getIndex(), equalTo(getConcreteIndexName()));
        }
    }

    @Test
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
                .add(client().prepareIndex().setIndex("test").setType("type1").setSource("{ xxx }")) // failure
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

        waitForRelocation(ClusterHealthStatus.GREEN);
        RefreshResponse refreshResponse = client().admin().indices().prepareRefresh("test").execute().actionGet();
        assertNoFailures(refreshResponse);
        assertThat(refreshResponse.getSuccessfulShards(), equalTo(numShards.totalNumShards));


        for (int i = 0; i < 5; i++) {
            GetResponse getResult = client().get(getRequest("test").type("type1").id("1")).actionGet();
            assertThat(getResult.getIndex(), equalTo(getConcreteIndexName()));
            assertThat("cycle #" + i, getResult.isExists(), equalTo(false));

            getResult = client().get(getRequest("test").type("type1").id("2")).actionGet();
            assertThat("cycle #" + i, getResult.getSourceAsString(), equalTo(source("2", "test").string()));
            assertThat(getResult.getIndex(), equalTo(getConcreteIndexName()));

            getResult = client().get(getRequest("test").type("type1").id(generatedId3)).actionGet();
            assertThat("cycle #" + i, getResult.getSourceAsString(), equalTo(source("3", "test").string()));
            assertThat(getResult.getIndex(), equalTo(getConcreteIndexName()));
        }
    }

    private XContentBuilder source(String id, String nameValue) throws IOException {
        return XContentFactory.jsonBuilder().startObject().field("id", id).field("name", nameValue).endObject();
    }
}
