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

package org.elasticsearch.test.integration.mlt;

import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthStatus;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.indices.IndexMissingException;
import org.elasticsearch.test.integration.AbstractNodesTests;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.elasticsearch.client.Requests.*;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.FilterBuilders.termFilter;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

/**
 *
 */
public class MoreLikeThisActionTests extends AbstractNodesTests {

    private Client client1;
    private Client client2;

    @BeforeClass
    public void startServers() {
        startNode("server1");
        startNode("server2");
        client1 = getClient1();
        client2 = getClient2();
    }

    @AfterClass
    public void closeServers() {
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
    public void testSimpleMoreLikeThis() throws Exception {
        try {
            client1.admin().indices().prepareDelete("test").execute().actionGet();
        } catch (IndexMissingException e) {
        }
        logger.info("Creating index test");
        client1.admin().indices().create(createIndexRequest("test")).actionGet();

        logger.info("Running Cluster Health");
        ClusterHealthResponse clusterHealth = client1.admin().cluster().health(clusterHealthRequest().waitForGreenStatus()).actionGet();
        logger.info("Done Cluster Health, status " + clusterHealth.getStatus());
        assertThat(clusterHealth.isTimedOut(), equalTo(false));
        assertThat(clusterHealth.getStatus(), equalTo(ClusterHealthStatus.GREEN));

        logger.info("Indexing...");
        client1.index(indexRequest("test").type("type1").id("1").source(jsonBuilder().startObject().field("text", "lucene").endObject())).actionGet();
        client1.index(indexRequest("test").type("type1").id("2").source(jsonBuilder().startObject().field("text", "lucene release").endObject())).actionGet();
        client1.admin().indices().refresh(refreshRequest()).actionGet();

        logger.info("Running moreLikeThis");
        SearchResponse mltResponse = client1.moreLikeThis(moreLikeThisRequest("test").type("type1").id("1").minTermFreq(1).minDocFreq(1)).actionGet();
        assertThat(mltResponse.getSuccessfulShards(), equalTo(5));
        assertThat(mltResponse.getFailedShards(), equalTo(0));
        assertThat(mltResponse.getHits().totalHits(), equalTo(1l));
    }


    @Test
    public void testMoreLikeThisWithAliases() throws Exception {
        try {
            client1.admin().indices().prepareDelete("test").execute().actionGet();
        } catch (IndexMissingException e) {
        }
        logger.info("Creating index test");
        client1.admin().indices().create(createIndexRequest("test")).actionGet();

        logger.info("Creating aliases alias release");
        client1.admin().indices().aliases(indexAliasesRequest().addAlias("test", "release", termFilter("text", "release"))).actionGet();
        client1.admin().indices().aliases(indexAliasesRequest().addAlias("test", "beta", termFilter("text", "beta"))).actionGet();

        logger.info("Running Cluster Health");
        ClusterHealthResponse clusterHealth = client1.admin().cluster().health(clusterHealthRequest().waitForGreenStatus()).actionGet();
        logger.info("Done Cluster Health, status " + clusterHealth.getStatus());
        assertThat(clusterHealth.isTimedOut(), equalTo(false));
        assertThat(clusterHealth.getStatus(), equalTo(ClusterHealthStatus.GREEN));

        logger.info("Indexing...");
        client1.index(indexRequest("test").type("type1").id("1").source(jsonBuilder().startObject().field("text", "lucene beta").endObject())).actionGet();
        client1.index(indexRequest("test").type("type1").id("2").source(jsonBuilder().startObject().field("text", "lucene release").endObject())).actionGet();
        client1.index(indexRequest("test").type("type1").id("3").source(jsonBuilder().startObject().field("text", "elasticsearch beta").endObject())).actionGet();
        client1.index(indexRequest("test").type("type1").id("4").source(jsonBuilder().startObject().field("text", "elasticsearch release").endObject())).actionGet();
        client1.admin().indices().refresh(refreshRequest()).actionGet();

        logger.info("Running moreLikeThis on index");
        SearchResponse mltResponse = client1.moreLikeThis(moreLikeThisRequest("test").type("type1").id("1").minTermFreq(1).minDocFreq(1)).actionGet();
        assertThat(mltResponse.getHits().totalHits(), equalTo(2l));

        logger.info("Running moreLikeThis on beta shard");
        mltResponse = client1.moreLikeThis(moreLikeThisRequest("beta").type("type1").id("1").minTermFreq(1).minDocFreq(1)).actionGet();
        assertThat(mltResponse.getHits().totalHits(), equalTo(1l));
        assertThat(mltResponse.getHits().getAt(0).id(), equalTo("3"));

        logger.info("Running moreLikeThis on release shard");
        mltResponse = client1.moreLikeThis(moreLikeThisRequest("test").type("type1").id("1").minTermFreq(1).minDocFreq(1).searchIndices("release")).actionGet();
        assertThat(mltResponse.getHits().totalHits(), equalTo(1l));
        assertThat(mltResponse.getHits().getAt(0).id(), equalTo("2"));
    }

    @Test
    public void testMoreLikeThisIssue2197() throws Exception {
        startNode("client-node", ImmutableSettings.settingsBuilder().put("node.client", true));
        try {
            client1.admin().indices().prepareDelete("foo").execute().actionGet();
        } catch (IndexMissingException e) {
        }
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("bar")
                .startObject("properties")
                .endObject()
                .endObject().endObject().string();
        client1.admin().indices().prepareCreate("foo").addMapping("bar", mapping).execute().actionGet();
        client1.prepareIndex("foo", "bar", "1")
                .setSource(jsonBuilder().startObject().startObject("foo").field("bar", "boz").endObject())
                .execute().actionGet();
        client1.admin().indices().prepareRefresh("foo").execute().actionGet();

        SearchResponse searchResponse = client1.prepareMoreLikeThis("foo", "bar", "1").execute().actionGet();
        assertThat(searchResponse, notNullValue());
        Client client3 = client("client-node");
        searchResponse = client3.prepareMoreLikeThis("foo", "bar", "1").execute().actionGet();
        assertThat(searchResponse, notNullValue());
        client3.close();
    }

    @Test
    // See: https://github.com/elasticsearch/elasticsearch/issues/2489
    public void testMoreLikeWithCustomRouting() throws Exception {
        try {
            client1.admin().indices().prepareDelete("foo").execute().actionGet();
        } catch (IndexMissingException e) {
        }
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("bar")
                .startObject("properties")
                .endObject()
                .endObject().endObject().string();
        client1.admin().indices().prepareCreate("foo").addMapping("bar", mapping).execute().actionGet();
        client1.prepareIndex("foo", "bar", "1")
                .setSource(jsonBuilder().startObject().startObject("foo").field("bar", "boz").endObject())
                .setRouting("2")
                .execute().actionGet();
        client1.admin().indices().prepareRefresh("foo").execute().actionGet();

        SearchResponse searchResponse = client1.prepareMoreLikeThis("foo", "bar", "1").setRouting("2").execute().actionGet();
        assertThat(searchResponse, notNullValue());
    }

}
