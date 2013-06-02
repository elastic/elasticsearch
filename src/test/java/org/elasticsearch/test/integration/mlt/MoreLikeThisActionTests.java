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

import static org.elasticsearch.client.Requests.indexAliasesRequest;
import static org.elasticsearch.client.Requests.indexRequest;
import static org.elasticsearch.client.Requests.moreLikeThisRequest;
import static org.elasticsearch.client.Requests.refreshRequest;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.FilterBuilders.termFilter;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

import org.elasticsearch.action.admin.cluster.health.ClusterHealthStatus;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.test.integration.AbstractSharedClusterTest;
import org.testng.annotations.Test;

/**
 *
 */
public class MoreLikeThisActionTests extends AbstractSharedClusterTest {

    @Test
    public void testSimpleMoreLikeThis() throws Exception {
        logger.info("Creating index test");
        createIndexMapped("test", "type1", "text", "string");
        logger.info("Running Cluster Health");
        assertThat(ensureGreen(), equalTo(ClusterHealthStatus.GREEN));

        logger.info("Indexing...");
        client().index(indexRequest("test").type("type1").id("1").source(jsonBuilder().startObject().field("text", "lucene").endObject())).actionGet();
        client().index(indexRequest("test").type("type1").id("2").source(jsonBuilder().startObject().field("text", "lucene release").endObject())).actionGet();
        client().admin().indices().refresh(refreshRequest()).actionGet();

        logger.info("Running moreLikeThis");
        SearchResponse mltResponse = client().moreLikeThis(moreLikeThisRequest("test").type("type1").id("1").minTermFreq(1).minDocFreq(1)).actionGet();
        assertThat(mltResponse.getSuccessfulShards(), equalTo(5));
        assertThat(mltResponse.getFailedShards(), equalTo(0));
        assertThat(mltResponse.getHits().totalHits(), equalTo(1l));
    }


    @Test
    public void testMoreLikeThisWithAliases() throws Exception {
        logger.info("Creating index test");
        createIndexMapped("test", "type1", "text", "string");
        logger.info("Creating aliases alias release");
        client().admin().indices().aliases(indexAliasesRequest().addAlias("test", "release", termFilter("text", "release"))).actionGet();
        client().admin().indices().aliases(indexAliasesRequest().addAlias("test", "beta", termFilter("text", "beta"))).actionGet();

        logger.info("Running Cluster Health");
        assertThat(ensureGreen(), equalTo(ClusterHealthStatus.GREEN));

        logger.info("Indexing...");
        client().index(indexRequest("test").type("type1").id("1").source(jsonBuilder().startObject().field("text", "lucene beta").endObject())).actionGet();
        client().index(indexRequest("test").type("type1").id("2").source(jsonBuilder().startObject().field("text", "lucene release").endObject())).actionGet();
        client().index(indexRequest("test").type("type1").id("3").source(jsonBuilder().startObject().field("text", "elasticsearch beta").endObject())).actionGet();
        client().index(indexRequest("test").type("type1").id("4").source(jsonBuilder().startObject().field("text", "elasticsearch release").endObject())).actionGet();
        client().admin().indices().refresh(refreshRequest()).actionGet();

        logger.info("Running moreLikeThis on index");
        SearchResponse mltResponse = client().moreLikeThis(moreLikeThisRequest("test").type("type1").id("1").minTermFreq(1).minDocFreq(1)).actionGet();
        assertThat(mltResponse.getHits().totalHits(), equalTo(2l));

        logger.info("Running moreLikeThis on beta shard");
        mltResponse = client().moreLikeThis(moreLikeThisRequest("beta").type("type1").id("1").minTermFreq(1).minDocFreq(1)).actionGet();
        assertThat(mltResponse.getHits().totalHits(), equalTo(1l));
        assertThat(mltResponse.getHits().getAt(0).id(), equalTo("3"));

        logger.info("Running moreLikeThis on release shard");
        mltResponse = client().moreLikeThis(moreLikeThisRequest("test").type("type1").id("1").minTermFreq(1).minDocFreq(1).searchIndices("release")).actionGet();
        assertThat(mltResponse.getHits().totalHits(), equalTo(1l));
        assertThat(mltResponse.getHits().getAt(0).id(), equalTo("2"));
    }

    @Test
    public void testMoreLikeThisIssue2197() throws Exception {
        Client client = cluster().nodeClient();
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("bar")
                .startObject("properties")
                .endObject()
                .endObject().endObject().string();
        client().admin().indices().prepareCreate("foo").addMapping("bar", mapping).execute().actionGet();
        client().prepareIndex("foo", "bar", "1")
                .setSource(jsonBuilder().startObject().startObject("foo").field("bar", "boz").endObject())
                .execute().actionGet();
        client().admin().indices().prepareRefresh("foo").execute().actionGet();
        assertThat(ensureGreen(), equalTo(ClusterHealthStatus.GREEN));

        SearchResponse searchResponse = client().prepareMoreLikeThis("foo", "bar", "1").execute().actionGet();
        assertThat(searchResponse, notNullValue());
        searchResponse = client.prepareMoreLikeThis("foo", "bar", "1").execute().actionGet();
        assertThat(searchResponse, notNullValue());
    }

    @Test
    // See: https://github.com/elasticsearch/elasticsearch/issues/2489
    public void testMoreLikeWithCustomRouting() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("bar")
                .startObject("properties")
                .endObject()
                .endObject().endObject().string();
        client().admin().indices().prepareCreate("foo").addMapping("bar", mapping).execute().actionGet();
        client().prepareIndex("foo", "bar", "1")
                .setSource(jsonBuilder().startObject().startObject("foo").field("bar", "boz").endObject())
                .setRouting("2")
                .execute().actionGet();
        client().admin().indices().prepareRefresh("foo").execute().actionGet();

        SearchResponse searchResponse = client().prepareMoreLikeThis("foo", "bar", "1").setRouting("2").execute().actionGet();
        assertThat(searchResponse, notNullValue());
    }

    @Test
    // See issue: https://github.com/elasticsearch/elasticsearch/issues/3039
    public void testMoreLikeThisIssueRoutingNotSerialized() throws Exception {
        cluster().ensureAtLeastNumNodes(3);
        prepareCreate("foo", 2, ImmutableSettings.builder().put("index.number_of_replicas", 0)
                .put("index.number_of_shards", 2))
                .execute().actionGet();
        client().admin().cluster().prepareHealth("foo").setWaitForGreenStatus().execute().actionGet();

        client().prepareIndex("foo", "bar", "1")
                .setSource(jsonBuilder().startObject().startObject("foo").field("bar", "boz").endObject())
                .setRouting("4000")
                .execute().actionGet();
        client().admin().indices().prepareRefresh("foo").execute().actionGet();
        SearchResponse searchResponse = client().prepareMoreLikeThis("foo", "bar", "1").setRouting("4000").execute().actionGet();
        assertThat(searchResponse, notNullValue());
    }

}
