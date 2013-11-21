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

package org.elasticsearch.mlt;

import org.elasticsearch.action.admin.cluster.health.ClusterHealthStatus;
import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.junit.Test;

import static org.elasticsearch.client.Requests.*;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.FilterBuilders.termFilter;
import static org.elasticsearch.index.query.QueryBuilders.moreLikeThisFieldQuery;
import static org.elasticsearch.index.query.QueryBuilders.moreLikeThisQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.*;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

/**
 *
 */
public class MoreLikeThisActionTests extends ElasticsearchIntegrationTest {

    @Test
    public void testSimpleMoreLikeThis() throws Exception {
        logger.info("Creating index test");
        assertAcked(prepareCreate("test").addMapping("type1", 
                jsonBuilder().startObject().startObject("type1").startObject("properties")
                    .startObject("text").field("type", "string").endObject()
                    .endObject().endObject().endObject()));
        
        logger.info("Running Cluster Health");
        assertThat(ensureGreen(), equalTo(ClusterHealthStatus.GREEN));

        logger.info("Indexing...");
        client().index(indexRequest("test").type("type1").id("1").source(jsonBuilder().startObject().field("text", "lucene").endObject())).actionGet();
        client().index(indexRequest("test").type("type1").id("2").source(jsonBuilder().startObject().field("text", "lucene release").endObject())).actionGet();
        client().admin().indices().refresh(refreshRequest()).actionGet();

        logger.info("Running moreLikeThis");
        SearchResponse mltResponse = client().moreLikeThis(moreLikeThisRequest("test").type("type1").id("1").minTermFreq(1).minDocFreq(1)).actionGet();
        assertHitCount(mltResponse, 1l);
    }
    
    
    @Test
    public void testSimpleMoreLikeOnLongField() throws Exception {
        logger.info("Creating index test");
        createIndex("test");
        logger.info("Running Cluster Health");
        assertThat(ensureGreen(), equalTo(ClusterHealthStatus.GREEN));

        logger.info("Indexing...");
        client().index(indexRequest("test").type("type1").id("1").source(jsonBuilder().startObject().field("some_long", 1367484649580l).endObject())).actionGet();
        client().index(indexRequest("test").type("type2").id("2").source(jsonBuilder().startObject().field("some_long", 0).endObject())).actionGet();
        client().index(indexRequest("test").type("type1").id("3").source(jsonBuilder().startObject().field("some_long", -666).endObject())).actionGet();


        client().admin().indices().refresh(refreshRequest()).actionGet();

        logger.info("Running moreLikeThis");
        SearchResponse mltResponse = client().moreLikeThis(moreLikeThisRequest("test").type("type1").id("1").minTermFreq(1).minDocFreq(1)).actionGet();
        assertHitCount(mltResponse, 0l);
    }


    @Test
    public void testMoreLikeThisWithAliases() throws Exception {
        logger.info("Creating index test");
        assertAcked(prepareCreate("test").addMapping("type1", 
                jsonBuilder().startObject().startObject("type1").startObject("properties")
                    .startObject("text").field("type", "string").endObject()
                    .endObject().endObject().endObject()));
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
        assertHitCount(mltResponse, 2l);

        logger.info("Running moreLikeThis on beta shard");
        mltResponse = client().moreLikeThis(moreLikeThisRequest("beta").type("type1").id("1").minTermFreq(1).minDocFreq(1)).actionGet();
        assertHitCount(mltResponse, 1l);
        assertThat(mltResponse.getHits().getAt(0).id(), equalTo("3"));

        logger.info("Running moreLikeThis on release shard");
        mltResponse = client().moreLikeThis(moreLikeThisRequest("test").type("type1").id("1").minTermFreq(1).minDocFreq(1).searchIndices("release")).actionGet();
        assertHitCount(mltResponse, 1l);
        assertThat(mltResponse.getHits().getAt(0).id(), equalTo("2"));

        logger.info("Running moreLikeThis on alias with node client");
        mltResponse = cluster().clientNodeClient().moreLikeThis(moreLikeThisRequest("beta").type("type1").id("1").minTermFreq(1).minDocFreq(1)).actionGet();
        assertHitCount(mltResponse, 1l);
        assertThat(mltResponse.getHits().getAt(0).id(), equalTo("3"));

    }

    @Test
    public void testMoreLikeThisIssue2197() throws Exception {
        Client client = client();
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
        assertNoFailures(searchResponse);
        assertThat(searchResponse, notNullValue());
        searchResponse = client.prepareMoreLikeThis("foo", "bar", "1").execute().actionGet();
        assertNoFailures(searchResponse);
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
        ensureGreen();

        client().prepareIndex("foo", "bar", "1")
                .setSource(jsonBuilder().startObject().startObject("foo").field("bar", "boz").endObject())
                .setRouting("2")
                .execute().actionGet();
        client().admin().indices().prepareRefresh("foo").execute().actionGet();

        SearchResponse searchResponse = client().prepareMoreLikeThis("foo", "bar", "1").setRouting("2").execute().actionGet();
        assertNoFailures(searchResponse);
        assertThat(searchResponse, notNullValue());
    }

    @Test
    // See issue: https://github.com/elasticsearch/elasticsearch/issues/3039
    public void testMoreLikeThisIssueRoutingNotSerialized() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("bar")
                .startObject("properties")
                .endObject()
                .endObject().endObject().string();
        prepareCreate("foo", 2, ImmutableSettings.builder().put("index.number_of_replicas", 0)
                .put("index.number_of_shards", 2))
                .addMapping("bar", mapping)
                .execute().actionGet();
        ensureGreen();

        client().prepareIndex("foo", "bar", "1")
                .setSource(jsonBuilder().startObject().startObject("foo").field("bar", "boz").endObject())
                .setRouting("4000")
                .execute().actionGet();
        client().admin().indices().prepareRefresh("foo").execute().actionGet();
        SearchResponse searchResponse = client().prepareMoreLikeThis("foo", "bar", "1").setRouting("4000").execute().actionGet();
        assertNoFailures(searchResponse);
        assertThat(searchResponse, notNullValue());
    }

    @Test
    // See issue https://github.com/elasticsearch/elasticsearch/issues/3252
    public void testNumericField() throws Exception {
        prepareCreate("test").addMapping("type", jsonBuilder()
                    .startObject().startObject("type")
                    .startObject("properties")
                        .startObject("int_value").field("type", randomNumericType(getRandom())).endObject()
                        .startObject("string_value").field("type", "string").endObject()
                        .endObject()
                    .endObject().endObject()).execute().actionGet();
        ensureGreen();
        client().prepareIndex("test", "type", "1")
                .setSource(jsonBuilder().startObject().field("string_value", "lucene index").field("int_value", 1).endObject())
                .execute().actionGet();
        client().prepareIndex("test", "type", "2")
                .setSource(jsonBuilder().startObject().field("string_value", "elasticsearch index").field("int_value", 42).endObject())
                .execute().actionGet();

        refresh();

        // Implicit list of fields -> ignore numeric fields
        SearchResponse searchResponse = client().prepareMoreLikeThis("test", "type", "1").setMinDocFreq(1).setMinTermFreq(1).execute().actionGet();
        assertHitCount(searchResponse, 1l);

        // Explicit list of fields including numeric fields -> fail
        assertThrows(client().prepareMoreLikeThis("test", "type", "1").setField("string_value", "int_value"), SearchPhaseExecutionException.class);

        // mlt query with no field -> OK
        searchResponse = client().prepareSearch().setQuery(moreLikeThisQuery().likeText("index").minTermFreq(1).minDocFreq(1)).execute().actionGet();
        assertHitCount(searchResponse, 2l);

        // mlt query with string fields
        searchResponse = client().prepareSearch().setQuery(moreLikeThisQuery("string_value").likeText("index").minTermFreq(1).minDocFreq(1)).execute().actionGet();
        assertHitCount(searchResponse, 2l);

        // mlt query with at least a numeric field -> fail by default
        assertThrows(client().prepareSearch().setQuery(moreLikeThisQuery("string_value", "int_value").likeText("index")), SearchPhaseExecutionException.class);

        // mlt query with at least a numeric field -> fail by command
        assertThrows(client().prepareSearch().setQuery(moreLikeThisQuery("string_value", "int_value").likeText("index").failOnUnsupportedField(true)), SearchPhaseExecutionException.class);


        // mlt query with at least a numeric field but fail_on_unsupported_field set to false
        searchResponse = client().prepareSearch().setQuery(moreLikeThisQuery("string_value", "int_value").likeText("index").minTermFreq(1).minDocFreq(1).failOnUnsupportedField(false)).get();
        assertHitCount(searchResponse, 2l);

        // mlt field query on a numeric field -> failure by default
        assertThrows(client().prepareSearch().setQuery(moreLikeThisFieldQuery("int_value").likeText("42").minTermFreq(1).minDocFreq(1)), SearchPhaseExecutionException.class);

        // mlt field query on a numeric field -> failure by command
        assertThrows(client().prepareSearch().setQuery(moreLikeThisFieldQuery("int_value").likeText("42").minTermFreq(1).minDocFreq(1).failOnUnsupportedField(true)),
                SearchPhaseExecutionException.class);

        // mlt field query on a numeric field but fail_on_unsupported_field set to false
        searchResponse = client().prepareSearch().setQuery(moreLikeThisFieldQuery("int_value").likeText("42").minTermFreq(1).minDocFreq(1).failOnUnsupportedField(false)).execute().actionGet();
        assertHitCount(searchResponse, 0l);
    }

}
