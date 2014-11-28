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

package org.elasticsearch.mlt;

import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.LuceneTestCase;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthStatus;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.mlt.MoreLikeThisRequest;
import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.MoreLikeThisQueryBuilder;
import org.elasticsearch.index.query.MoreLikeThisQueryBuilder.Item;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static org.elasticsearch.client.Requests.*;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_REPLICAS;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_SHARDS;
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
        assertAcked(prepareCreate("test").addMapping("type1", "some_long", "type=long"));
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
        client().admin().indices().aliases(indexAliasesRequest().addAlias("release", termFilter("text", "release"), "test")).actionGet();
        client().admin().indices().aliases(indexAliasesRequest().addAlias("beta", termFilter("text", "beta"), "test")).actionGet();

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
        mltResponse = internalCluster().clientNodeClient().moreLikeThis(moreLikeThisRequest("beta").type("type1").id("1").minTermFreq(1).minDocFreq(1)).actionGet();
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
        assertAcked(prepareCreate("foo", 2,
                ImmutableSettings.builder().put(SETTING_NUMBER_OF_SHARDS, 2).put(SETTING_NUMBER_OF_REPLICAS, 0))
                .addMapping("bar", mapping));
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

    @Test
    public void testSimpleMoreLikeInclude() throws Exception {
        logger.info("Creating index test");
        assertAcked(prepareCreate("test").addMapping("type1",
                jsonBuilder().startObject().startObject("type1").startObject("properties")
                        .startObject("text").field("type", "string").endObject()
                        .endObject().endObject().endObject()));

        logger.info("Running Cluster Health");
        assertThat(ensureGreen(), equalTo(ClusterHealthStatus.GREEN));

        logger.info("Indexing...");
        client().index(indexRequest("test").type("type1").id("1").source(
                jsonBuilder().startObject()
                        .field("text", "Apache Lucene is a free/open source information retrieval software library").endObject()))
                .actionGet();
        client().index(indexRequest("test").type("type1").id("2").source(
                jsonBuilder().startObject()
                        .field("text", "Lucene has been ported to other programming languages").endObject()))
                .actionGet();
        client().admin().indices().refresh(refreshRequest()).actionGet();

        logger.info("Running More Like This with include true");
        SearchResponse mltResponse = client().moreLikeThis(
                moreLikeThisRequest("test").type("type1").id("1").minTermFreq(1).minDocFreq(1).include(true).percentTermsToMatch(0))
                .actionGet();
        assertOrderedSearchHits(mltResponse, "1", "2");

        mltResponse = client().moreLikeThis(
                moreLikeThisRequest("test").type("type1").id("2").minTermFreq(1).minDocFreq(1).include(true).percentTermsToMatch(0))
                .actionGet();
        assertOrderedSearchHits(mltResponse, "2", "1");

        logger.info("Running More Like This with include false");
        mltResponse = client().moreLikeThis(
                moreLikeThisRequest("test").type("type1").id("1").minTermFreq(1).minDocFreq(1).percentTermsToMatch(0))
                .actionGet();
        assertSearchHits(mltResponse, "2");
    }

    @Test
    public void testMoreLikeThisBodyFromSize() throws Exception {
        logger.info("Creating index test");
        assertAcked(prepareCreate("test").addMapping("type1",
                jsonBuilder().startObject().startObject("type1").startObject("properties")
                        .startObject("text").field("type", "string").endObject()
                        .endObject().endObject().endObject()));

        logger.info("Running Cluster Health");
        assertThat(ensureGreen(), equalTo(ClusterHealthStatus.GREEN));

        logger.info("Indexing...");
        List<IndexRequestBuilder> builders = new ArrayList<>(10);
        for (int i = 1; i <= 10; i++) {
            builders.add(client().prepareIndex("test", "type1").setSource("text", "lucene").setId(String.valueOf(i)));
        }
        indexRandom(true, builders);

        logger.info("'size' set but 'search_from' and 'search_size' kept to defaults");
        SearchResponse mltResponse = client().moreLikeThis(
                moreLikeThisRequest("test").type("type1").id("1").minTermFreq(1).minDocFreq(1).include(true)
                        .searchSource(SearchSourceBuilder.searchSource().size(5)))
                .actionGet();
        assertSearchResponse(mltResponse);
        assertEquals(mltResponse.getHits().hits().length, 5);

        logger.info("'from' set but 'search_from' and 'search_size' kept to defaults");
        mltResponse = client().moreLikeThis(
                moreLikeThisRequest("test").type("type1").id("1").minTermFreq(1).minDocFreq(1).include(true)
                        .searchSource(SearchSourceBuilder.searchSource().from(5)))
                .actionGet();
        assertSearchResponse(mltResponse);
        assertEquals(mltResponse.getHits().hits().length, 5);

        logger.info("When set, 'search_from' and 'search_size' should override 'from' and 'size'");
        mltResponse = client().moreLikeThis(
                moreLikeThisRequest("test").type("type1").id("1").minTermFreq(1).minDocFreq(1).include(true)
                        .searchSize(10).searchFrom(2)
                        .searchSource(SearchSourceBuilder.searchSource().size(1).from(1)))
                .actionGet();
        assertSearchResponse(mltResponse);
        assertEquals(mltResponse.getHits().hits().length, 8);
    }

    public void testSimpleMoreLikeThisIds() throws Exception {
        logger.info("Creating index test");
        assertAcked(prepareCreate("test").addMapping("type1",
                jsonBuilder().startObject().startObject("type1").startObject("properties")
                        .startObject("text").field("type", "string").endObject()
                        .endObject().endObject().endObject()));

        logger.info("Running Cluster Health");
        assertThat(ensureGreen(), equalTo(ClusterHealthStatus.GREEN));

        logger.info("Indexing...");
        List<IndexRequestBuilder> builders = new ArrayList<>();
        builders.add(client().prepareIndex("test", "type1").setSource("text", "lucene").setId("1"));
        builders.add(client().prepareIndex("test", "type1").setSource("text", "lucene release").setId("2"));
        builders.add(client().prepareIndex("test", "type1").setSource("text", "apache lucene").setId("3"));
        indexRandom(true, builders);

        logger.info("Running MoreLikeThis");
        MoreLikeThisQueryBuilder queryBuilder = QueryBuilders.moreLikeThisQuery("text").ids("1").include(true).minTermFreq(1).minDocFreq(1);
        SearchResponse mltResponse = client().prepareSearch().setTypes("type1").setQuery(queryBuilder).execute().actionGet();
        assertHitCount(mltResponse, 3l);
    }

    @Test
    public void testCompareMoreLikeThisDSLWithAPI() throws Exception {
        logger.info("Creating index test");
        assertAcked(prepareCreate("test").addMapping("type1",
                jsonBuilder().startObject().startObject("type1").startObject("properties")
                        .startObject("text").field("type", "string").endObject()
                        .endObject().endObject().endObject()));

        logger.info("Running Cluster Health");
        assertThat(ensureGreen(), equalTo(ClusterHealthStatus.GREEN));

        logger.info("Indexing...");
        String[] texts = new String[] {
            "Apache Lucene",
            "free and open source",
            "information retrieval",
            "software library",
            "programmed in Java",
            "Doug Cutting",
            "Apache Software Foundation",
            "Apache Software License",
            "Lucene programming languages",
            "Delphi, Perl, C#, C++, Python, Ruby, and PHP"
        };
        List<IndexRequestBuilder> builders = new ArrayList<>(10);
        for (int i = 0; i < texts.length; i++) {
            builders.add(client().prepareIndex("test", "type1").setSource("text", texts[i]).setId(String.valueOf(i)));
        }
        indexRandom(true, false, builders);

        int iters = between(10, 20);
        for (int j = 0; j < iters; j++) {
            logger.info("Running MoreLikeThis DSL with IDs");
            String id = String.valueOf(getRandom().nextInt(texts.length));
            Client client = client();
            MoreLikeThisQueryBuilder queryBuilder = QueryBuilders.moreLikeThisQuery("text").ids(id).minTermFreq(1).minDocFreq(1)
                    .minimumShouldMatch("0%");
            SearchResponse mltResponseDSL = client.prepareSearch()
                    .setSearchType(SearchType.QUERY_THEN_FETCH)
                    .setTypes("type1")
                    .setQuery(queryBuilder)
                    .setSize(texts.length)
                    .execute().actionGet();
            assertSearchResponse(mltResponseDSL);

            logger.info("Running MoreLikeThis API");
            MoreLikeThisRequest mltRequest = moreLikeThisRequest("test").type("type1").searchSize(texts.length).id(id).minTermFreq(1).minDocFreq(1)
                    .minimumShouldMatch("0%");
            SearchResponse mltResponseAPI = client.moreLikeThis(mltRequest).actionGet();
            assertSearchResponse(mltResponseAPI);

            logger.info("Ensure the documents and scores returned are the same.");
            SearchHit[] hitsDSL = mltResponseDSL.getHits().hits();
            SearchHit[] hitsAPI = mltResponseAPI.getHits().hits();

            // we have to resort since the results might come from
            // different shards and docIDs that are used for tie-breaking might not be the same on the shards
            Comparator<SearchHit> cmp = new Comparator<SearchHit>() {

                @Override
                public int compare(SearchHit o1, SearchHit o2) {
                    if (Float.compare(o1.getScore(), o2.getScore()) == 0) {
                        return o1.getId().compareTo(o2.getId());
                    }
                    return Float.compare(o1.getScore(), o2.getScore());
                }
            };
            ArrayUtil.timSort(hitsDSL, cmp);
            ArrayUtil.timSort(hitsAPI, cmp);
            assertThat("Not the same number of results.", hitsAPI.length, equalTo(hitsDSL.length));
            for (int i = 0; i < hitsDSL.length; i++) {
                assertThat("Expected id: " + hitsDSL[i].getId() + " at position " + i + " but wasn't.",
                        hitsAPI[i].getId(), equalTo(hitsDSL[i].getId()));
                assertThat("Expected score: " + hitsDSL[i].getScore() + " at position " + i + " but wasn't.",
                        hitsAPI[i].getScore(), equalTo(hitsDSL[i].getScore()));
            }
        }
    }

    @Test
    public void testSimpleMoreLikeThisIdsMultipleTypes() throws Exception {
        logger.info("Creating index test");
        int numOfTypes = randomIntBetween(2, 10);
        CreateIndexRequestBuilder createRequestBuilder = prepareCreate("test");
        for (int i = 0; i < numOfTypes; i++) {
            createRequestBuilder.addMapping("type" + i, jsonBuilder().startObject().startObject("type" + i).startObject("properties")
                    .startObject("text").field("type", "string").endObject()
                    .endObject().endObject().endObject());
        }
        assertAcked(createRequestBuilder);

        logger.info("Running Cluster Health");
        assertThat(ensureGreen(), equalTo(ClusterHealthStatus.GREEN));

        logger.info("Indexing...");
        List<IndexRequestBuilder> builders = new ArrayList<>(numOfTypes);
        for (int i = 0; i < numOfTypes; i++) {
            builders.add(client().prepareIndex("test", "type" + i).setSource("text", "lucene" + " " + i).setId(String.valueOf(i)));
        }
        indexRandom(true, builders);

        logger.info("Running MoreLikeThis");
        MoreLikeThisQueryBuilder queryBuilder = QueryBuilders.moreLikeThisQuery("text").include(true).minTermFreq(1).minDocFreq(1)
                .addItem(new MoreLikeThisQueryBuilder.Item("test", "type0", "0"));

        String[] types = new String[numOfTypes];
        for (int i = 0; i < numOfTypes; i++) {
            types[i] = "type"+i;
        }
        SearchResponse mltResponse = client().prepareSearch().setTypes(types).setQuery(queryBuilder).execute().actionGet();
        assertHitCount(mltResponse, numOfTypes);
    }

    @Test
    public void testMoreLikeThisMultiValueFields() throws Exception {
        logger.info("Creating the index ...");
        assertAcked(prepareCreate("test")
                .addMapping("type1", "text", "type=string,analyzer=keyword")
                .setSettings(SETTING_NUMBER_OF_SHARDS, 1));
        ensureGreen();

        logger.info("Indexing ...");
        String[] values = {"aaaa", "bbbb", "cccc", "dddd", "eeee", "ffff", "gggg", "hhhh", "iiii", "jjjj"};
        List<IndexRequestBuilder> builders = new ArrayList<>(values.length + 1);
        // index one document with all the values
        builders.add(client().prepareIndex("test", "type1", "0").setSource("text", values));
        // index each document with only one of the values
        for (int i = 0; i < values.length; i++) {
            builders.add(client().prepareIndex("test", "type1", String.valueOf(i + 1)).setSource("text", values[i]));
        }
        indexRandom(true, builders);

        int maxIters = randomIntBetween(10, 20);
        for (int i = 0; i < maxIters; i++)
        {
            int max_query_terms = randomIntBetween(1, values.length);
            logger.info("Running More Like This with max_query_terms = %s", max_query_terms);
            MoreLikeThisQueryBuilder mltQuery = moreLikeThisQuery("text").ids("0").minTermFreq(1).minDocFreq(1)
                    .maxQueryTerms(max_query_terms).percentTermsToMatch(0);
            SearchResponse response = client().prepareSearch("test").setTypes("type1")
                    .setQuery(mltQuery).execute().actionGet();
            assertSearchResponse(response);
            assertHitCount(response, max_query_terms);

            logger.info("Running More Like This API with with max_query_terms = %s returns all docs!", max_query_terms);
            response = client().moreLikeThis(moreLikeThisRequest("test").type("type1")
                    .id("0").fields("text").minTermFreq(1).minDocFreq(1)
                    .maxQueryTerms(max_query_terms).percentTermsToMatch(0))
                    .actionGet();
            assertSearchResponse(response);
            assertHitCount(response, values.length);
        }
    }

    @Test
    public void testMinimumShouldMatch() throws ExecutionException, InterruptedException {
        logger.info("Creating the index ...");
        assertAcked(prepareCreate("test")
                .addMapping("type1", "text", "type=string,analyzer=whitespace")
                .setSettings(SETTING_NUMBER_OF_SHARDS, 1));
        ensureGreen();

        logger.info("Indexing with each doc having one less term ...");
        List<IndexRequestBuilder> builders = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            String text = "";
            for (int j = 1; j <= 10 - i; j++) {
                text += j + " ";
            }
            builders.add(client().prepareIndex("test", "type1", i + "").setSource("text", text));
        }
        indexRandom(true, builders);

        logger.info("Testing each minimum_should_match from 0% - 100% with 10% increment ...");
        for (int i = 0; i <= 10; i++) {
            String minimumShouldMatch = (10 * i) + "%";
            MoreLikeThisQueryBuilder mltQuery = moreLikeThisQuery("text")
                    .likeText("1 2 3 4 5 6 7 8 9 10")
                    .minTermFreq(1)
                    .minDocFreq(1)
                    .minimumShouldMatch(minimumShouldMatch);
            logger.info("Testing with minimum_should_match = " + minimumShouldMatch);
            SearchResponse response = client().prepareSearch("test").setTypes("type1")
                    .setQuery(mltQuery).get();
            assertSearchResponse(response);
            if (minimumShouldMatch.equals("0%")) {
                assertHitCount(response, 10);
            } else {
                assertHitCount(response, 11 - i);
            }
        }
    }

    @Test
    @LuceneTestCase.AwaitsFix(bugUrl = "alex k working on it")
    public void testMoreLikeThisArtificialDocs() throws Exception {
        int numFields = randomIntBetween(5, 10);

        logger.info("Creating an index with multiple fields ...");
        XContentBuilder mapping = jsonBuilder().startObject().startObject("type1").startObject("properties");
        for (int i = 0; i < numFields; i++) {
            mapping.startObject("field"+i).field("type", "string").endObject();
        }
        mapping.endObject().endObject().endObject();
        assertAcked(prepareCreate("test").addMapping("type1", mapping).get());
        ensureGreen();

        logger.info("Indexing a single document ...");
        XContentBuilder doc = jsonBuilder().startObject();
        for (int i = 0; i < numFields; i++) {
            doc.field("field"+i, generateRandomStringArray(5, 10));
        }
        doc.endObject();
        List<IndexRequestBuilder> builders = new ArrayList<>();
        builders.add(client().prepareIndex("test", "type1", "1").setSource(doc));
        indexRandom(true, builders);

        logger.info("Checking the document matches ...");
        MoreLikeThisQueryBuilder mltQuery = moreLikeThisQuery()
                .docs((Item) new Item().doc(doc).index("test").type("type1"))
                .minTermFreq(0)
                .minDocFreq(0)
                .maxQueryTerms(100)
                .percentTermsToMatch(1); // strict all terms must match!
        SearchResponse response = client().prepareSearch("test").setTypes("type1")
                .setQuery(mltQuery).get();
        assertSearchResponse(response);
        assertHitCount(response, 1);
    }

    @Test
    public void testMoreLikeThisMalformedArtificialDocs() throws Exception {
        logger.info("Creating the index ...");
        assertAcked(prepareCreate("test")
                .addMapping("type1", "text", "type=string,analyzer=whitespace", "date", "type=date"));
        ensureGreen("test");

        logger.info("Creating an index with a single document ...");
        indexRandom(true, client().prepareIndex("test", "type1", "1").setSource(jsonBuilder()
                .startObject()
                    .field("text", "Hello World!")
                    .field("date", "2009-01-01")
                .endObject()));

        logger.info("Checking with a malformed field value ...");
        XContentBuilder malformedFieldDoc = jsonBuilder()
                .startObject()
                    .field("text", "Hello World!")
                    .field("date", "this is not a date!")
                .endObject();
        MoreLikeThisQueryBuilder mltQuery = moreLikeThisQuery()
                .docs((Item) new Item().doc(malformedFieldDoc).index("test").type("type1"))
                .minTermFreq(0)
                .minDocFreq(0)
                .percentTermsToMatch(0);
        SearchResponse response = client().prepareSearch("test").setTypes("type1")
                .setQuery(mltQuery).get();
        assertSearchResponse(response);
        assertHitCount(response, 0);

        logger.info("Checking with an empty document ...");
        XContentBuilder emptyDoc = jsonBuilder().startObject().endObject();
        mltQuery = moreLikeThisQuery()
                .docs((Item) new Item().doc(emptyDoc).index("test").type("type1"))
                .minTermFreq(0)
                .minDocFreq(0)
                .percentTermsToMatch(0);
        response = client().prepareSearch("test").setTypes("type1")
                .setQuery(mltQuery).get();
        assertSearchResponse(response);
        assertHitCount(response, 0);

        logger.info("Checking when document is malformed ...");
        XContentBuilder malformedDoc = jsonBuilder().startObject();
        mltQuery = moreLikeThisQuery()
                .docs((Item) new Item().doc(malformedDoc).index("test").type("type1"))
                .minTermFreq(0)
                .minDocFreq(0)
                .percentTermsToMatch(0);
        response = client().prepareSearch("test").setTypes("type1")
                .setQuery(mltQuery).get();
        assertSearchResponse(response);
        assertHitCount(response, 0);

        logger.info("Checking the document matches otherwise ...");
        XContentBuilder normalDoc = jsonBuilder()
                .startObject()
                    .field("text", "Hello World!")
                    .field("date", "1000-01-01") // should be properly parsed but ignored ...
                .endObject();
        mltQuery = moreLikeThisQuery()
                .docs((Item) new Item().doc(normalDoc).index("test").type("type1"))
                .minTermFreq(0)
                .minDocFreq(0)
                .percentTermsToMatch(1);  // strict all terms must match but date is ignored
        response = client().prepareSearch("test").setTypes("type1")
                .setQuery(mltQuery).get();
        assertSearchResponse(response);
        assertHitCount(response, 1);
    }
}
