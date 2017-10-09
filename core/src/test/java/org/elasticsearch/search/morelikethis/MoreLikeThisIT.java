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

package org.elasticsearch.search.morelikethis;

import org.elasticsearch.Version;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.MoreLikeThisQueryBuilder;
import org.elasticsearch.index.query.MoreLikeThisQueryBuilder.Item;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.InternalSettingsPlugin;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static org.elasticsearch.client.Requests.indexRequest;
import static org.elasticsearch.client.Requests.refreshRequest;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_REPLICAS;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_SHARDS;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.MoreLikeThisQueryBuilder.ids;
import static org.elasticsearch.index.query.QueryBuilders.moreLikeThisQuery;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertOrderedSearchHits;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchHits;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchResponse;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertThrows;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

public class MoreLikeThisIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singleton(InternalSettingsPlugin.class);
    }

    public void testSimpleMoreLikeThis() throws Exception {
        logger.info("Creating index test");
        assertAcked(prepareCreate("test").addMapping("type1",
                jsonBuilder().startObject().startObject("type1").startObject("properties")
                        .startObject("text").field("type", "text").endObject()
                        .endObject().endObject().endObject()));

        logger.info("Running Cluster Health");
        assertThat(ensureGreen(), equalTo(ClusterHealthStatus.GREEN));

        logger.info("Indexing...");
        client().index(indexRequest("test").type("type1").id("1").source(jsonBuilder().startObject().field("text", "lucene").endObject())).actionGet();
        client().index(indexRequest("test").type("type1").id("2").source(jsonBuilder().startObject().field("text", "lucene release").endObject())).actionGet();
        client().admin().indices().refresh(refreshRequest()).actionGet();

        logger.info("Running moreLikeThis");
        SearchResponse response = client().prepareSearch().setQuery(
                new MoreLikeThisQueryBuilder(null, new Item[] {new Item("test", "type1", "1")}).minTermFreq(1).minDocFreq(1)).get();
        assertHitCount(response, 1L);
    }

    public void testSimpleMoreLikeOnLongField() throws Exception {
        logger.info("Creating index test");
        assertAcked(prepareCreate("test")
                .addMapping("type1", "some_long", "type=long"));
        logger.info("Running Cluster Health");
        assertThat(ensureGreen(), equalTo(ClusterHealthStatus.GREEN));

        logger.info("Indexing...");
        client().index(indexRequest("test").type("type1").id("1").source(jsonBuilder().startObject().field("some_long", 1367484649580L).endObject())).actionGet();
        client().index(indexRequest("test").type("type1").id("2").source(jsonBuilder().startObject().field("some_long", 0).endObject())).actionGet();
        client().index(indexRequest("test").type("type1").id("3").source(jsonBuilder().startObject().field("some_long", -666).endObject())).actionGet();

        client().admin().indices().refresh(refreshRequest()).actionGet();

        logger.info("Running moreLikeThis");
        SearchResponse response = client().prepareSearch().setQuery(
                new MoreLikeThisQueryBuilder(null, new Item[] {new Item("test", "type1", "1")}).minTermFreq(1).minDocFreq(1)).get();
        assertHitCount(response, 0L);
    }

    public void testMoreLikeThisWithAliases() throws Exception {
        logger.info("Creating index test");
        assertAcked(prepareCreate("test").addMapping("type1",
                jsonBuilder().startObject().startObject("type1").startObject("properties")
                        .startObject("text").field("type", "text").endObject()
                        .endObject().endObject().endObject()));
        logger.info("Creating aliases alias release");
        client().admin().indices().prepareAliases()
            .addAlias("test", "release", termQuery("text", "release"))
            .addAlias("test", "beta", termQuery("text", "beta")).get();

        logger.info("Running Cluster Health");
        assertThat(ensureGreen(), equalTo(ClusterHealthStatus.GREEN));

        logger.info("Indexing...");
        client().index(indexRequest("test").type("type1").id("1").source(jsonBuilder().startObject().field("text", "lucene beta").endObject())).actionGet();
        client().index(indexRequest("test").type("type1").id("2").source(jsonBuilder().startObject().field("text", "lucene release").endObject())).actionGet();
        client().index(indexRequest("test").type("type1").id("3").source(jsonBuilder().startObject().field("text", "elasticsearch beta").endObject())).actionGet();
        client().index(indexRequest("test").type("type1").id("4").source(jsonBuilder().startObject().field("text", "elasticsearch release").endObject())).actionGet();
        client().admin().indices().refresh(refreshRequest()).actionGet();

        logger.info("Running moreLikeThis on index");
        SearchResponse response = client().prepareSearch().setQuery(
                new MoreLikeThisQueryBuilder(null, new Item[] {new Item("test", "type1", "1")}).minTermFreq(1).minDocFreq(1)).get();
        assertHitCount(response, 2L);

        logger.info("Running moreLikeThis on beta shard");
        response = client().prepareSearch("beta").setQuery(
                new MoreLikeThisQueryBuilder(null, new Item[] {new Item("test", "type1", "1")}).minTermFreq(1).minDocFreq(1)).get();
        assertHitCount(response, 1L);
        assertThat(response.getHits().getAt(0).getId(), equalTo("3"));

        logger.info("Running moreLikeThis on release shard");
        response = client().prepareSearch("release").setQuery(
                new MoreLikeThisQueryBuilder(null, new Item[] {new Item("test", "type1", "1")}).minTermFreq(1).minDocFreq(1)).get();
        assertHitCount(response, 1L);
        assertThat(response.getHits().getAt(0).getId(), equalTo("2"));

        logger.info("Running moreLikeThis on alias with node client");
        response = internalCluster().coordOnlyNodeClient().prepareSearch("beta").setQuery(
                new MoreLikeThisQueryBuilder(null, new Item[] {new Item("test", "type1", "1")}).minTermFreq(1).minDocFreq(1)).get();
        assertHitCount(response, 1L);
        assertThat(response.getHits().getAt(0).getId(), equalTo("3"));
    }

    // Issue #14944
    public void testMoreLikeThisWithAliasesInLikeDocuments() throws Exception {
        String indexName = "foo";
        String aliasName = "foo_name";
        String typeName = "bar";

        String mapping = XContentFactory.jsonBuilder().startObject().startObject("bar")
                .startObject("properties")
                .endObject()
                .endObject().endObject().string();
        client().admin().indices().prepareCreate(indexName).addMapping(typeName, mapping, XContentType.JSON).get();
        client().admin().indices().prepareAliases().addAlias(indexName, aliasName).get();

        assertThat(ensureGreen(), equalTo(ClusterHealthStatus.GREEN));

        client().index(indexRequest(indexName).type(typeName).id("1").source(jsonBuilder().startObject().field("text", "elasticsearch index").endObject())).actionGet();
        client().index(indexRequest(indexName).type(typeName).id("2").source(jsonBuilder().startObject().field("text", "lucene index").endObject())).actionGet();
        client().index(indexRequest(indexName).type(typeName).id("3").source(jsonBuilder().startObject().field("text", "elasticsearch index").endObject())).actionGet();
        refresh(indexName);

        SearchResponse response = client().prepareSearch().setQuery(
                new MoreLikeThisQueryBuilder(null, new Item[] {new Item(aliasName, typeName, "1")}).minTermFreq(1).minDocFreq(1)).get();
        assertHitCount(response, 2L);
        assertThat(response.getHits().getAt(0).getId(), equalTo("3"));
    }

    public void testMoreLikeThisIssue2197() throws Exception {
        Client client = client();
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("bar")
                .startObject("properties")
                .endObject()
                .endObject().endObject().string();
        client().admin().indices().prepareCreate("foo").addMapping("bar", mapping, XContentType.JSON).execute().actionGet();
        client().prepareIndex("foo", "bar", "1")
                .setSource(jsonBuilder().startObject().startObject("foo").field("bar", "boz").endObject().endObject())
                .execute().actionGet();
        client().admin().indices().prepareRefresh("foo").execute().actionGet();
        assertThat(ensureGreen(), equalTo(ClusterHealthStatus.GREEN));

        SearchResponse response = client().prepareSearch().setQuery(
                new MoreLikeThisQueryBuilder(null, new Item[] {new Item("foo", "bar", "1")})).get();
        assertNoFailures(response);
        assertThat(response, notNullValue());
        response = client().prepareSearch().setQuery(
                new MoreLikeThisQueryBuilder(null, new Item[] {new Item("foo", "bar", "1")})).get();
        assertNoFailures(response);
        assertThat(response, notNullValue());
    }

    // Issue #2489
    public void testMoreLikeWithCustomRouting() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("bar")
                .startObject("properties")
                .endObject()
                .endObject().endObject().string();
        client().admin().indices().prepareCreate("foo").addMapping("bar", mapping, XContentType.JSON).execute().actionGet();
        ensureGreen();

        client().prepareIndex("foo", "bar", "1")
                .setSource(jsonBuilder().startObject().startObject("foo").field("bar", "boz").endObject().endObject())
                .setRouting("2")
                .execute().actionGet();
        client().admin().indices().prepareRefresh("foo").execute().actionGet();

        SearchResponse response = client().prepareSearch().setQuery(
                new MoreLikeThisQueryBuilder(null, new Item[] {new Item("foo", "bar", "1").routing("2")})).get();
        assertNoFailures(response);
        assertThat(response, notNullValue());
    }

    // Issue #3039
    public void testMoreLikeThisIssueRoutingNotSerialized() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("bar")
                .startObject("properties")
                .endObject()
                .endObject().endObject().string();
        assertAcked(prepareCreate("foo", 2,
                Settings.builder().put(SETTING_NUMBER_OF_SHARDS, 2).put(SETTING_NUMBER_OF_REPLICAS, 0))
                .addMapping("bar", mapping, XContentType.JSON));
        ensureGreen();

        client().prepareIndex("foo", "bar", "1")
                .setSource(jsonBuilder().startObject().startObject("foo").field("bar", "boz").endObject().endObject())
                .setRouting("4000")
                .execute().actionGet();
        client().admin().indices().prepareRefresh("foo").execute().actionGet();
        SearchResponse response = client().prepareSearch().setQuery(
                new MoreLikeThisQueryBuilder(null, new Item[] {new Item("foo", "bar", "1").routing("4000")})).get();
        assertNoFailures(response);
        assertThat(response, notNullValue());
    }

    // Issue #3252
    public void testNumericField() throws Exception {
        final String[] numericTypes = new String[]{"byte", "short", "integer", "long"};
        prepareCreate("test").addMapping("type", jsonBuilder()
                    .startObject().startObject("type")
                    .startObject("properties")
                        .startObject("int_value").field("type", randomFrom(numericTypes)).endObject()
                        .startObject("string_value").field("type", "text").endObject()
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
        SearchResponse searchResponse = client().prepareSearch().setQuery(
                new MoreLikeThisQueryBuilder(null, new Item[] {new Item("test", "type", "1")}).minTermFreq(1).minDocFreq(1)).get();
        assertHitCount(searchResponse, 1L);

        // Explicit list of fields including numeric fields -> fail
        assertThrows(client().prepareSearch().setQuery(
                new MoreLikeThisQueryBuilder(new String[] {"string_value", "int_value"}, null, new Item[] {new Item("test", "type", "1")}).minTermFreq(1).minDocFreq(1)), SearchPhaseExecutionException.class);

        // mlt query with no field -> No results (because _all is not enabled)
        searchResponse = client().prepareSearch().setQuery(moreLikeThisQuery(new String[] {"index"}).minTermFreq(1).minDocFreq(1)).execute().actionGet();
        assertHitCount(searchResponse, 0L);

        // mlt query with string fields
        searchResponse = client().prepareSearch().setQuery(moreLikeThisQuery(new String[]{"string_value"}, new String[] {"index"}, null).minTermFreq(1).minDocFreq(1)).execute().actionGet();
        assertHitCount(searchResponse, 2L);

        // mlt query with at least a numeric field -> fail by default
        assertThrows(client().prepareSearch().setQuery(moreLikeThisQuery(new String[] {"string_value", "int_value"}, new String[] {"index"}, null)), SearchPhaseExecutionException.class);

        // mlt query with at least a numeric field -> fail by command
        assertThrows(client().prepareSearch().setQuery(moreLikeThisQuery(new String[] {"string_value", "int_value"}, new String[] {"index"}, null).failOnUnsupportedField(true)), SearchPhaseExecutionException.class);


        // mlt query with at least a numeric field but fail_on_unsupported_field set to false
        searchResponse = client().prepareSearch().setQuery(moreLikeThisQuery(new String[] {"string_value", "int_value"}, new String[] {"index"}, null).minTermFreq(1).minDocFreq(1).failOnUnsupportedField(false)).get();
        assertHitCount(searchResponse, 2L);

        // mlt field query on a numeric field -> failure by default
        assertThrows(client().prepareSearch().setQuery(moreLikeThisQuery(new String[] {"int_value"}, new String[] {"42"}, null).minTermFreq(1).minDocFreq(1)), SearchPhaseExecutionException.class);

        // mlt field query on a numeric field -> failure by command
        assertThrows(client().prepareSearch().setQuery(moreLikeThisQuery(new String[] {"int_value"}, new String[] {"42"}, null).minTermFreq(1).minDocFreq(1).failOnUnsupportedField(true)),
                SearchPhaseExecutionException.class);

        // mlt field query on a numeric field but fail_on_unsupported_field set to false
        searchResponse = client().prepareSearch().setQuery(moreLikeThisQuery(new String[] {"int_value"}, new String[] {"42"}, null).minTermFreq(1).minDocFreq(1).failOnUnsupportedField(false)).execute().actionGet();
        assertHitCount(searchResponse, 0L);
    }

    public void testSimpleMoreLikeInclude() throws Exception {
        logger.info("Creating index test");
        assertAcked(prepareCreate("test").addMapping("type1",
                jsonBuilder().startObject().startObject("type1").startObject("properties")
                        .startObject("text").field("type", "text").endObject()
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
        SearchResponse response = client().prepareSearch().setQuery(
                new MoreLikeThisQueryBuilder(null, new Item[] {new Item("test", "type1", "1")}).minTermFreq(1).minDocFreq(1).include(true).minimumShouldMatch("0%")).get();
        assertOrderedSearchHits(response, "1", "2");

        response = client().prepareSearch().setQuery(
                new MoreLikeThisQueryBuilder(null, new Item[] {new Item("test", "type1", "2")}).minTermFreq(1).minDocFreq(1).include(true).minimumShouldMatch("0%")).get();
        assertOrderedSearchHits(response, "2", "1");

        logger.info("Running More Like This with include false");
        response = client().prepareSearch().setQuery(
                new MoreLikeThisQueryBuilder(null, new Item[] {new Item("test", "type1", "1")}).minTermFreq(1).minDocFreq(1).minimumShouldMatch("0%")).get();
        assertSearchHits(response, "2");
    }

    public void testSimpleMoreLikeThisIds() throws Exception {
        logger.info("Creating index test");
        assertAcked(prepareCreate("test").addMapping("type1",
                jsonBuilder().startObject().startObject("type1").startObject("properties")
                        .startObject("text").field("type", "text").endObject()
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
        MoreLikeThisQueryBuilder queryBuilder = QueryBuilders.moreLikeThisQuery(new String[] {"text"}, null, ids("1")).include(true).minTermFreq(1).minDocFreq(1);
        SearchResponse mltResponse = client().prepareSearch().setTypes("type1").setQuery(queryBuilder).execute().actionGet();
        assertHitCount(mltResponse, 3L);
    }

    public void testSimpleMoreLikeThisIdsMultipleTypes() throws Exception {
        logger.info("Creating index test");
        int numOfTypes = randomIntBetween(2, 10);
        CreateIndexRequestBuilder createRequestBuilder = prepareCreate("test")
                .setSettings(Settings.builder().put("index.version.created", Version.V_5_6_0.id));
        for (int i = 0; i < numOfTypes; i++) {
            createRequestBuilder.addMapping("type" + i, jsonBuilder().startObject().startObject("type" + i).startObject("properties")
                    .startObject("text").field("type", "text").endObject()
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
        MoreLikeThisQueryBuilder queryBuilder = QueryBuilders.moreLikeThisQuery(new String[] {"text"}, null, new Item[] {new Item("test", "type0", "0")}).include(true).minTermFreq(1).minDocFreq(1);

        String[] types = new String[numOfTypes];
        for (int i = 0; i < numOfTypes; i++) {
            types[i] = "type"+i;
        }
        SearchResponse mltResponse = client().prepareSearch().setTypes(types).setQuery(queryBuilder).execute().actionGet();
        assertHitCount(mltResponse, numOfTypes);
    }

    public void testMoreLikeThisMultiValueFields() throws Exception {
        logger.info("Creating the index ...");
        assertAcked(prepareCreate("test")
                .addMapping("type1", "text", "type=text,analyzer=keyword")
                .setSettings(Settings.builder().put(SETTING_NUMBER_OF_SHARDS, 1)));
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
        for (int i = 0; i < maxIters; i++) {
            int max_query_terms = randomIntBetween(1, values.length);
            logger.info("Running More Like This with max_query_terms = {}", max_query_terms);
            MoreLikeThisQueryBuilder mltQuery = moreLikeThisQuery(new String[] {"text"}, null, new Item[] {new Item(null, null, "0")})
                    .minTermFreq(1).minDocFreq(1)
                    .maxQueryTerms(max_query_terms).minimumShouldMatch("0%");
            SearchResponse response = client().prepareSearch("test").setTypes("type1")
                    .setQuery(mltQuery).execute().actionGet();
            assertSearchResponse(response);
            assertHitCount(response, max_query_terms);
        }
    }

    public void testMinimumShouldMatch() throws ExecutionException, InterruptedException {
        logger.info("Creating the index ...");
        assertAcked(prepareCreate("test")
                .addMapping("type1", "text", "type=text,analyzer=whitespace")
                .setSettings(Settings.builder().put(SETTING_NUMBER_OF_SHARDS, 1)));
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
            MoreLikeThisQueryBuilder mltQuery = moreLikeThisQuery(new String[] {"text"}, new String[] {"1 2 3 4 5 6 7 8 9 10"}, null)
                    .minTermFreq(1)
                    .minDocFreq(1)
                    .minimumShouldMatch(minimumShouldMatch);
            logger.info("Testing with minimum_should_match = {}", minimumShouldMatch);
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

    public void testMoreLikeThisArtificialDocs() throws Exception {
        int numFields = randomIntBetween(5, 10);

        createIndex("test");
        ensureGreen();

        logger.info("Indexing a single document ...");
        XContentBuilder doc = jsonBuilder().startObject();
        for (int i = 0; i < numFields; i++) {
            doc.field("field" + i, generateRandomStringArray(5, 10, false) + "a"); // make sure they are not all empty
        }
        doc.endObject();
        indexRandom(true, client().prepareIndex("test", "type1", "0").setSource(doc));

        logger.info("Checking the document matches ...");
        MoreLikeThisQueryBuilder mltQuery = moreLikeThisQuery(new Item[] {new Item("test", "type1", doc).routing("0")})  // routing to ensure we hit the shard with the doc
                .minTermFreq(0)
                .minDocFreq(0)
                .maxQueryTerms(100)
                .minimumShouldMatch("100%"); // strict all terms must match!
        SearchResponse response = client().prepareSearch("test").setTypes("type1")
                .setQuery(mltQuery).get();
        assertSearchResponse(response);
        assertHitCount(response, 1);
    }

    public void testMoreLikeThisMalformedArtificialDocs() throws Exception {
        logger.info("Creating the index ...");
        assertAcked(prepareCreate("test")
                .addMapping("type1", "text", "type=text,analyzer=whitespace", "date", "type=date"));
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
        MoreLikeThisQueryBuilder mltQuery = moreLikeThisQuery(new Item[] {new Item("test", "type1", malformedFieldDoc)})
                .minTermFreq(0)
                .minDocFreq(0)
                .minimumShouldMatch("0%");
        SearchResponse response = client().prepareSearch("test").setTypes("type1")
                .setQuery(mltQuery).get();
        assertSearchResponse(response);
        assertHitCount(response, 0);

        logger.info("Checking with an empty document ...");
        XContentBuilder emptyDoc = jsonBuilder().startObject().endObject();
        mltQuery = moreLikeThisQuery(null, new Item[] {new Item("test", "type1", emptyDoc)})
                .minTermFreq(0)
                .minDocFreq(0)
                .minimumShouldMatch("0%");
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
        mltQuery = moreLikeThisQuery(null, new Item[] {new Item("test", "type1", normalDoc)})
                .minTermFreq(0)
                .minDocFreq(0)
                .minimumShouldMatch("100%");  // strict all terms must match but date is ignored
        response = client().prepareSearch("test").setTypes("type1")
                .setQuery(mltQuery).get();
        assertSearchResponse(response);
        assertHitCount(response, 1);
    }

    public void testMoreLikeThisUnlike() throws ExecutionException, InterruptedException, IOException {
        createIndex("test");
        ensureGreen();
        int numFields = randomIntBetween(5, 10);

        logger.info("Create a document that has all the fields.");
        XContentBuilder doc = jsonBuilder().startObject();
        for (int i = 0; i < numFields; i++) {
            doc.field("field"+i, i+"");
        }
        doc.endObject();

        logger.info("Indexing each field value of this document as a single document.");
        List<IndexRequestBuilder> builders = new ArrayList<>();
        for (int i = 0; i < numFields; i++) {
            builders.add(client().prepareIndex("test", "type1", i+"").setSource("field"+i, i+""));
        }
        indexRandom(true, builders);

        logger.info("First check the document matches all indexed docs.");
        MoreLikeThisQueryBuilder mltQuery = moreLikeThisQuery(new Item[] {new Item("test", "type1", doc)})
                .minTermFreq(0)
                .minDocFreq(0)
                .maxQueryTerms(100)
                .minimumShouldMatch("0%");
        SearchResponse response = client().prepareSearch("test").setTypes("type1")
                .setQuery(mltQuery).get();
        assertSearchResponse(response);
        assertHitCount(response, numFields);

        logger.info("Now check like this doc, but ignore one doc in the index, then two and so on...");
        List<Item> docs = new ArrayList<>(numFields);
        for (int i = 0; i < numFields; i++) {
            docs.add(new Item("test", "type1", i+""));
            mltQuery = moreLikeThisQuery(null, new Item[] {new Item("test", "type1", doc)})
                    .unlike(docs.toArray(new Item[docs.size()]))
                    .minTermFreq(0)
                    .minDocFreq(0)
                    .maxQueryTerms(100)
                    .include(true)
                    .minimumShouldMatch("0%");

            response = client().prepareSearch("test").setTypes("type1").setQuery(mltQuery).get();
            assertSearchResponse(response);
            assertHitCount(response, numFields - (i + 1));
        }
    }

    public void testSelectFields() throws IOException, ExecutionException, InterruptedException {
        assertAcked(prepareCreate("test")
                .addMapping("type1", "text", "type=text,analyzer=whitespace", "text1", "type=text,analyzer=whitespace"));
        ensureGreen("test");

        indexRandom(true, client().prepareIndex("test", "type1", "1").setSource(jsonBuilder()
                        .startObject()
                        .field("text", "hello world")
                        .field("text1", "elasticsearch")
                        .endObject()),
                client().prepareIndex("test", "type1", "2").setSource(jsonBuilder()
                        .startObject()
                        .field("text", "goodby moon")
                        .field("text1", "elasticsearch")
                        .endObject()));

        MoreLikeThisQueryBuilder mltQuery = moreLikeThisQuery(new Item[] {new Item("test", "type1", "1")})
                .minTermFreq(0)
                .minDocFreq(0)
                .include(true)
                .minimumShouldMatch("1%");
        SearchResponse response = client().prepareSearch("test").setTypes("type1")
                .setQuery(mltQuery).get();
        assertSearchResponse(response);
        assertHitCount(response, 2);

        mltQuery = moreLikeThisQuery(new String[] {"text"}, null, new Item[] {new Item("test", "type1", "1")})
                .minTermFreq(0)
                .minDocFreq(0)
                .include(true)
                .minimumShouldMatch("1%");
        response = client().prepareSearch("test").setTypes("type1")
                .setQuery(mltQuery).get();
        assertSearchResponse(response);
        assertHitCount(response, 1);
    }

    public void testWithRouting() throws IOException {
        client().prepareIndex("index", "type", "1").setRouting("3").setSource("text", "this is a document").get();
        client().prepareIndex("index", "type", "2").setRouting("1").setSource("text", "this is another document").get();
        client().prepareIndex("index", "type", "3").setRouting("4").setSource("text", "this is yet another document").get();
        refresh("index");

        Item item = new Item("index", "type", "2").routing("1");
        MoreLikeThisQueryBuilder moreLikeThisQueryBuilder = new MoreLikeThisQueryBuilder(new String[]{"text"}, null, new Item[]{item});
        moreLikeThisQueryBuilder.minTermFreq(1);
        moreLikeThisQueryBuilder.minDocFreq(1);
        SearchResponse searchResponse = client().prepareSearch("index").setQuery(moreLikeThisQueryBuilder).get();
        assertEquals(2, searchResponse.getHits().totalHits);
    }
}
