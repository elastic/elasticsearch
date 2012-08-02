/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.test.integration.search.highlight;

import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.indices.IndexMissingException;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.test.integration.AbstractNodesTests;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.Arrays;

import static org.elasticsearch.action.search.SearchType.QUERY_THEN_FETCH;
import static org.elasticsearch.client.Requests.searchRequest;
import static org.elasticsearch.common.unit.TimeValue.timeValueMinutes;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.*;
import static org.elasticsearch.search.builder.SearchSourceBuilder.highlight;
import static org.elasticsearch.search.builder.SearchSourceBuilder.searchSource;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

/**
 *
 */
public class HighlighterSearchTests extends AbstractNodesTests {

    private Client client;

    @BeforeClass
    public void createNodes() throws Exception {
        startNode("server1");
        startNode("server2");
        client = getClient();
    }

    @AfterClass
    public void closeNodes() {
        client.close();
        closeAllNodes();
    }

    protected Client getClient() {
        return client("server1");
    }

    @Test
    public void testSourceLookupHighlightingUsingPlainHighlighter() throws Exception {
        try {
            client.admin().indices().prepareDelete("test").execute().actionGet();
        } catch (Exception e) {
            // ignore
        }

        client.admin().indices().prepareCreate("test").setSettings(ImmutableSettings.settingsBuilder().put("number_of_shards", 2))
                .addMapping("type1", jsonBuilder().startObject().startObject("type1").startObject("properties")
                        // we don't store title and don't use term vector, now lets see if it works...
                        .startObject("title").field("type", "string").field("store", "no").field("term_vector", "no").endObject()
                        .startObject("attachments").startObject("properties").startObject("body").field("type", "string").field("store", "no").field("term_vector", "no").endObject().endObject().endObject()
                        .endObject().endObject().endObject())
                .execute().actionGet();

        for (int i = 0; i < 5; i++) {
            client.prepareIndex("test", "type1", Integer.toString(i))
                    .setSource(XContentFactory.jsonBuilder().startObject()
                            .field("title", "This is a test on the highlighting bug present in elasticsearch")
                            .startArray("attachments").startObject().field("body", "attachment 1").endObject().startObject().field("body", "attachment 2").endObject().endArray()
                            .endObject())
                    .setRefresh(true).execute().actionGet();
        }

        SearchResponse search = client.prepareSearch()
                .setQuery(fieldQuery("title", "bug"))
                .addHighlightedField("title", -1, 0)
                .execute().actionGet();

        assertThat(Arrays.toString(search.shardFailures()), search.failedShards(), equalTo(0));

        assertThat(search.hits().totalHits(), equalTo(5l));
        assertThat(search.hits().hits().length, equalTo(5));

        for (SearchHit hit : search.hits()) {
            assertThat(hit.highlightFields().get("title").fragments()[0].string(), equalTo("This is a test on the highlighting <em>bug</em> present in elasticsearch"));
        }

        search = client.prepareSearch()
                .setQuery(fieldQuery("attachments.body", "attachment"))
                .addHighlightedField("attachments.body", -1, 0)
                .execute().actionGet();

        assertThat(Arrays.toString(search.shardFailures()), search.failedShards(), equalTo(0));

        assertThat(search.hits().totalHits(), equalTo(5l));
        assertThat(search.hits().hits().length, equalTo(5));

        for (SearchHit hit : search.hits()) {
            assertThat(hit.highlightFields().get("attachments.body").fragments()[0].string(), equalTo("<em>attachment</em> 1"));
            assertThat(hit.highlightFields().get("attachments.body").fragments()[1].string(), equalTo("<em>attachment</em> 2"));
        }
    }

    @Test
    public void testSourceLookupHighlightingUsingFastVectorHighlighter() throws Exception {
        try {
            client.admin().indices().prepareDelete("test").execute().actionGet();
        } catch (Exception e) {
            // ignore
        }

        client.admin().indices().prepareCreate("test").setSettings(ImmutableSettings.settingsBuilder().put("number_of_shards", 2))
                .addMapping("type1", jsonBuilder().startObject().startObject("type1").startObject("properties")
                        // we don't store title, now lets see if it works...
                        .startObject("title").field("type", "string").field("store", "no").field("term_vector", "with_positions_offsets").endObject()
                        .startObject("attachments").startObject("properties").startObject("body").field("type", "string").field("store", "no").field("term_vector", "with_positions_offsets").endObject().endObject().endObject()
                        .endObject().endObject().endObject())
                .execute().actionGet();

        for (int i = 0; i < 5; i++) {
            client.prepareIndex("test", "type1", Integer.toString(i))
                    .setSource(XContentFactory.jsonBuilder().startObject()
                            .field("title", "This is a test on the highlighting bug present in elasticsearch")
                            .startArray("attachments").startObject().field("body", "attachment 1").endObject().startObject().field("body", "attachment 2").endObject().endArray()
                            .endObject())
                    .setRefresh(true).execute().actionGet();
        }

        SearchResponse search = client.prepareSearch()
                .setQuery(fieldQuery("title", "bug"))
                .addHighlightedField("title", -1, 0)
                .execute().actionGet();

        assertThat(Arrays.toString(search.shardFailures()), search.failedShards(), equalTo(0));

        assertThat(search.hits().totalHits(), equalTo(5l));
        assertThat(search.hits().hits().length, equalTo(5));

        for (SearchHit hit : search.hits()) {
            assertThat(hit.highlightFields().get("title").fragments()[0].string(), equalTo("This is a test on the highlighting <em>bug</em> present in elasticsearch"));
        }

        search = client.prepareSearch()
                .setQuery(fieldQuery("attachments.body", "attachment"))
                .addHighlightedField("attachments.body", -1, 2)
                .execute().actionGet();

        assertThat(Arrays.toString(search.shardFailures()), search.failedShards(), equalTo(0));

        assertThat(search.hits().totalHits(), equalTo(5l));
        assertThat(search.hits().hits().length, equalTo(5));

        for (SearchHit hit : search.hits()) {
            assertThat(hit.highlightFields().get("attachments.body").fragments()[0].string(), equalTo("<em>attachment</em> 1"));
            assertThat(hit.highlightFields().get("attachments.body").fragments()[1].string(), equalTo("<em>attachment</em> 2"));
        }
    }

    @Test
    public void testHighlightIssue1994() throws Exception {
        try {
            client.admin().indices().prepareDelete("test").execute().actionGet();
        } catch (Exception e) {
            // ignore
        }

        client.admin().indices().prepareCreate("test").setSettings(ImmutableSettings.settingsBuilder().put("number_of_shards", 2))
                .addMapping("type1", jsonBuilder().startObject().startObject("type1").startObject("properties")
                        // we don't store title, now lets see if it works...
                        .startObject("title").field("type", "string").field("store", "no").endObject()
                        .startObject("titleTV").field("type", "string").field("store", "no").field("term_vector", "with_positions_offsets").endObject()
                        .endObject().endObject().endObject())
                .execute().actionGet();


        client.prepareIndex("test", "type1", "1")
                .setSource(XContentFactory.jsonBuilder().startObject()
                        .startArray("title")
                        .value("This is a test on the highlighting bug present in elasticsearch")
                        .value("The bug is bugging us")
                        .endArray()
                        .startArray("titleTV")
                        .value("This is a test on the highlighting bug present in elasticsearch")
                        .value("The bug is bugging us")
                        .endArray()
                        .endObject())
                .setRefresh(true).execute().actionGet();


        client.prepareIndex("test", "type1", "2")
                .setSource(XContentFactory.jsonBuilder().startObject()
                        .startArray("titleTV")
                        .value("some text to highlight")
                        .value("highlight other text")
                        .endArray()
                        .endObject())
                .setRefresh(true).execute().actionGet();

        SearchResponse search = client.prepareSearch()
                .setQuery(fieldQuery("title", "bug"))
                .addHighlightedField("title", -1, 2)
                .addHighlightedField("titleTV", -1, 2)
                .execute().actionGet();

        assertThat(search.hits().totalHits(), equalTo(1l));
        assertThat(search.hits().hits().length, equalTo(1));

        assertThat(search.hits().hits()[0].highlightFields().get("title").fragments().length, equalTo(2));
        assertThat(search.hits().hits()[0].highlightFields().get("title").fragments()[0].string(), equalTo("This is a test on the highlighting <em>bug</em> present in elasticsearch"));
        assertThat(search.hits().hits()[0].highlightFields().get("title").fragments()[1].string(), equalTo("The <em>bug</em> is bugging us"));
        assertThat(search.hits().hits()[0].highlightFields().get("titleTV").fragments().length, equalTo(2));
//    assertThat(search.hits().hits()[0].highlightFields().get("titleTV").fragments()[0], equalTo("This is a test on the highlighting <em>bug</em> present in elasticsearch"));
        assertThat(search.hits().hits()[0].highlightFields().get("titleTV").fragments()[0].string(), equalTo("highlighting <em>bug</em> present in elasticsearch")); // FastVectorHighlighter starts highlighting from startOffset - margin
        assertThat(search.hits().hits()[0].highlightFields().get("titleTV").fragments()[1].string(), equalTo("The <em>bug</em> is bugging us"));

        search = client.prepareSearch()
                .setQuery(fieldQuery("titleTV", "highlight"))
                .addHighlightedField("titleTV", -1, 2)
                .execute().actionGet();

        assertThat(search.hits().totalHits(), equalTo(1l));
        assertThat(search.hits().hits().length, equalTo(1));
        assertThat(search.hits().hits()[0].highlightFields().get("titleTV").fragments().length, equalTo(2));
        assertThat(search.hits().hits()[0].highlightFields().get("titleTV").fragments()[0].string(), equalTo("text to <em>highlight</em>"));
        assertThat(search.hits().hits()[0].highlightFields().get("titleTV").fragments()[1].string(), equalTo("<em>highlight</em> other text"));
    }

    @Test
    public void testPlainHighlighter() throws Exception {
        try {
            client.admin().indices().prepareDelete("test").execute().actionGet();
        } catch (IndexMissingException e) {
            // its ok
        }
        client.admin().indices().prepareCreate("test").execute().actionGet();
        client.admin().cluster().prepareHealth("test").setWaitForGreenStatus().execute().actionGet();

        client.prepareIndex("test", "type1")
                .setSource("field1", "this is a test", "field2", "The quick brown fox jumps over the lazy dog")
                .setRefresh(true).execute().actionGet();

        logger.info("--> highlighting and searching on field1");
        SearchSourceBuilder source = searchSource()
                .query(termQuery("field1", "test"))
                .from(0).size(60).explain(true)
                .highlight(highlight().field("field1").order("score").preTags("<xxx>").postTags("</xxx>"));

        SearchResponse searchResponse = client.search(searchRequest("test").source(source).searchType(QUERY_THEN_FETCH).scroll(timeValueMinutes(10))).actionGet();
        assertThat("Failures " + Arrays.toString(searchResponse.shardFailures()), searchResponse.shardFailures().length, equalTo(0));
        assertThat(searchResponse.hits().totalHits(), equalTo(1l));

        assertThat(searchResponse.hits().getAt(0).highlightFields().get("field1").fragments()[0].string(), equalTo("this is a <xxx>test</xxx>"));

        logger.info("--> searching on _all, highlighting on field1");
        source = searchSource()
                .query(termQuery("_all", "test"))
                .from(0).size(60).explain(true)
                .highlight(highlight().field("field1").order("score").preTags("<xxx>").postTags("</xxx>"));

        searchResponse = client.search(searchRequest("test").source(source).searchType(QUERY_THEN_FETCH).scroll(timeValueMinutes(10))).actionGet();
        assertThat("Failures " + Arrays.toString(searchResponse.shardFailures()), searchResponse.shardFailures().length, equalTo(0));
        assertThat(searchResponse.hits().totalHits(), equalTo(1l));

        assertThat(searchResponse.hits().getAt(0).highlightFields().get("field1").fragments()[0].string(), equalTo("this is a <xxx>test</xxx>"));

        logger.info("--> searching on _all, highlighting on field2");
        source = searchSource()
                .query(termQuery("_all", "quick"))
                .from(0).size(60).explain(true)
                .highlight(highlight().field("field2").order("score").preTags("<xxx>").postTags("</xxx>"));

        searchResponse = client.search(searchRequest("test").source(source).searchType(QUERY_THEN_FETCH).scroll(timeValueMinutes(10))).actionGet();
        assertThat("Failures " + Arrays.toString(searchResponse.shardFailures()), searchResponse.shardFailures().length, equalTo(0));
        assertThat(searchResponse.hits().totalHits(), equalTo(1l));

        assertThat(searchResponse.hits().getAt(0).highlightFields().get("field2").fragments()[0].string(), equalTo("The <xxx>quick</xxx> brown fox jumps over the lazy dog"));

        logger.info("--> searching on _all, highlighting on field2");
        source = searchSource()
                .query(prefixQuery("_all", "qui"))
                .from(0).size(60).explain(true)
                .highlight(highlight().field("field2").order("score").preTags("<xxx>").postTags("</xxx>"));

        searchResponse = client.search(searchRequest("test").source(source).searchType(QUERY_THEN_FETCH).scroll(timeValueMinutes(10))).actionGet();
        assertThat("Failures " + Arrays.toString(searchResponse.shardFailures()), searchResponse.shardFailures().length, equalTo(0));
        assertThat(searchResponse.hits().totalHits(), equalTo(1l));

        assertThat(searchResponse.hits().getAt(0).highlightFields().get("field2").fragments()[0].string(), equalTo("The <xxx>quick</xxx> brown fox jumps over the lazy dog"));
    }

    @Test
    public void testFastVectorHighlighter() throws Exception {
        try {
            client.admin().indices().prepareDelete("test").execute().actionGet();
        } catch (IndexMissingException e) {
            // its ok
        }
        client.admin().indices().prepareCreate("test").addMapping("type1", type1TermVectorMapping()).execute().actionGet();
        client.admin().cluster().prepareHealth("test").setWaitForGreenStatus().execute().actionGet();

        client.prepareIndex("test", "type1")
                .setSource("field1", "this is a test", "field2", "The quick brown fox jumps over the lazy dog")
                .setRefresh(true).execute().actionGet();

        logger.info("--> highlighting and searching on field1");
        SearchSourceBuilder source = searchSource()
                .query(termQuery("field1", "test"))
                .from(0).size(60).explain(true)
                .highlight(highlight().field("field1", 100, 0).order("score").preTags("<xxx>").postTags("</xxx>"));

        SearchResponse searchResponse = client.search(searchRequest("test").source(source).searchType(QUERY_THEN_FETCH).scroll(timeValueMinutes(10))).actionGet();
        assertThat("Failures " + Arrays.toString(searchResponse.shardFailures()), searchResponse.shardFailures().length, equalTo(0));
        assertThat(searchResponse.hits().totalHits(), equalTo(1l));

        assertThat(searchResponse.hits().getAt(0).highlightFields().get("field1").fragments()[0].string(), equalTo("this is a <xxx>test</xxx>"));

        logger.info("--> searching on _all, highlighting on field1");
        source = searchSource()
                .query(termQuery("_all", "test"))
                .from(0).size(60).explain(true)
                .highlight(highlight().field("field1", 100, 0).order("score").preTags("<xxx>").postTags("</xxx>"));

        searchResponse = client.search(searchRequest("test").source(source).searchType(QUERY_THEN_FETCH).scroll(timeValueMinutes(10))).actionGet();
        assertThat("Failures " + Arrays.toString(searchResponse.shardFailures()), searchResponse.shardFailures().length, equalTo(0));
        assertThat(searchResponse.hits().totalHits(), equalTo(1l));

        // LUCENE 3.1 UPGRADE: Caused adding the space at the end...
        assertThat(searchResponse.hits().getAt(0).highlightFields().get("field1").fragments()[0].string(), equalTo("this is a <xxx>test</xxx>"));

        logger.info("--> searching on _all, highlighting on field2");
        source = searchSource()
                .query(termQuery("_all", "quick"))
                .from(0).size(60).explain(true)
                .highlight(highlight().field("field2", 100, 0).order("score").preTags("<xxx>").postTags("</xxx>"));

        searchResponse = client.search(searchRequest("test").source(source).searchType(QUERY_THEN_FETCH).scroll(timeValueMinutes(10))).actionGet();
        assertThat("Failures " + Arrays.toString(searchResponse.shardFailures()), searchResponse.shardFailures().length, equalTo(0));
        assertThat(searchResponse.hits().totalHits(), equalTo(1l));

        // LUCENE 3.1 UPGRADE: Caused adding the space at the end...
        assertThat(searchResponse.hits().getAt(0).highlightFields().get("field2").fragments()[0].string(), equalTo("The <xxx>quick</xxx> brown fox jumps over the lazy dog"));

        logger.info("--> searching on _all, highlighting on field2");
        source = searchSource()
                .query(prefixQuery("_all", "qui"))
                .from(0).size(60).explain(true)
                .highlight(highlight().field("field2", 100, 0).order("score").preTags("<xxx>").postTags("</xxx>"));

        searchResponse = client.search(searchRequest("test").source(source).searchType(QUERY_THEN_FETCH).scroll(timeValueMinutes(10))).actionGet();
        assertThat("Failures " + Arrays.toString(searchResponse.shardFailures()), searchResponse.shardFailures().length, equalTo(0));
        assertThat(searchResponse.hits().totalHits(), equalTo(1l));

        // LUCENE 3.1 UPGRADE: Caused adding the space at the end...
        assertThat(searchResponse.hits().getAt(0).highlightFields().get("field2").fragments()[0].string(), equalTo("The <xxx>quick</xxx> brown fox jumps over the lazy dog"));
    }

    @Test
    public void testFastVectorHighlighterManyDocs() throws Exception {
        try {
            client.admin().indices().prepareDelete("test").execute().actionGet();
        } catch (ElasticSearchException e) {
            assertThat(e.unwrapCause(), instanceOf(IndexMissingException.class));
        }
        client.admin().indices().prepareCreate("test").addMapping("type1", type1TermVectorMapping()).execute().actionGet();
        client.admin().cluster().prepareHealth("test").setWaitForGreenStatus().execute().actionGet();

        int COUNT = 100;
        logger.info("--> indexing docs");
        for (int i = 0; i < COUNT; i++) {
            client.prepareIndex("test", "type1", Integer.toString(i)).setSource("field1", "test " + i).execute().actionGet();
            if (i % 5 == 0) {
                // flush so we get updated readers and segmented readers
                client.admin().indices().prepareFlush().execute().actionGet();
            }
        }

        client.admin().indices().prepareRefresh().execute().actionGet();

        logger.info("--> searching explicitly on field1 and highlighting on it");
        SearchResponse searchResponse = client.prepareSearch()
                .setSize(COUNT)
                .setQuery(termQuery("field1", "test"))
                .addHighlightedField("field1", 100, 0)
                .execute().actionGet();
        assertThat(searchResponse.hits().totalHits(), equalTo((long) COUNT));
        assertThat(searchResponse.hits().hits().length, equalTo(COUNT));
        for (SearchHit hit : searchResponse.hits()) {
            // LUCENE 3.1 UPGRADE: Caused adding the space at the end...
            assertThat(hit.highlightFields().get("field1").fragments()[0].string(), equalTo("<em>test</em> " + hit.id()));
        }

        logger.info("--> searching explicitly on field1 and highlighting on it, with DFS");
        searchResponse = client.prepareSearch()
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                .setSize(COUNT)
                .setQuery(termQuery("field1", "test"))
                .addHighlightedField("field1", 100, 0)
                .execute().actionGet();
        assertThat(searchResponse.hits().totalHits(), equalTo((long) COUNT));
        assertThat(searchResponse.hits().hits().length, equalTo(COUNT));
        for (SearchHit hit : searchResponse.hits()) {
            assertThat(hit.highlightFields().get("field1").fragments()[0].string(), equalTo("<em>test</em> " + hit.id()));
        }

        logger.info("--> searching explicitly _all and highlighting on _all");
        searchResponse = client.prepareSearch()
                .setSize(COUNT)
                .setQuery(termQuery("_all", "test"))
                .addHighlightedField("_all", 100, 0)
                .execute().actionGet();
        assertThat(searchResponse.hits().totalHits(), equalTo((long) COUNT));
        assertThat(searchResponse.hits().hits().length, equalTo(COUNT));
        for (SearchHit hit : searchResponse.hits()) {
            assertThat(hit.highlightFields().get("_all").fragments()[0].string(), equalTo("<em>test</em> " + hit.id() + " "));
        }
    }

    public XContentBuilder type1TermVectorMapping() throws IOException {
        return XContentFactory.jsonBuilder().startObject().startObject("type1")
                .startObject("_all").field("store", "yes").field("termVector", "with_positions_offsets").endObject()
                .startObject("properties")
                .startObject("field1").field("type", "string").field("termVector", "with_positions_offsets").endObject()
                .startObject("field2").field("type", "string").field("termVector", "with_positions_offsets").endObject()
                .endObject()
                .endObject().endObject();
    }

    @Test
    public void testSameContent() throws Exception {
        try {
            client.admin().indices().prepareDelete("test").execute().actionGet();
        } catch (Exception e) {
            // ignore
        }

        client.admin().indices().prepareCreate("test").setSettings(ImmutableSettings.settingsBuilder().put("number_of_shards", 2))
                .addMapping("type1", jsonBuilder().startObject().startObject("type1").startObject("properties")
                        .startObject("title").field("type", "string").field("store", "yes").field("term_vector", "with_positions_offsets").endObject()
                        .endObject().endObject().endObject())
                .execute().actionGet();

        for (int i = 0; i < 5; i++) {
            client.prepareIndex("test", "type1", Integer.toString(i))
                    .setSource("title", "This is a test on the highlighting bug present in elasticsearch").setRefresh(true).execute().actionGet();
        }

        SearchResponse search = client.prepareSearch()
                .setQuery(fieldQuery("title", "bug"))
                .addHighlightedField("title", -1, 0)
                .execute().actionGet();

        assertThat(search.hits().totalHits(), equalTo(5l));
        assertThat(search.hits().hits().length, equalTo(5));
        assertThat(search.getFailedShards(), equalTo(0));

        for (SearchHit hit : search.hits()) {
            // LUCENE 3.1 UPGRADE: Caused adding the space at the end...
            assertThat(hit.highlightFields().get("title").fragments()[0].string(), equalTo("This is a test on the highlighting <em>bug</em> present in elasticsearch"));
        }
    }

    @Test
    public void testFastVectorHighlighterOffsetParameter() throws Exception {
        try {
            client.admin().indices().prepareDelete("test").execute().actionGet();
        } catch (Exception e) {
            // ignore
        }

        client.admin().indices().prepareCreate("test").setSettings(ImmutableSettings.settingsBuilder().put("number_of_shards", 2))
                .addMapping("type1", jsonBuilder().startObject().startObject("type1").startObject("properties")
                        .startObject("title").field("type", "string").field("store", "yes").field("term_vector", "with_positions_offsets").endObject()
                        .endObject().endObject().endObject())
                .execute().actionGet();

        for (int i = 0; i < 5; i++) {
            client.prepareIndex("test", "type1", Integer.toString(i))
                    .setSource("title", "This is a test on the highlighting bug present in elasticsearch").setRefresh(true).execute().actionGet();
        }

        SearchResponse search = client.prepareSearch()
                .setQuery(fieldQuery("title", "bug"))
                .addHighlightedField("title", 50, 1, 10)
                .execute().actionGet();

        assertThat(Arrays.toString(search.shardFailures()), search.failedShards(), equalTo(0));

        assertThat(search.hits().totalHits(), equalTo(5l));
        assertThat(search.hits().hits().length, equalTo(5));

        for (SearchHit hit : search.hits()) {
            // LUCENE 3.1 UPGRADE: Caused adding the space at the end...
            assertThat(hit.highlightFields().get("title").fragments()[0].string(), equalTo("highlighting <em>bug</em> present in elasticsearch"));
        }
    }

    @Test
    public void testEscapeHtml() throws Exception {

        try {
            client.admin().indices().prepareDelete("test").execute().actionGet();
        } catch (Exception e) {
            // ignore
        }

        client.admin().indices().prepareCreate("test").setSettings(ImmutableSettings.settingsBuilder().put("number_of_shards", 2))
                .addMapping("type1", jsonBuilder().startObject().startObject("type1").startObject("properties")
                        .startObject("title").field("type", "string").field("store", "yes")
                        .endObject().endObject().endObject())
                .execute().actionGet();

        for (int i = 0; i < 5; i++) {
            client.prepareIndex("test", "type1", Integer.toString(i))
                    .setSource("title", "This is a html escaping highlighting test for *&? elasticsearch").setRefresh(true).execute().actionGet();
        }

        SearchResponse search = client.prepareSearch()
                .setQuery(fieldQuery("title", "test"))
                .setHighlighterEncoder("html")
                .addHighlightedField("title", 50, 1, 10)
                .execute().actionGet();

        assertThat(Arrays.toString(search.shardFailures()), search.failedShards(), equalTo(0));


        assertThat(search.hits().totalHits(), equalTo(5l));
        assertThat(search.hits().hits().length, equalTo(5));

        for (SearchHit hit : search.hits()) {
            // LUCENE 3.1 UPGRADE: Caused adding the space at the end...
            assertThat(hit.highlightFields().get("title").fragments()[0].string(), equalTo("This is a html escaping highlighting <em>test</em> for *&amp;? elasticsearch"));
        }
    }

    @Test
    public void testEscapeHtml_vector() throws Exception {

        try {
            client.admin().indices().prepareDelete("test").execute().actionGet();
        } catch (Exception e) {
            // ignore
        }

        client.admin().indices().prepareCreate("test").setSettings(ImmutableSettings.settingsBuilder().put("number_of_shards", 2))
                .addMapping("type1", jsonBuilder().startObject().startObject("type1").startObject("properties")
                        .startObject("title").field("type", "string").field("store", "yes").field("term_vector", "with_positions_offsets").endObject()
                        .endObject().endObject().endObject())
                .execute().actionGet();

        for (int i = 0; i < 5; i++) {
            client.prepareIndex("test", "type1", Integer.toString(i))
                    .setSource("title", "This is a html escaping highlighting test for *&? elasticsearch").setRefresh(true).execute().actionGet();
        }

        SearchResponse search = client.prepareSearch()
                .setQuery(fieldQuery("title", "test"))
                .setHighlighterEncoder("html")
                .addHighlightedField("title", 50, 1, 10)
                .execute().actionGet();


        assertThat(Arrays.toString(search.shardFailures()), search.failedShards(), equalTo(0));

        assertThat(search.hits().totalHits(), equalTo(5l));
        assertThat(search.hits().hits().length, equalTo(5));

        for (SearchHit hit : search.hits()) {
            assertThat(hit.highlightFields().get("title").fragments()[0].string(), equalTo("highlighting <em>test</em> for *&amp;? elasticsearch"));
        }
    }

    @Test
    public void testMultiMapperVectorWithStore() throws Exception {
        client.admin().indices().prepareDelete().execute().actionGet();

        client.admin().indices().prepareCreate("test").setSettings(ImmutableSettings.settingsBuilder().put("number_of_shards", 2))
                .addMapping("type1", jsonBuilder().startObject().startObject("type1").startObject("properties")
                        .startObject("title").field("type", "multi_field").startObject("fields")
                        .startObject("title").field("type", "string").field("store", "yes").field("term_vector", "with_positions_offsets").endObject()
                        .startObject("key").field("type", "string").field("store", "yes").field("term_vector", "with_positions_offsets").field("analyzer", "whitespace").endObject()
                        .endObject().endObject()
                        .endObject().endObject().endObject())
                .execute().actionGet();

        client.admin().cluster().prepareHealth().setWaitForYellowStatus().execute().actionGet();

        client.prepareIndex("test", "type1", "1").setSource("title", "this is a test").execute().actionGet();
        client.admin().indices().prepareRefresh().execute().actionGet();

        // simple search on body with standard analyzer with a simple field query
        SearchResponse search = client.prepareSearch()
                .setQuery(fieldQuery("title", "this is a test"))
                .setHighlighterEncoder("html")
                .addHighlightedField("title", 50, 1)
                .execute().actionGet();
        assertThat(Arrays.toString(search.shardFailures()), search.failedShards(), equalTo(0));

        SearchHit hit = search.hits().getAt(0);
        assertThat(hit.highlightFields().get("title").fragments()[0].string(), equalTo("this is a <em>test</em>"));

        // search on title.key and highlight on title
        search = client.prepareSearch()
                .setQuery(fieldQuery("title.key", "this is a test"))
                .setHighlighterEncoder("html")
                .addHighlightedField("title.key", 50, 1)
                .execute().actionGet();
        assertThat(Arrays.toString(search.shardFailures()), search.failedShards(), equalTo(0));

        hit = search.hits().getAt(0);
        assertThat(hit.highlightFields().get("title.key").fragments()[0].string(), equalTo("<em>this</em> <em>is</em> <em>a</em> <em>test</em>"));
    }

    @Test
    public void testMultiMapperVectorFromSource() throws Exception {
        client.admin().indices().prepareDelete().execute().actionGet();

        client.admin().indices().prepareCreate("test").setSettings(ImmutableSettings.settingsBuilder().put("number_of_shards", 2))
                .addMapping("type1", jsonBuilder().startObject().startObject("type1").startObject("properties")
                        .startObject("title").field("type", "multi_field").startObject("fields")
                        .startObject("title").field("type", "string").field("store", "no").field("term_vector", "with_positions_offsets").endObject()
                        .startObject("key").field("type", "string").field("store", "no").field("term_vector", "with_positions_offsets").field("analyzer", "whitespace").endObject()
                        .endObject().endObject()
                        .endObject().endObject().endObject())
                .execute().actionGet();

        client.admin().cluster().prepareHealth().setWaitForYellowStatus().execute().actionGet();

        client.prepareIndex("test", "type1", "1").setSource("title", "this is a test").execute().actionGet();
        client.admin().indices().prepareRefresh().execute().actionGet();

        // simple search on body with standard analyzer with a simple field query
        SearchResponse search = client.prepareSearch()
                .setQuery(fieldQuery("title", "this is a test"))
                .setHighlighterEncoder("html")
                .addHighlightedField("title", 50, 1)
                .execute().actionGet();
        assertThat(Arrays.toString(search.shardFailures()), search.failedShards(), equalTo(0));

        SearchHit hit = search.hits().getAt(0);
        assertThat(hit.highlightFields().get("title").fragments()[0].string(), equalTo("this is a <em>test</em>"));

        // search on title.key and highlight on title.key
        search = client.prepareSearch()
                .setQuery(fieldQuery("title.key", "this is a test"))
                .setHighlighterEncoder("html")
                .addHighlightedField("title.key", 50, 1)
                .execute().actionGet();
        assertThat(Arrays.toString(search.shardFailures()), search.failedShards(), equalTo(0));

        hit = search.hits().getAt(0);
        assertThat(hit.highlightFields().get("title.key").fragments()[0].string(), equalTo("<em>this</em> <em>is</em> <em>a</em> <em>test</em>"));
    }

    @Test
    public void testMultiMapperNoVectorWithStore() throws Exception {
        client.admin().indices().prepareDelete().execute().actionGet();

        client.admin().indices().prepareCreate("test").setSettings(ImmutableSettings.settingsBuilder().put("number_of_shards", 2))
                .addMapping("type1", jsonBuilder().startObject().startObject("type1").startObject("properties")
                        .startObject("title").field("type", "multi_field").startObject("fields")
                        .startObject("title").field("type", "string").field("store", "yes").field("term_vector", "no").endObject()
                        .startObject("key").field("type", "string").field("store", "yes").field("term_vector", "no").field("analyzer", "whitespace").endObject()
                        .endObject().endObject()
                        .endObject().endObject().endObject())
                .execute().actionGet();

        client.admin().cluster().prepareHealth().setWaitForYellowStatus().execute().actionGet();

        client.prepareIndex("test", "type1", "1").setSource("title", "this is a test").execute().actionGet();
        client.admin().indices().prepareRefresh().execute().actionGet();

        // simple search on body with standard analyzer with a simple field query
        SearchResponse search = client.prepareSearch()
                .setQuery(fieldQuery("title", "this is a test"))
                .setHighlighterEncoder("html")
                .addHighlightedField("title", 50, 1)
                .execute().actionGet();
        assertThat(Arrays.toString(search.shardFailures()), search.failedShards(), equalTo(0));

        SearchHit hit = search.hits().getAt(0);
        assertThat(hit.highlightFields().get("title").fragments()[0].string(), equalTo("this is a <em>test</em>"));

        // search on title.key and highlight on title
        search = client.prepareSearch()
                .setQuery(fieldQuery("title.key", "this is a test"))
                .setHighlighterEncoder("html")
                .addHighlightedField("title.key", 50, 1)
                .execute().actionGet();
        assertThat(Arrays.toString(search.shardFailures()), search.failedShards(), equalTo(0));

        hit = search.hits().getAt(0);
        assertThat(hit.highlightFields().get("title.key").fragments()[0].string(), equalTo("<em>this</em> <em>is</em> <em>a</em> <em>test</em>"));
    }

    @Test
    public void testMultiMapperNoVectorFromSource() throws Exception {
        client.admin().indices().prepareDelete().execute().actionGet();

        client.admin().indices().prepareCreate("test").setSettings(ImmutableSettings.settingsBuilder().put("number_of_shards", 2))
                .addMapping("type1", jsonBuilder().startObject().startObject("type1").startObject("properties")
                        .startObject("title").field("type", "multi_field").startObject("fields")
                        .startObject("title").field("type", "string").field("store", "no").field("term_vector", "no").endObject()
                        .startObject("key").field("type", "string").field("store", "no").field("term_vector", "no").field("analyzer", "whitespace").endObject()
                        .endObject().endObject()
                        .endObject().endObject().endObject())
                .execute().actionGet();

        client.admin().cluster().prepareHealth().setWaitForYellowStatus().execute().actionGet();

        client.prepareIndex("test", "type1", "1").setSource("title", "this is a test").execute().actionGet();
        client.admin().indices().prepareRefresh().execute().actionGet();

        // simple search on body with standard analyzer with a simple field query
        SearchResponse search = client.prepareSearch()
                .setQuery(fieldQuery("title", "this is a test"))
                .setHighlighterEncoder("html")
                .addHighlightedField("title", 50, 1)
                .execute().actionGet();
        assertThat(Arrays.toString(search.shardFailures()), search.failedShards(), equalTo(0));

        SearchHit hit = search.hits().getAt(0);
        assertThat(hit.highlightFields().get("title").fragments()[0].string(), equalTo("this is a <em>test</em>"));

        // search on title.key and highlight on title.key
        search = client.prepareSearch()
                .setQuery(fieldQuery("title.key", "this is a test"))
                .setHighlighterEncoder("html")
                .addHighlightedField("title.key", 50, 1)
                .execute().actionGet();
        assertThat(Arrays.toString(search.shardFailures()), search.failedShards(), equalTo(0));

        hit = search.hits().getAt(0);
        assertThat(hit.highlightFields().get("title.key").fragments()[0].string(), equalTo("<em>this</em> <em>is</em> <em>a</em> <em>test</em>"));
    }
}
