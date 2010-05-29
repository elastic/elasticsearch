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

package org.elasticsearch.test.integration.search.highlight;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.test.integration.AbstractNodesTests;
import org.elasticsearch.util.xcontent.XContentFactory;
import org.elasticsearch.util.xcontent.builder.XContentBuilder;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.Arrays;

import static org.elasticsearch.action.search.SearchType.*;
import static org.elasticsearch.client.Requests.*;
import static org.elasticsearch.index.query.xcontent.QueryBuilders.*;
import static org.elasticsearch.search.builder.SearchSourceBuilder.*;
import static org.elasticsearch.util.TimeValue.*;
import static org.elasticsearch.util.xcontent.XContentFactory.*;
import static org.hamcrest.MatcherAssert.*;
import static org.hamcrest.Matchers.*;

/**
 * @author kimchy (shay.banon)
 */
public class HighlightSearchTests extends AbstractNodesTests {

    private Client client;

    @BeforeClass public void createNodes() throws Exception {
        startNode("server1");
        startNode("server2");
        client = getClient();

        client.admin().indices().create(createIndexRequest("test")).actionGet();

        logger.info("Update mapping (_all to store and have term vectors)");
        client.admin().indices().putMapping(putMappingRequest("test").source(mapping())).actionGet();

        for (int i = 0; i < 100; i++) {
            index(client("server1"), Integer.toString(i), "test", i);
        }
        client.admin().indices().refresh(refreshRequest("test")).actionGet();
    }

    @AfterClass public void closeNodes() {
        client.close();
        closeAllNodes();
    }

    protected Client getClient() {
        return client("server1");
    }

    @Test public void testSimpleHighlighting() throws Exception {
        SearchResponse searchResponse = client.prepareSearch()
                .setIndices("test")
                .setSearchType(QUERY_THEN_FETCH)
                .setQuery(termQuery("_all", "test"))
                .setFrom(0).setSize(60)
                .addHighlightedField("_all").setHighlighterOrder("score").setHighlighterPreTags("<xxx>").setHighlighterPostTags("</xxx>")
                .setScroll(timeValueMinutes(10))
                .execute().actionGet();

        assertThat("Failures " + Arrays.toString(searchResponse.shardFailures()), searchResponse.shardFailures().length, equalTo(0));
        assertThat(searchResponse.hits().totalHits(), equalTo(100l));
        assertThat(searchResponse.hits().hits().length, equalTo(60));
        for (int i = 0; i < 60; i++) {
            SearchHit hit = searchResponse.hits().hits()[i];
//            System.out.println(hit.target() + ": " +  hit.explanation());
            assertThat("id[" + hit.id() + "]", hit.id(), equalTo(Integer.toString(100 - i - 1)));
//            System.out.println(hit.shard() + ": " + hit.highlightFields());
            assertThat(hit.highlightFields().size(), equalTo(1));
            assertThat(hit.highlightFields().get("_all").fragments().length, greaterThan(0));
        }

        searchResponse = client.prepareSearchScroll(searchResponse.scrollId()).execute().actionGet();

        assertThat(searchResponse.hits().totalHits(), equalTo(100l));
        assertThat(searchResponse.hits().hits().length, equalTo(40));
        for (int i = 0; i < 40; i++) {
            SearchHit hit = searchResponse.hits().hits()[i];
            assertThat("id[" + hit.id() + "]", hit.id(), equalTo(Integer.toString(100 - 60 - 1 - i)));
        }
    }

    @Test public void testPrefixHighlightingOnSpecificField() throws Exception {
        SearchSourceBuilder source = searchSource()
                .query(prefixQuery("multi", "te"))
                .from(0).size(60).explain(true)
                .highlight(highlight().field("_all").order("score").preTags("<xxx>").postTags("</xxx>"));

        SearchResponse searchResponse = client.search(searchRequest("test").source(source).searchType(QUERY_THEN_FETCH).scroll(timeValueMinutes(10))).actionGet();
        assertThat("Failures " + Arrays.toString(searchResponse.shardFailures()), searchResponse.shardFailures().length, equalTo(0));
        assertThat(searchResponse.hits().totalHits(), equalTo(100l));
        assertThat(searchResponse.hits().hits().length, equalTo(60));
        for (int i = 0; i < 60; i++) {
            SearchHit hit = searchResponse.hits().hits()[i];
//            System.out.println(hit.target() + ": " +  hit.explanation());
//            assertThat("id[" + hit.id() + "]", hit.id(), equalTo(Integer.toString(100 - i - 1)));
//            System.out.println(hit.shard() + ": " + hit.highlightFields());
            assertThat(hit.highlightFields().size(), equalTo(1));
            assertThat(hit.highlightFields().get("_all").fragments().length, greaterThan(0));
        }
    }

    @Test public void testPrefixHighlightingOnAllField() throws Exception {
        SearchSourceBuilder source = searchSource()
                .query(prefixQuery("_all", "te"))
                .from(0).size(60).explain(true)
                .highlight(highlight().field("_all").order("score").preTags("<xxx>").postTags("</xxx>"));

        SearchResponse searchResponse = client.search(searchRequest("test").source(source).searchType(QUERY_THEN_FETCH).scroll(timeValueMinutes(10))).actionGet();
        assertThat("Failures " + Arrays.toString(searchResponse.shardFailures()), searchResponse.shardFailures().length, equalTo(0));
        assertThat(searchResponse.hits().totalHits(), equalTo(100l));
        assertThat(searchResponse.hits().hits().length, equalTo(60));
        for (int i = 0; i < 60; i++) {
            SearchHit hit = searchResponse.hits().hits()[i];
//            System.out.println(hit.target() + ": " +  hit.explanation());
//            assertThat("id[" + hit.id() + "]", hit.id(), equalTo(Integer.toString(100 - i - 1)));
//            System.out.println(hit.shard() + ": " + hit.highlightFields());
            assertThat(hit.highlightFields().size(), equalTo(1));
            assertThat(hit.highlightFields().get("_all").fragments().length, greaterThan(0));
        }
    }

    private void index(Client client, String id, String nameValue, int age) throws IOException {
        client.index(Requests.indexRequest("test").type("type1").id(id).source(source(id, nameValue, age))).actionGet();
    }

    public XContentBuilder mapping() throws IOException {
        return XContentFactory.jsonBuilder().startObject().startObject("type1")
                .startObject("_all").field("store", "yes").field("termVector", "with_positions_offsets").endObject()
                .endObject().endObject();
    }

    private XContentBuilder source(String id, String nameValue, int age) throws IOException {
        StringBuilder multi = new StringBuilder().append(nameValue);
        for (int i = 0; i < age; i++) {
            multi.append(" ").append(nameValue);
        }
        return jsonBuilder().startObject()
                .field("id", id)
                .field("name", nameValue + id)
                .field("age", age)
                .field("multi", multi.toString())
                .field("_boost", age * 10)
                .endObject();
    }
}