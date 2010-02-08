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

package org.elasticsearch.test.integration.client.transport;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.search.Scroll;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.server.internal.InternalServer;
import org.elasticsearch.test.integration.AbstractServersTests;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.util.transport.TransportAddress;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.elasticsearch.action.search.SearchType.*;
import static org.elasticsearch.client.Requests.*;
import static org.elasticsearch.index.query.json.JsonQueryBuilders.*;
import static org.elasticsearch.search.builder.SearchSourceBuilder.*;
import static org.elasticsearch.util.TimeValue.*;
import static org.hamcrest.MatcherAssert.*;
import static org.hamcrest.Matchers.*;

/**
 * @author kimchy (Shay Banon)
 */
public class ClientTransportTwoServersSearchTests extends AbstractServersTests {

    private TransportClient client;

    @BeforeClass public void createServers() throws Exception {
        startServer("server1");
        startServer("server2");

        TransportAddress server1Address = ((InternalServer) server("server1")).injector().getInstance(TransportService.class).boundAddress().publishAddress();
        client = new TransportClient();
        client.addTransportAddress(server1Address);


        client.admin().indices().create(createIndexRequest("test")).actionGet();

        for (int i = 0; i < 100; i++) {
            index(client, Integer.toString(i), "test", i);
        }
        client.admin().indices().refresh(refreshRequest("test")).actionGet();
    }

    @AfterClass public void closeServers() {
        closeAllServers();
        if (client != null) {
            client.close();
        }
    }

    @Test public void testDfsQueryThenFetch() throws Exception {
        SearchSourceBuilder source = searchSource()
                .query(termQuery("multi", "test"))
                .from(0).size(60).explain(true);

        SearchResponse searchResponse = client.search(searchRequest("test").source(source).searchType(DFS_QUERY_THEN_FETCH).scroll(new Scroll(timeValueMinutes(10)))).actionGet();

        assertThat(searchResponse.hits().totalHits(), equalTo(100l));
        assertThat(searchResponse.hits().hits().length, equalTo(60));
        for (int i = 0; i < 60; i++) {
            SearchHit hit = searchResponse.hits().hits()[i];
//            System.out.println(hit.target() + ": " +  hit.explanation());
            assertThat("id[" + hit.id() + "]", hit.id(), equalTo(Integer.toString(100 - i - 1)));
        }

        searchResponse = client.searchScroll(searchScrollRequest(searchResponse.scrollId())).actionGet();

        assertThat(searchResponse.hits().totalHits(), equalTo(100l));
        assertThat(searchResponse.hits().hits().length, equalTo(40));
        for (int i = 0; i < 40; i++) {
            SearchHit hit = searchResponse.hits().hits()[i];
            assertThat("id[" + hit.id() + "]", hit.id(), equalTo(Integer.toString(100 - 60 - 1 - i)));
        }
    }

    //

    @Test public void testDfsQueryThenFetchWithSort() throws Exception {
        SearchSourceBuilder source = searchSource()
                .query(termQuery("multi", "test"))
                .from(0).size(60).explain(true).sort("age", false);

        SearchResponse searchResponse = client.search(searchRequest("test").source(source).searchType(DFS_QUERY_THEN_FETCH).scroll(new Scroll(timeValueMinutes(10)))).actionGet();
        assertThat(searchResponse.hits().totalHits(), equalTo(100l));
        assertThat(searchResponse.hits().hits().length, equalTo(60));
        for (int i = 0; i < 60; i++) {
            SearchHit hit = searchResponse.hits().hits()[i];
//            System.out.println(hit.target() + ": " +  hit.explanation());
            assertThat("id[" + hit.id() + "]", hit.id(), equalTo(Integer.toString(i)));
        }

        searchResponse = client.searchScroll(searchScrollRequest(searchResponse.scrollId())).actionGet();

        assertThat(searchResponse.hits().totalHits(), equalTo(100l));
        assertThat(searchResponse.hits().hits().length, equalTo(40));
        for (int i = 0; i < 40; i++) {
            SearchHit hit = searchResponse.hits().hits()[i];
            assertThat("id[" + hit.id() + "]", hit.id(), equalTo(Integer.toString(i + 60)));
        }
    }

    @Test public void testQueryThenFetch() throws Exception {
        SearchSourceBuilder source = searchSource()
                .query(termQuery("multi", "test"))
                .from(0).size(60).explain(true);

        SearchResponse searchResponse = client.search(searchRequest("test").source(source).searchType(QUERY_THEN_FETCH).scroll(new Scroll(timeValueMinutes(10)))).actionGet();
        assertThat(searchResponse.hits().totalHits(), equalTo(100l));
        assertThat(searchResponse.hits().hits().length, equalTo(60));
        for (int i = 0; i < 60; i++) {
            SearchHit hit = searchResponse.hits().hits()[i];
//            System.out.println(hit.target() + ": " +  hit.explanation());
            assertThat("id[" + hit.id() + "]", hit.id(), equalTo(Integer.toString(100 - i - 1)));
        }

        searchResponse = client.searchScroll(searchScrollRequest(searchResponse.scrollId())).actionGet();

        assertThat(searchResponse.hits().totalHits(), equalTo(100l));
        assertThat(searchResponse.hits().hits().length, equalTo(40));
        for (int i = 0; i < 40; i++) {
            SearchHit hit = searchResponse.hits().hits()[i];
            assertThat("id[" + hit.id() + "]", hit.id(), equalTo(Integer.toString(100 - 60 - 1 - i)));
        }
    }

    @Test public void testQueryThenFetchWithSort() throws Exception {
        SearchSourceBuilder source = searchSource()
                .query(termQuery("multi", "test"))
                .from(0).size(60).explain(true).sort("age", false);

        SearchResponse searchResponse = client.search(searchRequest("test").source(source).searchType(QUERY_THEN_FETCH).scroll(new Scroll(timeValueMinutes(10)))).actionGet();
        assertThat(searchResponse.hits().totalHits(), equalTo(100l));
        assertThat(searchResponse.hits().hits().length, equalTo(60));
        for (int i = 0; i < 60; i++) {
            SearchHit hit = searchResponse.hits().hits()[i];
//            System.out.println(hit.target() + ": " +  hit.explanation());
            assertThat("id[" + hit.id() + "]", hit.id(), equalTo(Integer.toString(i)));
        }

        searchResponse = client.searchScroll(searchScrollRequest(searchResponse.scrollId())).actionGet();

        assertThat(searchResponse.hits().totalHits(), equalTo(100l));
        assertThat(searchResponse.hits().hits().length, equalTo(40));
        for (int i = 0; i < 40; i++) {
            SearchHit hit = searchResponse.hits().hits()[i];
            assertThat("id[" + hit.id() + "]", hit.id(), equalTo(Integer.toString(i + 60)));
        }
    }

    @Test public void testQueryAndFetch() throws Exception {
        SearchSourceBuilder source = searchSource()
                .query(termQuery("multi", "test"))
                .from(0).size(20).explain(true);

        SearchResponse searchResponse = client.search(searchRequest("test").source(source).searchType(QUERY_AND_FETCH).scroll(new Scroll(timeValueMinutes(10)))).actionGet();
        assertThat(searchResponse.hits().totalHits(), equalTo(100l));
        assertThat(searchResponse.hits().hits().length, equalTo(60)); // 20 per shard
        for (int i = 0; i < 60; i++) {
            SearchHit hit = searchResponse.hits().hits()[i];
//            System.out.println(hit.target() + ": " +  hit.explanation());
            assertThat("id[" + hit.id() + "]", hit.id(), equalTo(Integer.toString(100 - i - 1)));
        }

        // TODO support scrolling
//        searchResponse = searchScrollAction.submit(new SearchScrollRequest(searchResponse.scrollId())).actionGet();
//
//        assertEquals(100, searchResponse.hits().totalHits());
//        assertEquals(40, searchResponse.hits().hits().length);
//        for (int i = 0; i < 40; i++) {
//            SearchHit hit = searchResponse.hits().hits()[i];
//            assertEquals("id[" + hit.id() + "]", Integer.toString(100 - 60 - 1 - i), hit.id());
//        }
    }

    @Test public void testDfsQueryAndFetch() throws Exception {
        SearchSourceBuilder source = searchSource()
                .query(termQuery("multi", "test"))
                .from(0).size(20).explain(true);

        SearchResponse searchResponse = client.search(searchRequest("test").source(source).searchType(DFS_QUERY_AND_FETCH).scroll(new Scroll(timeValueMinutes(10)))).actionGet();
        assertThat(searchResponse.hits().totalHits(), equalTo(100l));
        assertThat(searchResponse.hits().hits().length, equalTo(60)); // 20 per shard
        for (int i = 0; i < 60; i++) {
            SearchHit hit = searchResponse.hits().hits()[i];
//            System.out.println(hit.target() + ": " +  hit.explanation());
            assertThat("id[" + hit.id() + "]", hit.id(), equalTo(Integer.toString(100 - i - 1)));
        }

        // TODO support scrolling
//        searchResponse = searchScrollAction.submit(new SearchScrollRequest(searchResponse.scrollId())).actionGet();
//
//        assertEquals(100, searchResponse.hits().totalHits());
//        assertEquals(40, searchResponse.hits().hits().length);
//        for (int i = 0; i < 40; i++) {
//            SearchHit hit = searchResponse.hits().hits()[i];
//            assertEquals("id[" + hit.id() + "]", Integer.toString(100 - 60 - 1 - i), hit.id());
//        }
    }


    private void index(Client client, String id, String nameValue, int age) {
        client.index(Requests.indexRequest("test").type("type1").id(id).source(source(id, nameValue, age))).actionGet();
    }

    private String source(String id, String nameValue, int age) {
        StringBuilder multi = new StringBuilder().append(nameValue);
        for (int i = 0; i < age; i++) {
            multi.append(" ").append(nameValue);
        }
        return "{ type1 : { \"id\" : \"" + id + "\", \"name\" : \"" + (nameValue + id) + "\", age : " + age + ", multi : \"" + multi.toString() + "\", _boost : " + (age * 10) + " } }";
    }
}