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

package org.elasticsearch.test.integration.search;

import org.elasticsearch.client.Client;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.controller.SearchPhaseController;
import org.elasticsearch.search.controller.ShardDoc;
import org.elasticsearch.search.dfs.AggregatedDfs;
import org.elasticsearch.search.dfs.DfsSearchResult;
import org.elasticsearch.search.fetch.FetchSearchRequest;
import org.elasticsearch.search.fetch.FetchSearchResult;
import org.elasticsearch.search.fetch.QueryFetchSearchResult;
import org.elasticsearch.search.internal.InternalSearchRequest;
import org.elasticsearch.search.query.QuerySearchRequest;
import org.elasticsearch.search.query.QuerySearchResult;
import org.elasticsearch.server.internal.InternalServer;
import org.elasticsearch.test.integration.AbstractServersTests;
import org.elasticsearch.util.trove.ExtTIntArrayList;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Map;

import static com.google.common.collect.Lists.*;
import static org.elasticsearch.client.Requests.*;
import static org.elasticsearch.index.query.json.JsonQueryBuilders.*;
import static org.elasticsearch.search.builder.SearchSourceBuilder.*;
import static org.hamcrest.MatcherAssert.*;
import static org.hamcrest.Matchers.*;

/**
 * @author kimchy (Shay Banon)
 */
public class SingleInstanceEmbeddedSearchTests extends AbstractServersTests {

    private SearchService searchService;

    private SearchPhaseController searchPhaseController;

    @BeforeClass public void createServerAndInitWithData() throws Exception {
        startServer("server1");

        client("server1").admin().indices().create(createIndexRequest("test")).actionGet();
        index(client("server1"), "1", "test1", 1);
        index(client("server1"), "2", "test2", 2);
        index(client("server1"), "3", "test3", 2);
        index(client("server1"), "4", "test4", 2);
        index(client("server1"), "5", "test5", 2);
        client("server1").admin().indices().refresh(refreshRequest("test")).actionGet();

        searchService = ((InternalServer) server("server1")).injector().getInstance(SearchService.class);
        searchPhaseController = ((InternalServer) server("server1")).injector().getInstance(SearchPhaseController.class);
    }

    @AfterClass public void closeServer() {
        closeAllServers();
    }

    @Test public void testDirectDfs() throws Exception {
        DfsSearchResult dfsResult = searchService.executeDfsPhase(searchRequest(searchSource().query(termQuery("name", "test1"))));

        assertThat(dfsResult.terms().length, equalTo(1));
        assertThat(dfsResult.freqs().length, equalTo(1));
        assertThat(dfsResult.terms()[0].field(), equalTo("name"));
        assertThat(dfsResult.terms()[0].text(), equalTo("test1"));
        assertThat(dfsResult.freqs()[0], equalTo(1));
    }

    @Test public void testDirectQuery() throws Exception {
        QuerySearchResult queryResult = searchService.executeQueryPhase(searchRequest(searchSource().query(termQuery("name", "test1"))));
        assertThat(queryResult.topDocs().totalHits, equalTo(1));
    }

    @Test public void testDirectFetch() throws Exception {
        QueryFetchSearchResult queryFetchResult = searchService.executeFetchPhase(searchRequest(searchSource().query(termQuery("name", "test1"))));
        assertThat(queryFetchResult.queryResult().topDocs().totalHits, equalTo(1));
        assertThat(queryFetchResult.fetchResult().hits().hits().length, equalTo(1));
        assertThat(queryFetchResult.fetchResult().hits().hits()[0].source(), equalTo(source("1", "test1", 1)));
        assertThat(queryFetchResult.fetchResult().hits().hits()[0].id(), equalTo("1"));
        assertThat(queryFetchResult.fetchResult().hits().hits()[0].type(), equalTo("type1"));
    }

    @Test public void testQueryFetch() throws Exception {
        QuerySearchResult queryResult = searchService.executeQueryPhase(searchRequest(searchSource().query(termQuery("name", "test1"))));
        assertThat(queryResult.topDocs().totalHits, equalTo(1));

        ShardDoc[] sortedShardList = searchPhaseController.sortDocs(newArrayList(queryResult));
        Map<SearchShardTarget, ExtTIntArrayList> docIdsToLoad = searchPhaseController.docIdsToLoad(sortedShardList);
        assertThat(docIdsToLoad.size(), equalTo(1));
        assertThat(docIdsToLoad.values().iterator().next().size(), equalTo(1));

        FetchSearchResult fetchResult = searchService.executeFetchPhase(new FetchSearchRequest(queryResult.id(), docIdsToLoad.values().iterator().next()));
        assertThat(fetchResult.hits().hits()[0].source(), equalTo(source("1", "test1", 1)));
        assertThat(fetchResult.hits().hits()[0].id(), equalTo("1"));
        assertThat(fetchResult.hits().hits()[0].type(), equalTo("type1"));
    }

    @Test public void testQueryFetchInOneGo() throws Exception {
        QueryFetchSearchResult result = searchService.executeFetchPhase(searchRequest(searchSource().query(termQuery("name", "test1"))));
        FetchSearchResult fetchResult = result.fetchResult();
        assertThat(fetchResult.hits().hits()[0].source(), equalTo(source("1", "test1", 1)));
        assertThat(fetchResult.hits().hits()[0].id(), equalTo("1"));
        assertThat(fetchResult.hits().hits()[0].type(), equalTo("type1"));
    }

    @Test public void testDfsQueryFetch() throws Exception {
        DfsSearchResult dfsResult = searchService.executeDfsPhase(searchRequest(searchSource().query(termQuery("name", "test1"))));
        AggregatedDfs dfs = searchPhaseController.aggregateDfs(newArrayList(dfsResult));

        QuerySearchResult queryResult = searchService.executeQueryPhase(new QuerySearchRequest(dfsResult.id(), dfs));
        assertThat(queryResult.topDocs().totalHits, equalTo(1));

        ShardDoc[] sortedShardList = searchPhaseController.sortDocs(newArrayList(queryResult));
        Map<SearchShardTarget, ExtTIntArrayList> docIdsToLoad = searchPhaseController.docIdsToLoad(sortedShardList);
        assertThat(docIdsToLoad.size(), equalTo(1));
        assertThat(docIdsToLoad.values().iterator().next().size(), equalTo(1));

        FetchSearchResult fetchResult = searchService.executeFetchPhase(new FetchSearchRequest(queryResult.id(), docIdsToLoad.values().iterator().next()));
        assertThat(fetchResult.hits().hits()[0].source(), equalTo(source("1", "test1", 1)));
        assertThat(fetchResult.hits().hits()[0].id(), equalTo("1"));
        assertThat(fetchResult.hits().hits()[0].type(), equalTo("type1"));
    }

    @Test public void testSimpleQueryFacetsNoExecutionType() throws Exception {
        QuerySearchResult queryResult = searchService.executeQueryPhase(searchRequest(
                searchSource().query(wildcardQuery("name", "te*"))
                        .facets(facets().facet("age2", termQuery("age", 2)).facet("age1", termQuery("age", 1)))
        ));
        assertThat(queryResult.facets().countFacet("age2").count(), equalTo(4l));
        assertThat(queryResult.facets().countFacet("age1").count(), equalTo(1l));
    }

    @Test public void testSimpleQueryFacetsQueryExecutionCollect() throws Exception {
        QuerySearchResult queryResult = searchService.executeQueryPhase(searchRequest(
                searchSource().query(wildcardQuery("name", "te*"))
                        .facets(facets().queryExecution("collect").facet("age2", termQuery("age", 2)).facet("age1", termQuery("age", 1)))
        ));
        assertThat(queryResult.facets().countFacet("age2").count(), equalTo(4l));
        assertThat(queryResult.facets().countFacet("age1").count(), equalTo(1l));
    }

    @Test public void testSimpleQueryFacetsQueryExecutionIdset() throws Exception {
        QuerySearchResult queryResult = searchService.executeQueryPhase(searchRequest(
                searchSource().query(wildcardQuery("name", "te*"))
                        .facets(facets().queryExecution("idset").facet("age2", termQuery("age", 2)).facet("age1", termQuery("age", 1)))
        ));
        assertThat(queryResult.facets().countFacet("age2").count(), equalTo(4l));
        assertThat(queryResult.facets().countFacet("age1").count(), equalTo(1l));
    }

    private InternalSearchRequest searchRequest(SearchSourceBuilder builder) {
        return new InternalSearchRequest("test", 0, builder.build());
    }

    private void index(Client client, String id, String nameValue, int age) {
        client.index(indexRequest("test").type("type1").id(id).source(source(id, nameValue, age))).actionGet();
    }

    private String source(String id, String nameValue, int age) {
        return "{ type1 : { \"id\" : \"" + id + "\", \"name\" : \"" + nameValue + "\", age : " + age + " } }";
    }
}
