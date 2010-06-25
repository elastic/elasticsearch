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
import org.elasticsearch.common.trove.ExtTIntArrayList;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.node.internal.InternalNode;
import org.elasticsearch.search.Scroll;
import org.elasticsearch.search.SearchContextMissingException;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.controller.SearchPhaseController;
import org.elasticsearch.search.controller.ShardDoc;
import org.elasticsearch.search.dfs.AggregatedDfs;
import org.elasticsearch.search.dfs.DfsSearchResult;
import org.elasticsearch.search.facets.FacetBuilders;
import org.elasticsearch.search.facets.query.QueryFacet;
import org.elasticsearch.search.fetch.FetchSearchRequest;
import org.elasticsearch.search.fetch.FetchSearchResult;
import org.elasticsearch.search.fetch.QueryFetchSearchResult;
import org.elasticsearch.search.internal.InternalSearchRequest;
import org.elasticsearch.search.query.QuerySearchRequest;
import org.elasticsearch.search.query.QuerySearchResult;
import org.elasticsearch.test.integration.AbstractNodesTests;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Map;

import static org.elasticsearch.client.Requests.*;
import static org.elasticsearch.common.collect.Lists.*;
import static org.elasticsearch.index.query.xcontent.QueryBuilders.*;
import static org.elasticsearch.search.builder.SearchSourceBuilder.*;
import static org.hamcrest.MatcherAssert.*;
import static org.hamcrest.Matchers.*;

/**
 * @author kimchy (Shay Banon)
 */
public class SingleInstanceEmbeddedSearchTests extends AbstractNodesTests {

    private SearchService searchService;

    private SearchPhaseController searchPhaseController;

    @BeforeClass public void createNodeAndInitWithData() throws Exception {
        startNode("server1");

        client("server1").admin().indices().create(createIndexRequest("test")).actionGet();
        index(client("server1"), "1", "test1", 1);
        index(client("server1"), "2", "test2", 2);
        index(client("server1"), "3", "test3", 2);
        index(client("server1"), "4", "test4", 2);
        index(client("server1"), "5", "test5", 2);
        client("server1").admin().indices().refresh(refreshRequest("test")).actionGet();

        searchService = ((InternalNode) node("server1")).injector().getInstance(SearchService.class);
        searchPhaseController = ((InternalNode) node("server1")).injector().getInstance(SearchPhaseController.class);
    }

    @AfterClass public void closeNode() {
        closeAllNodes();
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
        assertThat(queryFetchResult.fetchResult().hits().hits()[0].sourceAsString(), equalTo(source("1", "test1", 1)));
        assertThat(queryFetchResult.fetchResult().hits().hits()[0].id(), equalTo("1"));
        assertThat(queryFetchResult.fetchResult().hits().hits()[0].type(), equalTo("type1"));
    }

    @Test public void testQueryThenFetch() throws Exception {
        QuerySearchResult queryResult = searchService.executeQueryPhase(searchRequest(searchSource().query(termQuery("name", "test1"))));
        assertThat(queryResult.topDocs().totalHits, equalTo(1));

        ShardDoc[] sortedShardList = searchPhaseController.sortDocs(newArrayList(queryResult));
        Map<SearchShardTarget, ExtTIntArrayList> docIdsToLoad = searchPhaseController.docIdsToLoad(sortedShardList);
        assertThat(docIdsToLoad.size(), equalTo(1));
        assertThat(docIdsToLoad.values().iterator().next().size(), equalTo(1));

        FetchSearchResult fetchResult = searchService.executeFetchPhase(new FetchSearchRequest(queryResult.id(), docIdsToLoad.values().iterator().next()));
        assertThat(fetchResult.hits().hits()[0].sourceAsString(), equalTo(source("1", "test1", 1)));
        assertThat(fetchResult.hits().hits()[0].id(), equalTo("1"));
        assertThat(fetchResult.hits().hits()[0].type(), equalTo("type1"));
    }

    @Test public void testQueryAndFetch() throws Exception {
        QueryFetchSearchResult result = searchService.executeFetchPhase(searchRequest(searchSource().query(termQuery("name", "test1"))));
        FetchSearchResult fetchResult = result.fetchResult();
        assertThat(fetchResult.hits().hits()[0].sourceAsString(), equalTo(source("1", "test1", 1)));
        assertThat(fetchResult.hits().hits()[0].id(), equalTo("1"));
        assertThat(fetchResult.hits().hits()[0].type(), equalTo("type1"));
    }

    @Test public void testDfsQueryThenFetch() throws Exception {
        DfsSearchResult dfsResult = searchService.executeDfsPhase(searchRequest(searchSource().query(termQuery("name", "test1"))));
        AggregatedDfs dfs = searchPhaseController.aggregateDfs(newArrayList(dfsResult));

        QuerySearchResult queryResult = searchService.executeQueryPhase(new QuerySearchRequest(dfsResult.id(), dfs));
        assertThat(queryResult.topDocs().totalHits, equalTo(1));

        ShardDoc[] sortedShardList = searchPhaseController.sortDocs(newArrayList(queryResult));
        Map<SearchShardTarget, ExtTIntArrayList> docIdsToLoad = searchPhaseController.docIdsToLoad(sortedShardList);
        assertThat(docIdsToLoad.size(), equalTo(1));
        assertThat(docIdsToLoad.values().iterator().next().size(), equalTo(1));

        FetchSearchResult fetchResult = searchService.executeFetchPhase(new FetchSearchRequest(queryResult.id(), docIdsToLoad.values().iterator().next()));
        assertThat(fetchResult.hits().hits()[0].sourceAsString(), equalTo(source("1", "test1", 1)));
        assertThat(fetchResult.hits().hits()[0].id(), equalTo("1"));
        assertThat(fetchResult.hits().hits()[0].type(), equalTo("type1"));
    }

    @Test public void testSimpleQueryFacets() throws Exception {
        QuerySearchResult queryResult = searchService.executeQueryPhase(searchRequest(
                searchSource().query(wildcardQuery("name", "te*"))
                        .facet(FacetBuilders.queryFacet("age1", termQuery("age", 1)))
                        .facet(FacetBuilders.queryFacet("age2", termQuery("age", 2)))
        ));
        assertThat(queryResult.facets().facet(QueryFacet.class, "age2").count(), equalTo(4l));
        assertThat(queryResult.facets().facet(QueryFacet.class, "age1").count(), equalTo(1l));
    }

    @Test public void testQueryFetchKeepAliveTimeout() throws Exception {
        QuerySearchResult queryResult = searchService.executeQueryPhase(searchRequest(searchSource().query(termQuery("name", "test1"))).scroll(new Scroll(TimeValue.timeValueMillis(10))));
        assertThat(queryResult.topDocs().totalHits, equalTo(1));

        ShardDoc[] sortedShardList = searchPhaseController.sortDocs(newArrayList(queryResult));
        Map<SearchShardTarget, ExtTIntArrayList> docIdsToLoad = searchPhaseController.docIdsToLoad(sortedShardList);
        assertThat(docIdsToLoad.size(), equalTo(1));
        assertThat(docIdsToLoad.values().iterator().next().size(), equalTo(1));

        // sleep more than the 100ms the timeout wheel it set to
        Thread.sleep(300);

        try {
            searchService.executeFetchPhase(new FetchSearchRequest(queryResult.id(), docIdsToLoad.values().iterator().next()));
            assert true : "context should be missing since it timed out";
        } catch (SearchContextMissingException e) {
            // all is well
        }
    }


    private InternalSearchRequest searchRequest(SearchSourceBuilder builder) {
        return new InternalSearchRequest("test", 0).source(builder.buildAsBytes());
    }

    private void index(Client client, String id, String nameValue, int age) {
        client.index(indexRequest("test").type("type1").id(id).source(source(id, nameValue, age))).actionGet();
    }

    private String source(String id, String nameValue, int age) {
        return "{ type1 : { \"id\" : \"" + id + "\", \"name\" : \"" + nameValue + "\", age : " + age + " } }";
    }
}
