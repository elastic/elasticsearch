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

package org.elasticsearch.test.integration.search.embedded;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.trove.ExtTIntArrayList;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.node.internal.InternalNode;
import org.elasticsearch.search.*;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.controller.SearchPhaseController;
import org.elasticsearch.search.controller.ShardDoc;
import org.elasticsearch.search.dfs.AggregatedDfs;
import org.elasticsearch.search.dfs.DfsSearchResult;
import org.elasticsearch.search.fetch.FetchSearchRequest;
import org.elasticsearch.search.fetch.FetchSearchResult;
import org.elasticsearch.search.fetch.QueryFetchSearchResult;
import org.elasticsearch.search.internal.InternalScrollSearchRequest;
import org.elasticsearch.search.internal.InternalSearchRequest;
import org.elasticsearch.search.internal.InternalSearchResponse;
import org.elasticsearch.search.query.QuerySearchRequest;
import org.elasticsearch.search.query.QuerySearchResult;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.test.integration.AbstractNodesTests;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.Set;

import static com.google.common.collect.Lists.newArrayList;
import static org.elasticsearch.client.Requests.indexRequest;
import static org.elasticsearch.client.Requests.refreshRequest;
import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.elasticsearch.search.builder.SearchSourceBuilder.searchSource;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

/**
 *
 */
public class SingleShardEmbeddedSearchTests extends AbstractNodesTests {

    private SearchService searchService;

    private SearchPhaseController searchPhaseController;

    private Client client;

    protected boolean optimizeSingleShard() {
        return true;
    }

    @BeforeClass
    public void createNodeAndInitWithData() throws Exception {
        putDefaultSettings(settingsBuilder().put("search.controller.optimize_single_shard", optimizeSingleShard()));
        startNode("server1");
        client = client("server1");

        client.admin().indices().prepareCreate("test")
                .setSettings(settingsBuilder().put("number_of_shards", 1).put("number_of_replicas", 0))
                .execute().actionGet();
        client("server1").admin().cluster().prepareHealth().setWaitForGreenStatus().execute().actionGet();
        index("1", "test1", 1);
        index("2", "test2", 2);
        index("3", "test3", 3);
        index("4", "test4", 4);
        index("5", "test5", 5);
        client.admin().indices().refresh(refreshRequest("test")).actionGet();

        searchService = ((InternalNode) node("server1")).injector().getInstance(SearchService.class);
        searchPhaseController = ((InternalNode) node("server1")).injector().getInstance(SearchPhaseController.class);
    }

    @AfterClass
    public void closeNode() {
        closeAllNodes();
    }

    @Test
    public void verifyOptimizeSingleShardSetting() {
        assertThat(searchPhaseController.optimizeSingleShard(), equalTo(optimizeSingleShard()));
    }

    @Test
    public void testDirectDfs() throws Exception {
        DfsSearchResult dfsResult = searchService.executeDfsPhase(searchRequest(searchSource().query(termQuery("name", "test1")), SearchType.DFS_QUERY_THEN_FETCH));

        assertThat(dfsResult.terms().length, equalTo(1));
        assertThat(dfsResult.freqs().length, equalTo(1));
        assertThat(dfsResult.terms()[0].field(), equalTo("name"));
        assertThat(dfsResult.terms()[0].text(), equalTo("test1"));
        assertThat(dfsResult.freqs()[0], equalTo(1));
    }

    @Test
    public void testDirectQuery() throws Exception {
        QuerySearchResult queryResult = searchService.executeQueryPhase(searchRequest(searchSource().query(termQuery("name", "test1")), SearchType.QUERY_THEN_FETCH));
        assertThat(queryResult.topDocs().totalHits, equalTo(1));
    }

    @Test
    public void testDirectFetch() throws Exception {
        QueryFetchSearchResult queryFetchResult = searchService.executeFetchPhase(searchRequest(searchSource().query(termQuery("name", "test1")), SearchType.QUERY_AND_FETCH));
        assertThat(queryFetchResult.queryResult().topDocs().totalHits, equalTo(1));
        assertThat(queryFetchResult.fetchResult().hits().hits().length, equalTo(1));
        assertThat(queryFetchResult.fetchResult().hits().hits()[0].sourceAsString(), equalTo(source("1", "test1", 1)));
        assertThat(queryFetchResult.fetchResult().hits().hits()[0].id(), equalTo("1"));
        assertThat(queryFetchResult.fetchResult().hits().hits()[0].type(), equalTo("type1"));
    }

    @Test
    public void testScan() throws Exception {
        Scroll scroll = new Scroll(TimeValue.timeValueMillis(500));
        QuerySearchResult scanResult = searchService.executeScan(searchRequest(searchSource().query(matchAllQuery()).size(2), SearchType.SCAN).scroll(scroll));
        assertThat(scanResult.queryResult().topDocs().totalHits, equalTo(5));

        Set<String> idsLoaded = Sets.newHashSet();
        // start scrolling
        FetchSearchResult fetchResult = searchService.executeScan(new InternalScrollSearchRequest(scanResult.id()).scroll(scroll)).result().fetchResult();
        assertThat(fetchResult.hits().hits().length, equalTo(2));
        for (SearchHit hit : fetchResult.hits()) {
            idsLoaded.add(hit.id());
        }
        // and again...
        fetchResult = searchService.executeScan(new InternalScrollSearchRequest(scanResult.id()).scroll(scroll)).result().fetchResult();
        assertThat(fetchResult.hits().hits().length, equalTo(2));
        for (SearchHit hit : fetchResult.hits()) {
            idsLoaded.add(hit.id());
        }

        fetchResult = searchService.executeScan(new InternalScrollSearchRequest(scanResult.id()).scroll(scroll)).result().fetchResult();
        assertThat(fetchResult.hits().hits().length, equalTo(1));
        for (SearchHit hit : fetchResult.hits()) {
            idsLoaded.add(hit.id());
        }


        Set<String> expectedIds = Sets.newHashSet("1", "2", "3", "4", "5");
        assertThat(idsLoaded, equalTo(expectedIds));

        // do another one for fun, should be expired context
        try {
            searchService.executeScan(new InternalScrollSearchRequest(scanResult.id()).scroll(scroll)).result().fetchResult();
            assert false;
        } catch (SearchContextMissingException e) {
            // ignore
        }
    }

    @Test
    public void testQueryThenFetch() throws Exception {
        QuerySearchResult queryResult = searchService.executeQueryPhase(searchRequest(searchSource().query(termQuery("name", "test1")), SearchType.QUERY_THEN_FETCH));
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

    @Test
    public void testQueryThenFetchIterateWithFrom() throws Exception {
        QuerySearchResult queryResult = searchService.executeQueryPhase(searchRequest(searchSource().query(matchAllQuery()).from(0).size(2), SearchType.QUERY_THEN_FETCH));
        assertThat(queryResult.topDocs().totalHits, equalTo(5));

        Set<String> idsLoaded = Sets.newHashSet();

        ShardDoc[] sortedShardList = searchPhaseController.sortDocs(newArrayList(queryResult));
        Map<SearchShardTarget, ExtTIntArrayList> docIdsToLoad = searchPhaseController.docIdsToLoad(sortedShardList);
        assertThat(docIdsToLoad.size(), equalTo(1));
        assertThat(docIdsToLoad.values().iterator().next().size(), equalTo(2));

        FetchSearchResult fetchResult = searchService.executeFetchPhase(new FetchSearchRequest(queryResult.id(), docIdsToLoad.values().iterator().next()));
        assertThat(fetchResult.hits().hits().length, equalTo(2));
        for (SearchHit hit : fetchResult.hits()) {
            idsLoaded.add(hit.id());
        }

        // iterate to the next 2
        queryResult = searchService.executeQueryPhase(searchRequest(searchSource().query(matchAllQuery()).from(2).size(2), SearchType.QUERY_THEN_FETCH));
        assertThat(queryResult.topDocs().totalHits, equalTo(5));

        sortedShardList = searchPhaseController.sortDocs(newArrayList(queryResult));
        docIdsToLoad = searchPhaseController.docIdsToLoad(sortedShardList);
        assertThat(docIdsToLoad.size(), equalTo(1));
        assertThat(docIdsToLoad.values().iterator().next().size(), equalTo(2));

        fetchResult = searchService.executeFetchPhase(new FetchSearchRequest(queryResult.id(), docIdsToLoad.values().iterator().next()));
        assertThat(fetchResult.hits().hits().length, equalTo(2));
        for (SearchHit hit : fetchResult.hits()) {
            idsLoaded.add(hit.id());
        }

        // iterate to the next 2
        queryResult = searchService.executeQueryPhase(searchRequest(searchSource().query(matchAllQuery()).from(4).size(2), SearchType.QUERY_THEN_FETCH));
        assertThat(queryResult.topDocs().totalHits, equalTo(5));

        sortedShardList = searchPhaseController.sortDocs(newArrayList(queryResult));
        docIdsToLoad = searchPhaseController.docIdsToLoad(sortedShardList);
        assertThat(docIdsToLoad.size(), equalTo(1));
        assertThat(docIdsToLoad.values().iterator().next().size(), equalTo(1));

        fetchResult = searchService.executeFetchPhase(new FetchSearchRequest(queryResult.id(), docIdsToLoad.values().iterator().next()));
        assertThat(fetchResult.hits().hits().length, equalTo(1));
        for (SearchHit hit : fetchResult.hits()) {
            idsLoaded.add(hit.id());
        }

        // verify all ids were loaded
        Set<String> expectedIds = Sets.newHashSet("1", "2", "3", "4", "5");
        assertThat(idsLoaded, equalTo(expectedIds));
    }

    @Test
    public void testQueryThenFetchIterateWithFromSortedByAge() throws Exception {
        QuerySearchResult queryResult = searchService.executeQueryPhase(searchRequest(searchSource().query(matchAllQuery()).from(0).size(2).sort("age", SortOrder.DESC), SearchType.QUERY_THEN_FETCH));
        assertThat(queryResult.topDocs().totalHits, equalTo(5));

        Set<String> idsLoaded = Sets.newHashSet();

        ShardDoc[] sortedShardList = searchPhaseController.sortDocs(newArrayList(queryResult));
        Map<SearchShardTarget, ExtTIntArrayList> docIdsToLoad = searchPhaseController.docIdsToLoad(sortedShardList);
        assertThat(docIdsToLoad.size(), equalTo(1));
        assertThat(docIdsToLoad.values().iterator().next().size(), equalTo(2));

        FetchSearchResult fetchResult = searchService.executeFetchPhase(new FetchSearchRequest(queryResult.id(), docIdsToLoad.values().iterator().next()));
        assertThat(fetchResult.hits().hits().length, equalTo(2));
        for (SearchHit hit : fetchResult.hits()) {
            idsLoaded.add(hit.id());
        }

        // iterate to the next 2
        queryResult = searchService.executeQueryPhase(searchRequest(searchSource().query(matchAllQuery()).from(2).size(2).sort("age", SortOrder.DESC), SearchType.QUERY_THEN_FETCH));
        assertThat(queryResult.topDocs().totalHits, equalTo(5));

        sortedShardList = searchPhaseController.sortDocs(newArrayList(queryResult));
        docIdsToLoad = searchPhaseController.docIdsToLoad(sortedShardList);
        assertThat(docIdsToLoad.size(), equalTo(1));
        assertThat(docIdsToLoad.values().iterator().next().size(), equalTo(2));

        fetchResult = searchService.executeFetchPhase(new FetchSearchRequest(queryResult.id(), docIdsToLoad.values().iterator().next()));
        assertThat(fetchResult.hits().hits().length, equalTo(2));
        for (SearchHit hit : fetchResult.hits()) {
            idsLoaded.add(hit.id());
        }

        // iterate to the next 2
        queryResult = searchService.executeQueryPhase(searchRequest(searchSource().query(matchAllQuery()).from(4).size(2).sort("age", SortOrder.DESC), SearchType.QUERY_THEN_FETCH));
        assertThat(queryResult.topDocs().totalHits, equalTo(5));

        sortedShardList = searchPhaseController.sortDocs(newArrayList(queryResult));
        docIdsToLoad = searchPhaseController.docIdsToLoad(sortedShardList);
        assertThat(docIdsToLoad.size(), equalTo(1));
        assertThat(docIdsToLoad.values().iterator().next().size(), equalTo(1));

        fetchResult = searchService.executeFetchPhase(new FetchSearchRequest(queryResult.id(), docIdsToLoad.values().iterator().next()));
        assertThat(fetchResult.hits().hits().length, equalTo(1));
        for (SearchHit hit : fetchResult.hits()) {
            idsLoaded.add(hit.id());
        }

        // verify all ids were loaded
        Set<String> expectedIds = Sets.newHashSet("1", "2", "3", "4", "5");
        assertThat(idsLoaded, equalTo(expectedIds));
    }

    @Test
    public void testQueryAndFetch() throws Exception {
        QueryFetchSearchResult result = searchService.executeFetchPhase(searchRequest(searchSource().query(termQuery("name", "test1")), SearchType.QUERY_AND_FETCH));
        FetchSearchResult fetchResult = result.fetchResult();
        assertThat(fetchResult.hits().hits()[0].sourceAsString(), equalTo(source("1", "test1", 1)));
        assertThat(fetchResult.hits().hits()[0].id(), equalTo("1"));
        assertThat(fetchResult.hits().hits()[0].type(), equalTo("type1"));
    }

    @Test
    public void testQueryAndFetchIterateWithFrom() throws Exception {
        QueryFetchSearchResult result = searchService.executeFetchPhase(searchRequest(searchSource().query(matchAllQuery()).from(0).size(2).sort("age", SortOrder.DESC), SearchType.QUERY_AND_FETCH));
        assertThat(result.queryResult().topDocs().totalHits, equalTo(5));

        Set<String> idsLoaded = Sets.newHashSet();

        ShardDoc[] sortedShardList = searchPhaseController.sortDocs(newArrayList(result));
        Map<SearchShardTarget, QueryFetchSearchResult> queryResults = Maps.newHashMap();
        queryResults.put(result.queryResult().shardTarget(), result);
        InternalSearchResponse searchResponse = searchPhaseController.merge(sortedShardList, queryResults, queryResults);

        for (SearchHit hit : searchResponse.hits()) {
            idsLoaded.add(hit.id());
        }

        // iterate to the next 2
        result = searchService.executeFetchPhase(searchRequest(searchSource().query(matchAllQuery()).from(2).size(2).sort("age", SortOrder.DESC), SearchType.QUERY_AND_FETCH));
        assertThat(result.queryResult().topDocs().totalHits, equalTo(5));

        sortedShardList = searchPhaseController.sortDocs(newArrayList(result));
        queryResults = Maps.newHashMap();
        queryResults.put(result.queryResult().shardTarget(), result);
        searchResponse = searchPhaseController.merge(sortedShardList, queryResults, queryResults);

        for (SearchHit hit : searchResponse.hits()) {
            idsLoaded.add(hit.id());
        }
        result = searchService.executeFetchPhase(searchRequest(searchSource().query(matchAllQuery()).from(4).size(2).sort("age", SortOrder.DESC), SearchType.QUERY_AND_FETCH));
        assertThat(result.queryResult().topDocs().totalHits, equalTo(5));

        sortedShardList = searchPhaseController.sortDocs(newArrayList(result));
        queryResults = Maps.newHashMap();
        queryResults.put(result.queryResult().shardTarget(), result);
        searchResponse = searchPhaseController.merge(sortedShardList, queryResults, queryResults);

        for (SearchHit hit : searchResponse.hits()) {
            idsLoaded.add(hit.id());
        }

        // verify all ids were loaded
        Set<String> expectedIds = Sets.newHashSet("1", "2", "3", "4", "5");
        assertThat(idsLoaded, equalTo(expectedIds));
    }

    @Test
    public void testDfsQueryThenFetch() throws Exception {
        DfsSearchResult dfsResult = searchService.executeDfsPhase(searchRequest(searchSource().query(termQuery("name", "test1")), SearchType.DFS_QUERY_THEN_FETCH));
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

    @Test
    public void testQueryFetchKeepAliveTimeout() throws Exception {
        QuerySearchResult queryResult = searchService.executeQueryPhase(searchRequest(searchSource().query(termQuery("name", "test1")), SearchType.QUERY_THEN_FETCH).scroll(new Scroll(TimeValue.timeValueMillis(10))));
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


    private InternalSearchRequest searchRequest(SearchSourceBuilder builder, SearchType searchType) {
        return new InternalSearchRequest("test", 0, 1, searchType).source(builder.buildAsBytes());
    }

    private void index(String id, String nameValue, int age) {
        client.index(indexRequest("test").type("type1").id(id).source(source(id, nameValue, age))).actionGet();
    }

    private String source(String id, String nameValue, int age) {
        return "{ type1 : { \"id\" : \"" + id + "\", \"name\" : \"" + nameValue + "\", age : " + age + " } }";
    }
}
