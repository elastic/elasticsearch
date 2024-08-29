/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.search;

import org.apache.lucene.search.TotalHits;
import org.elasticsearch.action.admin.cluster.shards.ClusterSearchShardsRequest;
import org.elasticsearch.action.admin.cluster.shards.ClusterSearchShardsResponse;
import org.elasticsearch.action.admin.cluster.shards.TransportClusterSearchShardsAction;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.core.Strings;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.query.QuerySearchResult;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.test.ESSingleNodeTestCase;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.lessThan;

public class SearchProgressActionListenerIT extends ESSingleNodeTestCase {
    private List<SearchShard> shards;

    public void setUp() throws Exception {
        super.setUp();
        shards = createRandomIndices(client());
    }

    public void testSearchProgressSimple() throws Exception {
        for (SearchType searchType : SearchType.values()) {
            SearchRequest request = new SearchRequest("index-*").searchType(searchType).source(new SearchSourceBuilder().size(0));
            testCase((NodeClient) client(), request, shards, false);
        }
    }

    public void testSearchProgressWithHits() throws Exception {
        for (SearchType searchType : SearchType.values()) {
            SearchRequest request = new SearchRequest("index-*").searchType(searchType).source(new SearchSourceBuilder().size(10));
            testCase((NodeClient) client(), request, shards, true);
        }
    }

    public void testSearchProgressWithAggs() throws Exception {
        for (SearchType searchType : SearchType.values()) {
            SearchRequest request = new SearchRequest("index-*").searchType(searchType)
                .source(new SearchSourceBuilder().size(0).aggregation(AggregationBuilders.max("max").field("number")));
            testCase((NodeClient) client(), request, shards, false);
        }
    }

    public void testSearchProgressWithHitsAndAggs() throws Exception {
        for (SearchType searchType : SearchType.values()) {
            SearchRequest request = new SearchRequest("index-*").searchType(searchType)
                .source(new SearchSourceBuilder().size(10).aggregation(AggregationBuilders.max("max").field("number")));
            testCase((NodeClient) client(), request, shards, true);
        }
    }

    public void testSearchProgressWithQuery() throws Exception {
        for (SearchType searchType : SearchType.values()) {
            SearchRequest request = new SearchRequest("index-*").searchType(searchType)
                .source(new SearchSourceBuilder().size(10).query(QueryBuilders.termQuery("foo", "bar")));
            testCase((NodeClient) client(), request, shards, true);
        }
    }

    public void testSearchProgressWithShardSort() throws Exception {
        SearchRequest request = new SearchRequest("index-*").source(
            new SearchSourceBuilder().size(0).sort(new FieldSortBuilder("number").order(SortOrder.DESC))
        );
        request.setPreFilterShardSize(1);
        List<SearchShard> sortShards = new ArrayList<>(shards);
        Collections.sort(sortShards, Comparator.reverseOrder());
        testCase((NodeClient) client(), request, sortShards, false);
    }

    private void testCase(NodeClient client, SearchRequest request, List<SearchShard> expectedShards, boolean hasFetchPhase)
        throws InterruptedException {
        AtomicInteger numQueryResults = new AtomicInteger();
        AtomicInteger numQueryFailures = new AtomicInteger();
        AtomicInteger numFetchResults = new AtomicInteger();
        AtomicInteger numFetchFailures = new AtomicInteger();
        AtomicInteger numReduces = new AtomicInteger();
        AtomicReference<SearchResponse> searchResponse = new AtomicReference<>();
        AtomicReference<List<SearchShard>> shardsListener = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(1);
        SearchProgressActionListener listener = new SearchProgressActionListener() {
            @Override
            public void onListShards(
                List<SearchShard> searchShards,
                List<SearchShard> skippedShards,
                SearchResponse.Clusters clusters,
                boolean fetchPhase,
                TransportSearchAction.SearchTimeProvider timeProvider
            ) {
                shardsListener.set(searchShards);
                assertEquals(fetchPhase, hasFetchPhase);
            }

            @Override
            public void onQueryResult(int shardIndex, QuerySearchResult result) {
                assertThat(shardIndex, lessThan(shardsListener.get().size()));
                numQueryResults.incrementAndGet();
            }

            @Override
            public void onQueryFailure(int shardIndex, SearchShardTarget shardTarget, Exception exc) {
                assertThat(shardIndex, lessThan(shardsListener.get().size()));
                numQueryFailures.incrementAndGet();
            }

            @Override
            public void onFetchResult(int shardIndex) {
                assertThat(shardIndex, lessThan(shardsListener.get().size()));
                numFetchResults.incrementAndGet();
            }

            @Override
            public void onFetchFailure(int shardIndex, SearchShardTarget shardTarget, Exception exc) {
                assertThat(shardIndex, lessThan(shardsListener.get().size()));
                numFetchFailures.incrementAndGet();
            }

            @Override
            public void onPartialReduce(List<SearchShard> searchShards, TotalHits totalHits, InternalAggregations aggs, int reducePhase) {
                numReduces.incrementAndGet();
            }

            @Override
            public void onFinalReduce(List<SearchShard> searchShards, TotalHits totalHits, InternalAggregations aggs, int reducePhase) {
                numReduces.incrementAndGet();
            }

            @Override
            public void onResponse(SearchResponse response) {
                searchResponse.set(response);
                latch.countDown();
            }

            @Override
            public void onFailure(Exception e) {
                throw new AssertionError();
            }
        };
        client.executeLocally(TransportSearchAction.TYPE, new SearchRequest(request) {
            @Override
            public SearchTask createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
                SearchTask task = super.createTask(id, type, action, parentTaskId, headers);
                task.setProgressListener(listener);
                return task;
            }
        }, listener);
        latch.await();
        assertThat(shardsListener.get(), equalTo(expectedShards));
        assertThat(numQueryResults.get(), equalTo(searchResponse.get().getSuccessfulShards()));
        assertThat(numQueryFailures.get(), equalTo(searchResponse.get().getFailedShards()));
        if (hasFetchPhase) {
            assertThat(numFetchResults.get(), equalTo(searchResponse.get().getSuccessfulShards()));
            assertThat(numFetchFailures.get(), equalTo(0));
        } else {
            assertThat(numFetchResults.get(), equalTo(0));
            assertThat(numFetchFailures.get(), equalTo(0));
        }
        assertThat(numReduces.get(), equalTo(searchResponse.get().getNumReducePhases()));
    }

    private static List<SearchShard> createRandomIndices(Client client) {
        int numIndices = randomIntBetween(3, 20);
        for (int i = 0; i < numIndices; i++) {
            String indexName = Strings.format("index-%03d", i);
            assertAcked(client.admin().indices().prepareCreate(indexName).get());
            client.prepareIndex(indexName).setSource("number", i, "foo", "bar").get();
        }
        client.admin().indices().prepareRefresh("index-*").get();
        ClusterSearchShardsResponse resp = safeExecute(
            client,
            TransportClusterSearchShardsAction.TYPE,
            new ClusterSearchShardsRequest(TEST_REQUEST_TIMEOUT, "index-*")
        );
        return Arrays.stream(resp.getGroups()).map(e -> new SearchShard(null, e.getShardId())).sorted().toList();
    }
}
