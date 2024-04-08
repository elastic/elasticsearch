/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.search;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.admin.indices.stats.CommonStats;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchContextMissingException;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.search.builder.PointInTimeBuilder;
import org.elasticsearch.search.sort.SortBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.tasks.TaskInfo;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.transport.MockTransportService;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertFailures;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailuresAndResponse;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.hamcrest.Matchers.arrayWithSize;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.in;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;

public class PointInTimeIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.appendToCopy(super.nodePlugins(), MockTransportService.TestPlugin.class);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put(SearchService.KEEPALIVE_INTERVAL_SETTING.getKey(), TimeValue.timeValueMillis(randomIntBetween(100, 500)))
            .build();
    }

    public void testBasic() {
        createIndex("test");
        int numDocs = randomIntBetween(10, 50);
        for (int i = 0; i < numDocs; i++) {
            String id = Integer.toString(i);
            prepareIndex("test").setId(id).setSource("value", i).get();
        }
        refresh("test");
        String pitId = openPointInTime(new String[] { "test" }, TimeValue.timeValueMinutes(2));
        assertResponse(prepareSearch().setPointInTime(new PointInTimeBuilder(pitId)), resp1 -> {
            assertThat(resp1.pointInTimeId(), equalTo(pitId));
            assertHitCount(resp1, numDocs);
        });
        int deletedDocs = 0;
        for (int i = 0; i < numDocs; i++) {
            if (randomBoolean()) {
                String id = Integer.toString(i);
                client().prepareDelete("test", id).get();
                deletedDocs++;
            }
        }
        refresh("test");
        if (randomBoolean()) {
            final int delDocCount = deletedDocs;
            assertNoFailuresAndResponse(
                prepareSearch("test").setQuery(new MatchAllQueryBuilder()),
                resp2 -> assertHitCount(resp2, numDocs - delDocCount)
            );
        }
        try {
            assertNoFailuresAndResponse(
                prepareSearch().setQuery(new MatchAllQueryBuilder()).setPointInTime(new PointInTimeBuilder(pitId)),
                resp3 -> {
                    assertHitCount(resp3, numDocs);
                    assertThat(resp3.pointInTimeId(), equalTo(pitId));
                }
            );
        } finally {
            closePointInTime(pitId);
        }
    }

    public void testMultipleIndices() {
        int numIndices = randomIntBetween(1, 5);
        for (int i = 1; i <= numIndices; i++) {
            createIndex("index-" + i);
        }
        int numDocs = randomIntBetween(10, 50);
        for (int i = 0; i < numDocs; i++) {
            String id = Integer.toString(i);
            String index = "index-" + randomIntBetween(1, numIndices);
            prepareIndex(index).setId(id).setSource("value", i).get();
        }
        refresh();
        String pitId = openPointInTime(new String[] { "*" }, TimeValue.timeValueMinutes(2));
        try {
            int moreDocs = randomIntBetween(10, 50);
            assertNoFailuresAndResponse(prepareSearch().setPointInTime(new PointInTimeBuilder(pitId)), resp -> {
                assertHitCount(resp, numDocs);
                assertNotNull(resp.pointInTimeId());
                assertThat(resp.pointInTimeId(), equalTo(pitId));
                for (int i = 0; i < moreDocs; i++) {
                    String id = "more-" + i;
                    String index = "index-" + randomIntBetween(1, numIndices);
                    prepareIndex(index).setId(id).setSource("value", i).get();
                }
                refresh();
            });
            assertNoFailuresAndResponse(prepareSearch(), resp -> assertHitCount(resp, numDocs + moreDocs));
            assertNoFailuresAndResponse(prepareSearch().setPointInTime(new PointInTimeBuilder(pitId)), resp -> {
                assertHitCount(resp, numDocs);
                assertNotNull(resp.pointInTimeId());
                assertThat(resp.pointInTimeId(), equalTo(pitId));
            });
        } finally {
            closePointInTime(pitId);
        }
    }

    public void testIndexFilter() {
        int numDocs = randomIntBetween(1, 9);
        for (int i = 1; i <= 3; i++) {
            String index = "index-" + i;
            createIndex(index);
            for (int j = 1; j <= numDocs; j++) {
                String id = Integer.toString(j);
                client().prepareIndex(index).setId(id).setSource("@timestamp", "2023-0" + i + "-0" + j).get();
            }
        }
        refresh();

        {

            OpenPointInTimeRequest request = new OpenPointInTimeRequest("*").keepAlive(TimeValue.timeValueMinutes(2));
            final OpenPointInTimeResponse response = client().execute(TransportOpenPointInTimeAction.TYPE, request).actionGet();
            try {
                SearchContextId searchContextId = SearchContextId.decode(writableRegistry(), response.getPointInTimeId());
                String[] actualIndices = searchContextId.getActualIndices();
                assertEquals(3, actualIndices.length);
            } finally {
                closePointInTime(response.getPointInTimeId());
            }
        }
        {
            OpenPointInTimeRequest request = new OpenPointInTimeRequest("*").keepAlive(TimeValue.timeValueMinutes(2));
            request.indexFilter(new RangeQueryBuilder("@timestamp").gte("2023-03-01"));
            final OpenPointInTimeResponse response = client().execute(TransportOpenPointInTimeAction.TYPE, request).actionGet();
            String pitId = response.getPointInTimeId();
            try {
                SearchContextId searchContextId = SearchContextId.decode(writableRegistry(), pitId);
                String[] actualIndices = searchContextId.getActualIndices();
                assertEquals(1, actualIndices.length);
                assertEquals("index-3", actualIndices[0]);
                assertNoFailuresAndResponse(prepareSearch().setPointInTime(new PointInTimeBuilder(pitId)).setSize(50), resp -> {
                    assertHitCount(resp, numDocs);
                    assertNotNull(resp.pointInTimeId());
                    assertThat(resp.pointInTimeId(), equalTo(pitId));
                    for (SearchHit hit : resp.getHits()) {
                        assertEquals("index-3", hit.getIndex());
                    }
                });
            } finally {
                closePointInTime(pitId);
            }
        }
    }

    public void testRelocation() throws Exception {
        internalCluster().ensureAtLeastNumDataNodes(4);
        createIndex("test", Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, between(0, 1)).build());
        ensureGreen("test");
        int numDocs = randomIntBetween(10, 50);
        for (int i = 0; i < numDocs; i++) {
            prepareIndex("test").setId(Integer.toString(i)).setSource("value", i).get();
        }
        refresh();
        String pitId = openPointInTime(new String[] { "test" }, TimeValue.timeValueMinutes(2));
        try {
            assertNoFailuresAndResponse(prepareSearch().setPointInTime(new PointInTimeBuilder(pitId)), resp -> {
                assertHitCount(resp, numDocs);
                assertThat(resp.pointInTimeId(), equalTo(pitId));
            });
            final Set<String> dataNodes = clusterService().state()
                .nodes()
                .getDataNodes()
                .values()
                .stream()
                .map(DiscoveryNode::getId)
                .collect(Collectors.toSet());
            final List<String> excludedNodes = randomSubsetOf(2, dataNodes);
            updateIndexSettings(Settings.builder().put("index.routing.allocation.exclude._id", String.join(",", excludedNodes)), "test");
            if (randomBoolean()) {
                int moreDocs = randomIntBetween(10, 50);
                for (int i = 0; i < moreDocs; i++) {
                    prepareIndex("test").setId("more-" + i).setSource("value", i).get();
                }
                refresh();
            }
            assertNoFailuresAndResponse(prepareSearch().setPointInTime(new PointInTimeBuilder(pitId)), resp -> {
                assertHitCount(resp, numDocs);
                assertThat(resp.pointInTimeId(), equalTo(pitId));
            });
            assertBusy(() -> {
                final Set<String> assignedNodes = clusterService().state()
                    .routingTable()
                    .allShards()
                    .filter(shr -> shr.index().getName().equals("test") && shr.assignedToNode())
                    .map(ShardRouting::currentNodeId)
                    .collect(Collectors.toSet());
                assertThat(assignedNodes, everyItem(not(in(excludedNodes))));
            }, 30, TimeUnit.SECONDS);
            assertNoFailuresAndResponse(prepareSearch().setPointInTime(new PointInTimeBuilder(pitId)), resp -> {
                assertHitCount(resp, numDocs);
                assertThat(resp.pointInTimeId(), equalTo(pitId));
            });
        } finally {
            closePointInTime(pitId);
        }
    }

    public void testPointInTimeNotFound() throws Exception {
        createIndex("index");
        int index1 = randomIntBetween(10, 50);
        for (int i = 0; i < index1; i++) {
            String id = Integer.toString(i);
            prepareIndex("index").setId(id).setSource("value", i).get();
        }
        refresh();
        String pit = openPointInTime(new String[] { "index" }, TimeValue.timeValueSeconds(5));
        assertNoFailuresAndResponse(prepareSearch().setPointInTime(new PointInTimeBuilder(pit)), resp1 -> {
            assertHitCount(resp1, index1);
            if (rarely()) {
                try {
                    assertBusy(() -> {
                        final CommonStats stats = indicesAdmin().prepareStats().setSearch(true).get().getTotal();
                        assertThat(stats.search.getOpenContexts(), equalTo(0L));
                    }, 60, TimeUnit.SECONDS);
                } catch (Exception e) {
                    throw new AssertionError(e);
                }
            } else {
                closePointInTime(resp1.pointInTimeId());
            }
        });
        SearchPhaseExecutionException e = expectThrows(
            SearchPhaseExecutionException.class,
            prepareSearch().setPointInTime(new PointInTimeBuilder(pit))
        );
        for (ShardSearchFailure failure : e.shardFailures()) {
            assertThat(ExceptionsHelper.unwrapCause(failure.getCause()), instanceOf(SearchContextMissingException.class));
        }
    }

    public void testIndexNotFound() {
        createIndex("index-1");
        createIndex("index-2");

        int index1 = randomIntBetween(10, 50);
        for (int i = 0; i < index1; i++) {
            String id = Integer.toString(i);
            prepareIndex("index-1").setId(id).setSource("value", i).get();
        }

        int index2 = randomIntBetween(10, 50);
        for (int i = 0; i < index2; i++) {
            String id = Integer.toString(i);
            prepareIndex("index-2").setId(id).setSource("value", i).get();
        }
        refresh();
        String pit = openPointInTime(new String[] { "index-*" }, TimeValue.timeValueMinutes(2));
        try {
            assertNoFailuresAndResponse(
                prepareSearch().setPointInTime(new PointInTimeBuilder(pit)),
                resp -> assertHitCount(resp, index1 + index2)
            );
            indicesAdmin().prepareDelete("index-1").get();
            if (randomBoolean()) {
                assertNoFailuresAndResponse(prepareSearch("index-*"), resp -> assertHitCount(resp, index2));
            }

            // Allow partial search result
            assertResponse(prepareSearch().setAllowPartialSearchResults(true).setPointInTime(new PointInTimeBuilder(pit)), resp -> {
                assertFailures(resp);
                assertHitCount(resp, index2);
            });

            // Do not allow partial search result
            expectThrows(
                ElasticsearchException.class,
                prepareSearch().setAllowPartialSearchResults(false).setPointInTime(new PointInTimeBuilder(pit))
            );
        } finally {
            closePointInTime(pit);
        }
    }

    public void testAllowNoIndex() {
        var request = new OpenPointInTimeRequest("my_index").indicesOptions(IndicesOptions.LENIENT_EXPAND_OPEN)
            .keepAlive(TimeValue.timeValueMinutes(between(1, 10)));
        String pit = client().execute(TransportOpenPointInTimeAction.TYPE, request).actionGet().getPointInTimeId();
        var closeResp = client().execute(TransportClosePointInTimeAction.TYPE, new ClosePointInTimeRequest(pit)).actionGet();
        assertThat(closeResp.status(), equalTo(RestStatus.OK));
    }

    public void testCanMatch() throws Exception {
        final Settings.Builder settings = indexSettings(randomIntBetween(5, 10), 0).put(
            IndexSettings.INDEX_SEARCH_IDLE_AFTER.getKey(),
            TimeValue.timeValueMillis(randomIntBetween(50, 100))
        );
        assertAcked(prepareCreate("test").setSettings(settings).setMapping("""
            {"properties":{"created_date":{"type": "date", "format": "yyyy-MM-dd"}}}"""));
        ensureGreen("test");
        String pitId = openPointInTime(new String[] { "test*" }, TimeValue.timeValueMinutes(2));
        try {
            for (String node : internalCluster().nodesInclude("test")) {
                for (IndexService indexService : internalCluster().getInstance(IndicesService.class, node)) {
                    for (IndexShard indexShard : indexService) {
                        assertBusy(() -> assertTrue(indexShard.isSearchIdle()));
                    }
                }
            }
            prepareIndex("test").setId("1").setSource("created_date", "2020-01-01").get();
            assertResponse(
                prepareSearch().setQuery(new RangeQueryBuilder("created_date").gte("2020-01-02").lte("2020-01-03"))
                    .setSearchType(SearchType.QUERY_THEN_FETCH)
                    .setPreFilterShardSize(randomIntBetween(2, 3))
                    .setMaxConcurrentShardRequests(randomIntBetween(1, 2))
                    .setPointInTime(new PointInTimeBuilder(pitId)),
                resp -> assertThat(resp.getHits().getHits(), arrayWithSize(0))
            );
            for (String node : internalCluster().nodesInclude("test")) {
                for (IndexService indexService : internalCluster().getInstance(IndicesService.class, node)) {
                    for (IndexShard indexShard : indexService) {
                        // all shards are still search-idle as we did not acquire new searchers
                        assertTrue(indexShard.isSearchIdle());
                    }
                }
            }
        } finally {
            closePointInTime(pitId);
        }
    }

    public void testPartialResults() throws Exception {
        internalCluster().ensureAtLeastNumDataNodes(2);
        final List<String> dataNodes = internalCluster().clusterService()
            .state()
            .nodes()
            .getDataNodes()
            .values()
            .stream()
            .map(DiscoveryNode::getName)
            .toList();
        final String assignedNodeForIndex1 = randomFrom(dataNodes);

        createIndex(
            "test-1",
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .put("index.routing.allocation.include._name", assignedNodeForIndex1)
                .build()
        );
        createIndex(
            "test-2",
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .put("index.routing.allocation.exclude._name", assignedNodeForIndex1)
                .build()
        );

        int numDocs1 = randomIntBetween(10, 50);
        for (int i = 0; i < numDocs1; i++) {
            prepareIndex(randomFrom("test-1")).setId(Integer.toString(i)).setSource("value", i).get();
        }
        int numDocs2 = randomIntBetween(10, 50);
        for (int i = 0; i < numDocs2; i++) {
            prepareIndex(randomFrom("test-2")).setId(Integer.toString(i)).setSource("value", i).get();
        }
        refresh();
        String pitId = openPointInTime(new String[] { "test-*" }, TimeValue.timeValueMinutes(2));
        try {
            assertNoFailuresAndResponse(prepareSearch().setPointInTime(new PointInTimeBuilder(pitId)), resp -> {
                assertHitCount(resp, numDocs1 + numDocs2);
                assertThat(resp.pointInTimeId(), equalTo(pitId));
            });

            internalCluster().restartNode(assignedNodeForIndex1);
            assertResponse(prepareSearch().setAllowPartialSearchResults(true).setPointInTime(new PointInTimeBuilder(pitId)), resp -> {
                assertFailures(resp);
                assertThat(resp.pointInTimeId(), equalTo(pitId));
                assertHitCount(resp, numDocs2);
            });
        } finally {
            closePointInTime(pitId);
        }
    }

    public void testPITTiebreak() throws Exception {
        assertAcked(indicesAdmin().prepareDelete("index-*").get());
        int numIndex = randomIntBetween(2, 10);
        int expectedNumDocs = 0;
        for (int i = 0; i < numIndex; i++) {
            String index = "index-" + i;
            createIndex(index, Settings.builder().put("index.number_of_shards", 1).build());
            int numDocs = randomIntBetween(3, 20);
            for (int j = 0; j < numDocs; j++) {
                prepareIndex(index).setSource("value", randomIntBetween(0, 2)).get();
                expectedNumDocs++;
            }
        }
        refresh("index-*");
        String pit = openPointInTime(new String[] { "index-*" }, TimeValue.timeValueHours(1));
        try {
            for (int size = 1; size <= numIndex; size++) {
                SortOrder order = randomBoolean() ? SortOrder.ASC : SortOrder.DESC;

                assertPagination(new PointInTimeBuilder(pit), expectedNumDocs, size, SortBuilders.pitTiebreaker().order(order));

                assertPagination(new PointInTimeBuilder(pit), expectedNumDocs, size, SortBuilders.scoreSort());
                assertPagination(
                    new PointInTimeBuilder(pit),
                    expectedNumDocs,
                    size,
                    SortBuilders.scoreSort(),
                    SortBuilders.pitTiebreaker().order(order)
                );

                assertPagination(new PointInTimeBuilder(pit), expectedNumDocs, size, SortBuilders.fieldSort("value"));
                assertPagination(
                    new PointInTimeBuilder(pit),
                    expectedNumDocs,
                    size,
                    SortBuilders.fieldSort("value"),
                    SortBuilders.pitTiebreaker().order(order)
                );
            }
        } finally {
            closePointInTime(pit);
        }
    }

    public void testCloseInvalidPointInTime() {
        expectThrows(Exception.class, client().execute(TransportClosePointInTimeAction.TYPE, new ClosePointInTimeRequest("")));
        List<TaskInfo> tasks = clusterAdmin().prepareListTasks().setActions(TransportClosePointInTimeAction.TYPE.name()).get().getTasks();
        assertThat(tasks, empty());
    }

    public void testOpenPITConcurrentShardRequests() throws Exception {
        DiscoveryNode dataNode = randomFrom(clusterService().state().nodes().getDataNodes().values());
        int numShards = randomIntBetween(5, 10);
        int maxConcurrentRequests = randomIntBetween(2, 5);
        assertAcked(
            client().admin()
                .indices()
                .prepareCreate("test")
                .setSettings(
                    Settings.builder()
                        .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, numShards)
                        .put("index.routing.allocation.require._id", dataNode.getId())
                        .build()
                )
        );
        final var transportService = MockTransportService.getInstance(dataNode.getName());
        try {
            CountDownLatch sentLatch = new CountDownLatch(maxConcurrentRequests);
            CountDownLatch readyLatch = new CountDownLatch(1);
            transportService.addRequestHandlingBehavior(
                TransportOpenPointInTimeAction.OPEN_SHARD_READER_CONTEXT_NAME,
                (handler, request, channel, task) -> {
                    sentLatch.countDown();
                    Thread thread = new Thread(() -> {
                        try {
                            assertTrue(readyLatch.await(1, TimeUnit.MINUTES));
                            handler.messageReceived(request, channel, task);
                        } catch (Exception e) {
                            throw new AssertionError(e);
                        }
                    });
                    thread.start();
                }
            );
            OpenPointInTimeRequest request = new OpenPointInTimeRequest("test").keepAlive(TimeValue.timeValueMinutes(1));
            request.maxConcurrentShardRequests(maxConcurrentRequests);
            PlainActionFuture<OpenPointInTimeResponse> future = new PlainActionFuture<>();
            client().execute(TransportOpenPointInTimeAction.TYPE, request, future);
            assertTrue(sentLatch.await(1, TimeUnit.MINUTES));
            readyLatch.countDown();
            closePointInTime(future.actionGet().getPointInTimeId());
        } finally {
            transportService.clearAllRules();
        }
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    private void assertPagination(PointInTimeBuilder pit, int expectedNumDocs, int size, SortBuilder<?>... sorts) throws Exception {
        Set<String> seen = new HashSet<>();
        SearchRequestBuilder builder = prepareSearch().setSize(size).setPointInTime(pit);
        for (SortBuilder<?> sort : sorts) {
            builder.addSort(sort);
        }
        final SearchRequest searchRequest = builder.request().rewrite(null);

        final List<SortBuilder<?>> expectedSorts = searchRequest.source().sorts();
        final int[] reverseMuls = new int[expectedSorts.size()];
        for (int i = 0; i < expectedSorts.size(); i++) {
            reverseMuls[i] = expectedSorts.get(i).order() == SortOrder.ASC ? 1 : -1;
        }
        SearchResponse response = client().search(searchRequest).get();
        try {
            Object[] lastSortValues = null;
            while (response.getHits().getHits().length > 0) {
                Object[] lastHitSortValues = null;
                for (SearchHit hit : response.getHits().getHits()) {
                    assertTrue(seen.add(hit.getIndex() + hit.getId()));

                    if (lastHitSortValues != null) {
                        for (int i = 0; i < expectedSorts.size(); i++) {
                            Comparable value = (Comparable) hit.getRawSortValues()[i];
                            int cmp = value.compareTo(lastHitSortValues[i]) * reverseMuls[i];
                            if (cmp != 0) {
                                assertThat(cmp, equalTo(1));
                                break;
                            }
                        }
                    }
                    lastHitSortValues = hit.getRawSortValues();
                }
                int len = response.getHits().getHits().length;
                SearchHit last = response.getHits().getHits()[len - 1];
                if (lastSortValues != null) {
                    for (int i = 0; i < expectedSorts.size(); i++) {
                        Comparable value = (Comparable) last.getSortValues()[i];
                        int cmp = value.compareTo(lastSortValues[i]) * reverseMuls[i];
                        if (cmp != 0) {
                            assertThat(cmp, equalTo(1));
                            break;
                        }
                    }
                }
                assertThat(last.getSortValues().length, equalTo(expectedSorts.size()));
                lastSortValues = last.getSortValues();
                searchRequest.source().searchAfter(last.getSortValues());
                response.decRef();
                response = client().search(searchRequest).get();
            }
        } finally {
            response.decRef();
        }
        assertThat(seen.size(), equalTo(expectedNumDocs));
    }

    private String openPointInTime(String[] indices, TimeValue keepAlive) {
        OpenPointInTimeRequest request = new OpenPointInTimeRequest(indices).keepAlive(keepAlive);
        final OpenPointInTimeResponse response = client().execute(TransportOpenPointInTimeAction.TYPE, request).actionGet();
        return response.getPointInTimeId();
    }

    private void closePointInTime(String readerId) {
        client().execute(TransportClosePointInTimeAction.TYPE, new ClosePointInTimeRequest(readerId)).actionGet();
    }
}
