/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.search;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.NoShardAvailableActionException;
import org.elasticsearch.action.admin.cluster.reroute.ClusterRerouteUtils;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.admin.indices.stats.CommonStats;
import org.elasticsearch.action.admin.indices.stats.ShardStats;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.command.AllocateEmptyPrimaryAllocationCommand;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
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
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.in;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.lessThan;
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
        BytesReference pitId = openPointInTime(new String[] { "test" }, TimeValue.timeValueMinutes(2)).getPointInTimeId();
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

    public void testIndexWithAlias() {
        String indexName = "index_1";
        String alias = "alias_1";
        assertAcked(indicesAdmin().prepareCreate(indexName).setSettings(indexSettings(10, 0)).addAlias(new Alias(alias)));
        int numDocs = randomIntBetween(50, 150);
        for (int i = 0; i < numDocs; i++) {
            String id = Integer.toString(i);
            prepareIndex(indexName).setId(id).setSource("value", i).get();
        }
        refresh(indexName);
        BytesReference pitId = openPointInTime(new String[] { alias }, TimeValue.timeValueMinutes(1)).getPointInTimeId();
        ;
        try {
            assertResponse(prepareSearch().setPointInTime(new PointInTimeBuilder(pitId)), resp1 -> {
                assertThat(resp1.pointInTimeId(), equalTo(pitId));
                assertHitCount(resp1, numDocs);
            });
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
        BytesReference pitId = openPointInTime(new String[] { "*" }, TimeValue.timeValueMinutes(2)).getPointInTimeId();
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
            BytesReference pitId = response.getPointInTimeId();
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
        BytesReference pitId = openPointInTime(new String[] { "test" }, TimeValue.timeValueMinutes(2)).getPointInTimeId();
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
        BytesReference pit = openPointInTime(new String[] { "index" }, TimeValue.timeValueSeconds(5)).getPointInTimeId();
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
        BytesReference pit = openPointInTime(new String[] { "index-*" }, TimeValue.timeValueMinutes(2)).getPointInTimeId();
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
        BytesReference pit = client().execute(TransportOpenPointInTimeAction.TYPE, request).actionGet().getPointInTimeId();
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
        BytesReference pitId = openPointInTime(new String[] { "test*" }, TimeValue.timeValueMinutes(2)).getPointInTimeId();
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
        BytesReference pitId = openPointInTime(new String[] { "test-*" }, TimeValue.timeValueMinutes(2)).getPointInTimeId();
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
        BytesReference pit = openPointInTime(new String[] { "index-*" }, TimeValue.timeValueHours(1)).getPointInTimeId();
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
        expectThrows(
            Exception.class,
            client().execute(TransportClosePointInTimeAction.TYPE, new ClosePointInTimeRequest(BytesArray.EMPTY))
        );
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

    public void testMissingShardsWithPointInTime() throws Exception {
        final Settings nodeAttributes = Settings.builder().put("node.attr.foo", "bar").build();
        final String masterNode = internalCluster().startMasterOnlyNode(nodeAttributes);
        List<String> dataNodes = internalCluster().startDataOnlyNodes(2, nodeAttributes);

        final String index = "my_test_index";
        // tried to have randomIntBetween(3, 10) but having more shards than 3 was taking forever and throwing timeouts
        final int numShards = 3;
        final int numReplicas = 0;
        // create an index with numShards shards and 0 replicas
        createIndex(
            index,
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, numShards)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, numReplicas)
                .put("index.routing.allocation.require.foo", "bar")
                .build()
        );

        // index some documents
        int numDocs = randomIntBetween(10, 50);
        for (int i = 0; i < numDocs; i++) {
            String id = Integer.toString(i);
            prepareIndex(index).setId(id).setSource("value", i).get();
        }
        refresh(index);

        // create a PIT when all shards are present
        OpenPointInTimeResponse pointInTimeResponse = openPointInTime(new String[] { index }, TimeValue.timeValueMinutes(1));
        try {
            // ensure that the PIT created has all the shards there
            assertThat(numShards, equalTo(pointInTimeResponse.getTotalShards()));
            assertThat(numShards, equalTo(pointInTimeResponse.getSuccessfulShards()));
            assertThat(0, equalTo(pointInTimeResponse.getFailedShards()));
            assertThat(0, equalTo(pointInTimeResponse.getSkippedShards()));

            // make a request using the above PIT
            assertResponse(
                prepareSearch().setQuery(new MatchAllQueryBuilder())
                    .setPointInTime(new PointInTimeBuilder(pointInTimeResponse.getPointInTimeId())),
                resp -> {
                    // ensure that al docs are returned
                    assertThat(resp.pointInTimeId(), equalTo(pointInTimeResponse.getPointInTimeId()));
                    assertHitCount(resp, numDocs);
                }
            );

            // pick up a random data node to shut down
            final String randomDataNode = randomFrom(dataNodes);

            // find which shards to relocate
            final String nodeId = admin().cluster().prepareNodesInfo(randomDataNode).get().getNodes().get(0).getNode().getId();
            Set<Integer> shardsToRelocate = new HashSet<>();
            for (ShardStats stats : admin().indices().prepareStats(index).get().getShards()) {
                if (nodeId.equals(stats.getShardRouting().currentNodeId())) {
                    shardsToRelocate.add(stats.getShardRouting().shardId().id());
                }
            }

            final int shardsRemoved = shardsToRelocate.size();

            // shut down the random data node
            internalCluster().stopNode(randomDataNode);

            // ensure that the index is Red
            ensureRed(index);

            // verify that not all documents can now be retrieved
            assertResponse(prepareSearch().setQuery(new MatchAllQueryBuilder()), resp -> {
                assertThat(resp.getSuccessfulShards(), equalTo(numShards - shardsRemoved));
                assertThat(resp.getFailedShards(), equalTo(shardsRemoved));
                assertNotNull(resp.getHits().getTotalHits());
                assertThat(resp.getHits().getTotalHits().value, lessThan((long) numDocs));
            });

            // create a PIT when some shards are missing
            OpenPointInTimeResponse pointInTimeResponseOneNodeDown = openPointInTime(
                new String[] { index },
                TimeValue.timeValueMinutes(10),
                true
            );
            try {
                // assert that some shards are indeed missing from PIT
                assertThat(pointInTimeResponseOneNodeDown.getTotalShards(), equalTo(numShards));
                assertThat(pointInTimeResponseOneNodeDown.getSuccessfulShards(), equalTo(numShards - shardsRemoved));
                assertThat(pointInTimeResponseOneNodeDown.getFailedShards(), equalTo(shardsRemoved));
                assertThat(pointInTimeResponseOneNodeDown.getSkippedShards(), equalTo(0));

                // ensure that the response now contains fewer documents than the total number of indexed documents
                assertResponse(
                    prepareSearch().setQuery(new MatchAllQueryBuilder())
                        .setPointInTime(new PointInTimeBuilder(pointInTimeResponseOneNodeDown.getPointInTimeId())),
                    resp -> {
                        assertThat(resp.getSuccessfulShards(), equalTo(numShards - shardsRemoved));
                        assertThat(resp.getFailedShards(), equalTo(shardsRemoved));
                        assertThat(resp.pointInTimeId(), equalTo(pointInTimeResponseOneNodeDown.getPointInTimeId()));
                        assertNotNull(resp.getHits().getTotalHits());
                        assertThat(resp.getHits().getTotalHits().value, lessThan((long) numDocs));
                    }
                );

                // add another node to the cluster and re-allocate the shards
                final String newNodeName = internalCluster().startDataOnlyNode(nodeAttributes);
                try {
                    for (int shardId : shardsToRelocate) {
                        ClusterRerouteUtils.reroute(client(), new AllocateEmptyPrimaryAllocationCommand(index, shardId, newNodeName, true));
                    }
                    ensureGreen(TimeValue.timeValueMinutes(2), index);

                    // index some more documents
                    for (int i = numDocs; i < numDocs * 2; i++) {
                        String id = Integer.toString(i);
                        prepareIndex(index).setId(id).setSource("value", i).get();
                    }
                    refresh(index);

                    // ensure that we now see at least numDocs results from the updated index
                    assertResponse(prepareSearch().setQuery(new MatchAllQueryBuilder()), resp -> {
                        assertThat(resp.getSuccessfulShards(), equalTo(numShards));
                        assertThat(resp.getFailedShards(), equalTo(0));
                        assertNotNull(resp.getHits().getTotalHits());
                        assertThat(resp.getHits().getTotalHits().value, greaterThan((long) numDocs));
                    });

                    // ensure that when using the previously created PIT, we'd see the same number of documents as before regardless of the
                    // newly indexed documents
                    assertResponse(
                        prepareSearch().setQuery(new MatchAllQueryBuilder())
                            .setPointInTime(new PointInTimeBuilder(pointInTimeResponseOneNodeDown.getPointInTimeId())),
                        resp -> {
                            assertThat(resp.pointInTimeId(), equalTo(pointInTimeResponseOneNodeDown.getPointInTimeId()));
                            assertThat(resp.getTotalShards(), equalTo(numShards));
                            assertThat(resp.getSuccessfulShards(), equalTo(numShards - shardsRemoved));
                            assertThat(resp.getFailedShards(), equalTo(shardsRemoved));
                            assertThat(resp.getShardFailures().length, equalTo(shardsRemoved));
                            for (var failure : resp.getShardFailures()) {
                                assertTrue(shardsToRelocate.contains(failure.shardId()));
                                assertThat(failure.getCause(), instanceOf(NoShardAvailableActionException.class));
                            }
                            assertNotNull(resp.getHits().getTotalHits());
                            // we expect less documents as the newly indexed ones should not be part of the PIT
                            assertThat(resp.getHits().getTotalHits().value, lessThan((long) numDocs));
                        }
                    );

                    Exception exc = expectThrows(
                        Exception.class,
                        () -> prepareSearch().setQuery(new MatchAllQueryBuilder())
                            .setPointInTime(new PointInTimeBuilder(pointInTimeResponseOneNodeDown.getPointInTimeId()))
                            .setAllowPartialSearchResults(false)
                            .get()
                    );
                    assertThat(exc.getCause().getMessage(), containsString("missing shards"));

                } finally {
                    internalCluster().stopNode(newNodeName);
                }
            } finally {
                closePointInTime(pointInTimeResponseOneNodeDown.getPointInTimeId());
            }

        } finally {
            closePointInTime(pointInTimeResponse.getPointInTimeId());
            internalCluster().stopNode(masterNode);
            for (String dataNode : dataNodes) {
                internalCluster().stopNode(dataNode);
            }
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

    private OpenPointInTimeResponse openPointInTime(String[] indices, TimeValue keepAlive) {
        return openPointInTime(indices, keepAlive, false);
    }

    private OpenPointInTimeResponse openPointInTime(String[] indices, TimeValue keepAlive, boolean allowPartialSearchResults) {
        OpenPointInTimeRequest request = new OpenPointInTimeRequest(indices).keepAlive(keepAlive)
            .allowPartialSearchResults(allowPartialSearchResults);
        return client().execute(TransportOpenPointInTimeAction.TYPE, request).actionGet();
    }

    private void closePointInTime(BytesReference readerId) {
        client().execute(TransportClosePointInTimeAction.TYPE, new ClosePointInTimeRequest(readerId)).actionGet();
    }
}
