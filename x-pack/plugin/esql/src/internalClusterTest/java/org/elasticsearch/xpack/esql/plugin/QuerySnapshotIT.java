/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.SliceSelection;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.search.builder.PointInTimeBuilder;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.transport.AbstractTransportRequest;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.esql.action.AbstractEsqlIntegTestCase;
import org.junit.Before;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.action.support.WriteRequest.RefreshPolicy.IMMEDIATE;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.startsWith;

/**
 * The point in time of a query's snapshot: which shards it holds, and that nothing is left behind however the query ends.
 * A test task stands for the query.
 */
public class QuerySnapshotIT extends AbstractEsqlIntegTestCase {

    private static final int INDICES = 3;
    /**
     * Longer than any test, so only a close frees the readers before it.
     */
    private static final TimeValue LONG = TimeValue.timeValueMinutes(1);

    /**
     * The reaper checks every 100ms, so readers that expire go away within the test.
     */
    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put(SearchService.KEEPALIVE_INTERVAL_SETTING.getKey(), "100ms")
            .build();
    }

    /**
     * Indices {@code old-*} hold 2020, {@code new-*} hold 2026, one shard each with one replica.
     */
    @Before
    public void createIndices() {
        internalCluster().ensureAtLeastNumDataNodes(2);
        List<IndexRequestBuilder> docs = new ArrayList<>();
        for (int i = 0; i < INDICES; i++) {
            for (String era : List.of("old", "new")) {
                String index = era + "-" + i;
                assertAcked(prepareCreate(index).setSettings(indexSettings(1, 1)).setMapping("@timestamp", "type=date"));
                String timestamp = era.equals("old") ? "2020-01-01T00:00:00Z" : "2026-01-01T00:00:00Z";
                for (int d = 0; d < 5; d++) {
                    docs.add(prepareIndex(index).setSource("@timestamp", timestamp));
                }
            }
        }
        indexRandom(true, docs);
        ensureGreen("old-*", "new-*");
    }

    /**
     * The snapshot holds a reader of each shard the filter may match, and closing it frees them.
     */
    public void testHoldsTheShardsTheFilterMayMatch() throws Exception {
        Query query = startQuery(internalCluster().getRandomNodeName());
        QuerySnapshot snapshot = query.open(new RangeQueryBuilder("@timestamp").gte("2025-01-01"), LONG);
        assertThat(snapshot.shards().size(), equalTo(INDICES));
        assertThat(snapshot.shards().keySet().stream().map(ShardId::getIndexName).toList(), everyItem(startsWith("new-")));
        assertThat(activeContexts(), equalTo(INDICES));
        var targets = snapshot.targetShards(clusterService().state().projectState());
        assertThat(targets.totalShards(), equalTo(2 * INDICES));
        assertThat(targets.skippedShards(), equalTo(INDICES));
        query.close(snapshot);
        assertNothingHeld();
        query.end();
    }

    /**
     * Each shard is read on the node that holds its reader: a writable shard's reader is bound to its copy.
     */
    public void testReadsEachShardWhereItsReaderIs() {
        Query query = startQuery(internalCluster().getRandomNodeName());
        QuerySnapshot snapshot = query.open(null, LONG);
        var targets = snapshot.targetShards(clusterService().state().projectState());
        assertThat(targets.shards().size(), equalTo(2 * INDICES));
        for (var target : targets.shards().values()) {
            assertFalse(snapshot.reproducible(target.shardId()));
            assertThat(target.remainingNodes().size(), equalTo(1));
            assertThat(target.remainingNodes().getFirst().getId(), equalTo(snapshot.shards().get(target.shardId()).getNode()));
        }
        query.close(snapshot);
        assertNothingHeld();
        query.end();
    }

    /**
     * A search through the snapshot sees the documents as of its opening, while a fresh one sees later writes.
     */
    public void testHoldsTheVersionItOpenedOn() {
        Query query = startQuery(internalCluster().getRandomNodeName());
        QuerySnapshot snapshot = query.open(null, LONG);
        prepareIndex("old-0").setSource("@timestamp", "2020-01-01T00:00:00Z").setRefreshPolicy(IMMEDIATE).get();
        assertHitCount(prepareSearch().setPointInTime(new PointInTimeBuilder(snapshot.pointInTimeId())).setSize(0), 10 * INDICES);
        assertHitCount(prepareSearch("old-*", "new-*").setSize(0), 10 * INDICES + 1);
        query.close(snapshot);
        assertNothingHeld();
        query.end();
    }

    /**
     * A cancelled query frees its readers at once.
     */
    public void testCancelledQueryFreesTheReaders() {
        Query query = startQuery(internalCluster().getRandomNodeName());
        QuerySnapshot snapshot = query.open(null, LONG);
        assertThat(activeContexts(), equalTo(snapshot.shards().size()));
        query.cancel();
        assertNothingHeld();
        query.end();
    }

    /**
     * A query whose node leaves the cluster before it closes its snapshot doesn't leave its readers behind: they expire.
     */
    public void testReadersOfAQueryWhoseNodeLeftExpire() throws Exception {
        String coordinator = internalCluster().startCoordinatingOnlyNode(Settings.EMPTY);
        Query query = startQuery(coordinator);
        QuerySnapshot snapshot = query.open(null, TimeValue.timeValueSeconds(5));
        assertThat(activeContexts(), equalTo(snapshot.shards().size()));
        internalCluster().stopNode(coordinator);
        assertTrue("the query never closed its snapshot", snapshot.markClosed());
        assertBusy(() -> assertThat("the readers expired", activeContexts(), equalTo(0)), 30, TimeUnit.SECONDS);
    }

    private static Set<String> indices() {
        Set<String> indices = new HashSet<>();
        for (int i = 0; i < INDICES; i++) {
            indices.add("old-" + i);
            indices.add("new-" + i);
        }
        return indices;
    }

    private Query startQuery(String node) {
        TransportService transportService = internalCluster().getInstance(TransportService.class, node);
        CancellableTask task = (CancellableTask) transportService.getTaskManager().register("test", "test:query", new QueryRequest());
        return new Query(node, transportService, task);
    }

    /**
     * A query on {@code node}, which a registered task stands for, like the task of an ES|QL query.
     */
    private record Query(String node, TransportService transportService, CancellableTask task) {
        QuerySnapshot open(@Nullable QueryBuilder filter, TimeValue keepAlive) {
            PlainActionFuture<QuerySnapshot> future = new PlainActionFuture<>();
            QuerySnapshots.open(
                client(),
                internalCluster().getInstance(NamedWriteableRegistry.class, node),
                transportService.getLocalNode().getId(),
                task,
                QuerySnapshots.openRequest(
                    indices().toArray(String[]::new),
                    SearchRequest.DEFAULT_INDICES_OPTIONS,
                    filter,
                    SliceSelection.UNSPECIFIED,
                    false,
                    keepAlive
                ),
                future
            );
            return future.actionGet(30, TimeUnit.SECONDS);
        }

        void close(QuerySnapshot snapshot) {
            QuerySnapshots.close(client(), snapshot);
        }

        void cancel() {
            transportService.getTaskManager().cancel(task, "test", () -> {});
        }

        void end() {
            transportService.getTaskManager().unregister(task);
        }

        private Client client() {
            return internalCluster().client(node);
        }
    }

    private static final class QueryRequest extends AbstractTransportRequest {
        @Override
        public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
            return new CancellableTask(id, type, action, "a test query", parentTaskId, headers);
        }
    }

    private void assertNothingHeld() {
        try {
            assertBusy(() -> assertThat("the readers were freed", activeContexts(), equalTo(0)), 10, TimeUnit.SECONDS);
        } catch (Exception e) {
            throw new AssertionError(e);
        }
    }

    private int activeContexts() {
        int contexts = 0;
        for (SearchService searchService : internalCluster().getInstances(SearchService.class)) {
            contexts += searchService.getActiveContexts();
        }
        return contexts;
    }
}
