/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.snapshots;

import org.elasticsearch.cluster.SnapshotsInProgress;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.CheckedRunnable;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.snapshots.mockstore.MockRepository;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.transport.MockTransportService;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.StreamSupport;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0)
public class SnapshotAndRelocationIT extends AbstractSnapshotIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(MockTransportService.TestPlugin.class, MockRepository.Plugin.class);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put(SnapshotsService.SHARD_SNAPSHOT_PER_NODE_LIMIT_SETTING.getKey(), 1)
            .build();
    }

    public void testLimitingInitAndRelocationForAssignedQueueShards() throws Exception {
        final String masterNode = internalCluster().startMasterOnlyNode();
        final String dataNodeA = internalCluster().startDataOnlyNode();
        final String dataNodeB = internalCluster().startDataOnlyNode();
        ensureStableCluster(3);
        final String repoName = "test-repo";
        createRepository(repoName, "mock");

        final AtomicBoolean delayOnce = new AtomicBoolean(false);
        final AtomicReference<CheckedRunnable<Exception>> delayedAction = new AtomicReference<>();
        final var delayedActionSetLatch = new CountDownLatch(1);
        MockTransportService.getInstance(masterNode)
            .addRequestHandlingBehavior(TransportUpdateSnapshotStatusAction.NAME, (handler, request, channel, task) -> {
                if (delayOnce.compareAndSet(false, true)) {
                    delayedAction.set(() -> handler.messageReceived(request, channel, task));
                    delayedActionSetLatch.countDown();
                } else {
                    handler.messageReceived(request, channel, task);
                }
            });

        final var numIndices = between(2, 4);
        final var indexNames = IntStream.range(0, numIndices).mapToObj(i -> "index-" + i).toList();

        for (var indexName : indexNames) {
            createIndex(indexName, indexSettings(1, 0).put("index.routing.allocation.include._name", dataNodeA).build());
            indexRandomDocs(indexName, between(10, 42));
        }
        ensureGreen();

        final var future = startFullSnapshot(repoName, "snapshot");
        safeAwait(delayedActionSetLatch);

        final var clusterService = internalCluster().getCurrentMasterNodeInstance(ClusterService.class);
        final SnapshotsInProgress.Entry snapshot = SnapshotsInProgress.get(clusterService.state()).asStream().iterator().next();
        logger.info("--> snapshot=[{}]", snapshot);
        final var shards = snapshot.shards();
        assertThat(shards.size(), equalTo(numIndices));

        final var dataNodeAId = getNodeId(dataNodeA);
        final var initShards = shards.entrySet()
            .stream()
            .filter(entry -> entry.getValue().state() == SnapshotsInProgress.ShardState.INIT)
            .peek(entry -> assertThat(entry.getValue().nodeId(), equalTo(dataNodeAId)))
            .map(Map.Entry::getKey)
            .toList();
        logger.info("--> init shards [{}]", initShards);
        assertThat(initShards.size(), equalTo(1));

        final var assignedQueuedShards = shards.entrySet()
            .stream()
            .filter(entry -> entry.getValue().isAssignedQueued())
            .peek(entry -> assertThat(entry.getValue().nodeId(), equalTo(dataNodeAId)))
            .map(Map.Entry::getKey)
            .toList();
        logger.info("--> assigned queued shards [{}]", assignedQueuedShards);
        assertThat(assignedQueuedShards.size(), equalTo(numIndices - 1));

        // Relocate indices that are assigned queued
        final String[] indices = assignedQueuedShards.stream().map(ShardId::getIndexName).toArray(String[]::new);
        logger.info("--> relocate indices [{}]", Arrays.toString(indices));
        updateIndexSettings(Settings.builder().put("index.routing.allocation.include._name", dataNodeB), indices);
        ensureGreen(indices);

        final var dataNodeBIndicesService = internalCluster().getInstance(IndicesService.class, dataNodeB);
        for (var shardId : assignedQueuedShards) {
            assertTrue(
                "indices: "
                    + StreamSupport.stream(dataNodeBIndicesService.spliterator(), false)
                        .map(indexService -> indexService.index().getName())
                        .toList(),
                dataNodeBIndicesService.hasIndex(shardId.getIndex())
            );
        }

        assertThat(future.isDone(), is(false));
        logger.info("--> run delayed action");
        delayedAction.get().run();
        assertSuccessful(future);
    }

    public void testSnapshotStartedEarlierCompletesFirst() throws Exception {
        final String masterNode = internalCluster().startMasterOnlyNode();
        final String dataNode = internalCluster().startDataOnlyNode();
        ensureStableCluster(2);
        final String repoName = "test-repo";
        createRepository(repoName, "mock");

        final var numIndices = between(2, 4);
        final var indexNames = IntStream.range(0, numIndices).mapToObj(i -> "index-" + i).toList();

        for (var indexName : indexNames) {
            createIndex(indexName, 1, 0);
            indexRandomDocs(indexName, between(10, 42));
        }
        ensureGreen();

        final String firstSnapshot = "snapshot-0";
        final String secondSnapshot = "snapshot-1";

        // Start two snapshots and wait for both of them to appear in the cluster state
        blockDataNode(repoName, dataNode);
        final var future0 = startFullSnapshot(repoName, firstSnapshot);
        safeAwait(ClusterServiceUtils.addMasterTemporaryStateListener(state -> {
            final var snapshotsInProgress = SnapshotsInProgress.get(state);
            final List<String> snapshotNames = snapshotsInProgress.asStream()
                .map(entry -> entry.snapshot().getSnapshotId().getName())
                .toList();
            return snapshotNames.equals(List.of(firstSnapshot));
        }));
        final var future1 = startFullSnapshot(repoName, secondSnapshot);
        safeAwait(ClusterServiceUtils.addMasterTemporaryStateListener(state -> {
            final var snapshotsInProgress = SnapshotsInProgress.get(state);
            final List<String> snapshotNames = snapshotsInProgress.asStream()
                .map(entry -> entry.snapshot().getSnapshotId().getName())
                .toList();
            return snapshotNames.equals(List.of(firstSnapshot, secondSnapshot));
        }));

        // Ensure the first snapshot is completed first before the second one by observing a cluster state containing only the 2nd one
        final var listenerForFirstSnapshotCompletion = ClusterServiceUtils.addMasterTemporaryStateListener(state -> {
            final var snapshotsInProgress = SnapshotsInProgress.get(state);
            final Set<String> snapshotNames = snapshotsInProgress.asStream()
                .map(entry -> entry.snapshot().getSnapshotId().getName())
                .collect(Collectors.toSet());
            return snapshotNames.equals(Set.of(secondSnapshot));
        });
        unblockNode(repoName, dataNode);
        safeAwait(listenerForFirstSnapshotCompletion);

        assertSuccessful(future0);
        assertSuccessful(future1);
    }
}
