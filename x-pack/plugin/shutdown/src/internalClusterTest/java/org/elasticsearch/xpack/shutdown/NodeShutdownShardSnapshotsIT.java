/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.shutdown;

import org.elasticsearch.cluster.SnapshotsInProgress;
import org.elasticsearch.cluster.metadata.ShutdownShardSnapshotsStatus;
import org.elasticsearch.cluster.metadata.SingleNodeShutdownMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.snapshots.AbstractSnapshotIntegTestCase;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.transport.MockTransportService;

import java.util.Collection;

import static org.elasticsearch.test.NodeShutdownTestUtils.clearShutdownMetadata;
import static org.elasticsearch.test.NodeShutdownTestUtils.putShutdownForRemovalMetadata;
import static org.hamcrest.Matchers.equalTo;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0)
public class NodeShutdownShardSnapshotsIT extends AbstractSnapshotIntegTestCase {

    @SuppressWarnings("unchecked")
    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.appendToCopyNoNullElements(super.nodePlugins(), ShutdownPlugin.class, MockTransportService.TestPlugin.class);
    }

    public void testShardSnapshotsStatus() throws Exception {
        internalCluster().startMasterOnlyNode();
        final String nodeForRemoval = internalCluster().startDataOnlyNode(
            Settings.builder()
                // we block a snapshot thread and expect another shard snapshot to complete concurrently
                .put("thread_pool.snapshot.max", 2)
                .build()
        );
        ensureStableCluster(2);
        final String nodeForRemovalId = getNodeId(nodeForRemoval);
        final var clusterService = internalCluster().getCurrentMasterNodeInstance(ClusterService.class);

        final String repoName = randomRepoName();
        createRepository(repoName, "mock");

        final String index1 = randomIndexName();
        createIndex(index1, 1, 0);
        final String index2 = randomIndexName();
        createIndex(index2, 1, 0);
        createFullSnapshot(repoName, "initial-full-snapshot");

        // Index more docs so that the 2nd snapshot has data to write for index1 but not index 2.
        indexRandomDocs(index1, between(10, 100));
        final String snapshotName = randomSnapshotName();
        final var snapshotFuture = startFullSnapshotBlockedOnDataNode(snapshotName, repoName, nodeForRemoval);

        // Wait for index2 to complete its snapshot, then start shutdown so that we observe 1 completed shard snapshot
        safeAwait(
            ClusterServiceUtils.addTemporaryStateListener(
                clusterService,
                state -> SnapshotsInProgress.get(state)
                    .asStream()
                    .anyMatch(
                        entry -> snapshotName.equals(entry.snapshot().getSnapshotId().getName())
                            && entry.shards()
                                .entrySet()
                                .stream()
                                .anyMatch(
                                    shardEntry -> shardEntry.getKey().getIndexName().equals(index2)
                                        && shardEntry.getValue().state().completed()
                                )
                    )
            )
        );
        putShutdownForRemovalMetadata(nodeForRemoval, clusterService);

        // Observe 1 complete shard snapshot and 1 running one
        {
            final var response = safeGet(
                client().execute(
                    GetShutdownStatusAction.INSTANCE,
                    new GetShutdownStatusAction.Request(TEST_REQUEST_TIMEOUT, nodeForRemovalId)
                )
            );
            assertFalse(response.getShutdownStatuses().isEmpty());
            final ShutdownShardSnapshotsStatus snapshotsStatus = response.getShutdownStatuses().getFirst().shardSnapshotsStatus();
            assertThat(snapshotsStatus.status(), equalTo(SingleNodeShutdownMetadata.Status.IN_PROGRESS));
            assertThat(snapshotsStatus.completedShards(), equalTo(1L));
            assertThat(snapshotsStatus.pausedShards(), equalTo(0L));
            assertThat(snapshotsStatus.runningShards(), equalTo(1L));
        }

        // Let pause progress
        unblockNode(repoName, nodeForRemoval);
        safeAwait(
            ClusterServiceUtils.addTemporaryStateListener(
                clusterService,
                state -> SnapshotsInProgress.get(state)
                    .asStream()
                    .anyMatch(
                        entry -> snapshotName.equals(entry.snapshot().getSnapshotId().getName())
                            && entry.shards()
                                .entrySet()
                                .stream()
                                .anyMatch(
                                    shardEntry -> shardEntry.getKey().getIndexName().equals(index1)
                                        && shardEntry.getValue().state() == SnapshotsInProgress.ShardState.PAUSED_FOR_NODE_REMOVAL
                                )
                    )
            )
        );

        // Paused shard snapshot should be observed
        {
            final var response = safeGet(
                client().execute(
                    GetShutdownStatusAction.INSTANCE,
                    new GetShutdownStatusAction.Request(TEST_REQUEST_TIMEOUT, nodeForRemovalId)
                )
            );
            final ShutdownShardSnapshotsStatus snapshotsStatus = response.getShutdownStatuses().getFirst().shardSnapshotsStatus();
            assertThat(snapshotsStatus.status(), equalTo(SingleNodeShutdownMetadata.Status.COMPLETE));
            assertThat(snapshotsStatus.completedShards(), equalTo(1L));
            assertThat(snapshotsStatus.pausedShards(), equalTo(1L));
            assertThat(snapshotsStatus.runningShards(), equalTo(0L));
        }

        clearShutdownMetadata(clusterService);
        safeGet(snapshotFuture);
    }
}
