/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.action.support.replication;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.replication.ESIndexLevelReplicationTestCase;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

/**
 * <p>The scenario: primary P and in-sync replica R1. R1 is lagging — it has not yet acknowledged
 * the most recent writes, so its tracked local checkpoint on the primary (4) is behind the primary's
 * own local checkpoint (8). Global checkpoint is therefore pinned at 4. While a new write is being replicated,
 * R1 is concurrently kicked out of the in-sync set (simulating a cluster-state update that marks
 * the stale replica as failed). Removing R1 from the in-sync set advances global checkpoint to 8.
 *
 * <p>The race is injected inside {@code getReplicationGroup()}: the pre-kick group snapshot is
 * captured first and returned to {@link ReplicationOperation} so that R1 always appears as a
 * replication target.
 *
 * <p>The test asserts that replica R1 receives global checkpoint 4 (pre-kick), which is valid
 * (as it's ≤ R1's local checkpoint).
 */
public class ReplicationGroupGlobalCheckpointRaceTests extends ESIndexLevelReplicationTestCase {

    public void testReplicaDoesNotReceiveInvalidGlobalCheckpointWhenKickedConcurrently() throws Exception {
        try (ReplicationGroup group = createGroup(1)) {
            group.startAll();

            final IndexShard primaryShard = group.getPrimary();
            final IndexShard replicaShard = group.getReplicas().get(0);
            final String primaryAllocId = primaryShard.routingEntry().allocationId().getId();
            final String replicaAllocId = replicaShard.routingEntry().allocationId().getId();

            // Index 5 docs through the full replication path so both shards are at lc=4 and GCP=4.
            group.indexDocs(5);
            assertThat(primaryShard.getLocalCheckpoint(), equalTo(4L));
            assertThat(replicaShard.getLocalCheckpoint(), equalTo(4L));
            assertThat(primaryShard.getLastKnownGlobalCheckpoint(), equalTo(4L));

            // Simulate a lagging replica: index 4 more docs directly on the primary, bypassing
            // replication. R1's tracked local checkpoint on the primary stays at 4.
            for (int i = 0; i < 4; i++) {
                indexOnPrimary(new IndexRequest(index.getName()).id("lagging-" + i).source("{}", XContentType.JSON), primaryShard);
            }
            assertThat(primaryShard.getLocalCheckpoint(), equalTo(8L));
            assertThat(replicaShard.getLocalCheckpoint(), equalTo(4L));

            // Advance the primary's own entry in the replication tracker so the replication group
            // reflects the real engine local checkpoint. Without this, GCP would not advance after
            // the kick because the tracker still thinks the primary is at lc=4.
            primaryShard.updateLocalCheckpointForShard(primaryAllocId, primaryShard.getLocalCheckpoint()); // 8

            // GCP = min(tracker.primary.lc=8, replica.tracked_lc=4) = 4: replica's stale entry pins GCP.
            assertThat(primaryShard.getLastKnownGlobalCheckpoint(), equalTo(4L));

            // --- Primary wrapper that injects the race ---
            final AtomicBoolean kicked = new AtomicBoolean(false);
            final AtomicLong capturedGlobalCheckpointForReplica = new AtomicLong(Long.MIN_VALUE);

            final ReplicationOperation.Primary<
                ReplicationOperationTests.Request,
                ReplicationOperationTests.Request,
                ReplicationOperationTests.TestPrimary.Result> primaryWrapper = new ReplicationOperation.Primary<>() {

                    @Override
                    public ShardRouting routingEntry() {
                        return primaryShard.routingEntry();
                    }

                    @Override
                    public void failShard(String message, Exception exception) {
                        throw new AssertionError("shard should not be failed: " + message, exception);
                    }

                    @Override
                    public void perform(
                        ReplicationOperationTests.Request request,
                        ActionListener<ReplicationOperationTests.TestPrimary.Result> listener
                    ) {
                        request.processedOnPrimary.compareAndSet(false, true);
                        listener.onResponse(new ReplicationOperationTests.TestPrimary.Result(request));
                    }

                    @Override
                    public void updateLocalCheckpointForShard(String allocationId, long checkpoint) {
                        primaryShard.updateLocalCheckpointForShard(allocationId, checkpoint);
                    }

                    @Override
                    public void updateGlobalCheckpointForShard(String allocationId, long globalCheckpoint) {
                        primaryShard.updateGlobalCheckpointForShard(allocationId, globalCheckpoint);
                    }

                    @Override
                    public long localCheckpoint() {
                        return primaryShard.getLocalCheckpoint();
                    }

                    @Override
                    public long globalCheckpoint() {
                        return primaryShard.getLastSyncedGlobalCheckpoint();
                    }

                    @Override
                    public long computedGlobalCheckpoint() {
                        return primaryShard.getLastKnownGlobalCheckpoint();
                    }

                    @Override
                    public long maxSeqNoOfUpdatesOrDeletes() {
                        return primaryShard.getMaxSeqNoOfUpdatesOrDeletes();
                    }

                    @Override
                    public org.elasticsearch.index.shard.ReplicationGroup getReplicationGroup() {
                        final org.elasticsearch.index.shard.ReplicationGroup preKickGroup = primaryShard.getReplicationGroup();
                        if (kicked.compareAndSet(false, true)) {
                            // Remove replica from the in-sync set, simulating a concurrent cluster-state update that marks the lagging
                            // replica as stale.
                            try {
                                primaryShard.updateShardState(
                                    primaryShard.routingEntry(),
                                    primaryShard.getPendingPrimaryTerm(),
                                    null,
                                    currentClusterStateVersion.incrementAndGet(),
                                    Set.of(primaryAllocId),
                                    new IndexShardRoutingTable.Builder(primaryShard.shardId()).addShard(primaryShard.routingEntry()).build()
                                );
                            } catch (IOException e) {
                                throw new UncheckedIOException(e);
                            }
                        }
                        // but still return the replication group containing the replica (to simulate concurrency)
                        return preKickGroup;
                    }

                    @Override
                    public PendingReplicationActions getPendingReplicationActions() {
                        // The primary's real PendingReplicationActions has been notified of the kick and no longer tracks the replica.
                        // Return the pre-seeded fresh instance so replica's action is not immediately canceled.
                        // This effectively eliminates the protection mechanism from production code that ensures that
                        // global checkpoints > local checkpoints never reach replicas.
                        final PendingReplicationActions freshPendingReplicationActions = new PendingReplicationActions(
                            primaryShard.shardId(),
                            threadPool
                        );
                        freshPendingReplicationActions.acceptNewTrackedAllocationIds(Set.of(primaryAllocId, replicaAllocId));
                        return freshPendingReplicationActions;
                    }
                };

            // --- Replicas proxy that captures the GCP forwarded to R1 ---
            final ReplicationOperation.Replicas<ReplicationOperationTests.Request> replicasProxy = new ReplicationOperation.Replicas<>() {
                @Override
                public void performOn(
                    ShardRouting replica,
                    ReplicationOperationTests.Request request,
                    long primaryTerm,
                    long globalCheckpoint,
                    long maxSeqNoOfUpdatesOrDeletes,
                    ActionListener<ReplicationOperation.ReplicaResponse> listener
                ) {
                    if (replica.allocationId().getId().equals(replicaAllocId)) {
                        capturedGlobalCheckpointForReplica.set(globalCheckpoint);
                    }
                    listener.onResponse(new ReplicationOperationTests.ReplicaResponse(replicaShard.getLocalCheckpoint(), globalCheckpoint));
                }

                @Override
                public void failShardIfNeeded(
                    ShardRouting replica,
                    long primaryTerm,
                    String message,
                    Exception exception,
                    ActionListener<Void> listener
                ) {
                    listener.onResponse(null);
                }

                @Override
                public void markShardCopyAsStaleIfNeeded(
                    ShardId shardId,
                    String allocationId,
                    long primaryTerm,
                    ActionListener<Void> listener
                ) {
                    listener.onResponse(null);
                }
            };

            final ReplicationOperationTests.Request request = new ReplicationOperationTests.Request(primaryShard.shardId());
            final PlainActionFuture<ReplicationOperationTests.TestPrimary.Result> future = new PlainActionFuture<>();
            new ReplicationOperation<>(
                request,
                primaryWrapper,
                future,
                replicasProxy,
                logger,
                threadPool,
                "test",
                primaryShard.getPendingPrimaryTerm(),
                TimeValue.timeValueMillis(50),
                TimeValue.timeValueSeconds(1)
            ).execute();
            future.actionGet();

            assertTrue("the kick must have been triggered inside getReplicationGroup()", kicked.get());
            assertNotEquals("replica must have received a replication request", Long.MIN_VALUE, capturedGlobalCheckpointForReplica.get());
            // The global checkpoint forwarded to replica must not exceed replica's local checkpoint
            assertThat(
                "global checkpoint forwarded to replica must be the pre-kick value (≤ replica's local checkpoint of 4)",
                capturedGlobalCheckpointForReplica.get(),
                equalTo(4L)
            );

            group.removeReplica(replicaShard);
            closeShards(replicaShard);
        }
    }
}
