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
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.routing.AllocationId;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardTestCase;
import org.elasticsearch.index.shard.ReplicationGroup;
import org.elasticsearch.index.shard.ShardId;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.cluster.routing.TestShardRouting.shardRoutingBuilder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

/**
 * Integration-level tests for {@link ReplicationOperation#handlePrimaryResult} that exercise a real
 * {@link IndexShard} and its {@link org.elasticsearch.index.seqno.ReplicationTracker} to verify that
 * the global checkpoint is sampled <em>before</em> the replication group is read.
 */
public class ReplicationOperationWithRealShardTests extends IndexShardTestCase {

    /**
     * Verifies the ordering guarantee in {@link ReplicationOperation#handlePrimaryResult}: the global checkpoint
     * must be sampled <em>before</em> the replication group, using a real {@link IndexShard} and its
     * {@link org.elasticsearch.index.seqno.ReplicationTracker}.
     *
     * <p>Setup:
     * <ul>
     *   <li>Primary P with 9 indexed docs (engine lc = 8).</li>
     *   <li>In-sync replica R1 whose tracked local checkpoint on the primary is 4, constraining GCP to 4.</li>
     *   <li>Tracked-but-not-in-sync replica R2 (simulates a shard mid-recovery).</li>
     * </ul>
     *
     * <p>Race injection: on the first call to {@code getReplicationGroup()}, a real
     * {@link IndexShard#updateShardState} removes R1 from the in-sync set, advancing GCP to 8 (primary.lc).
     *
     * <p>Assertion: R2 must receive a global checkpoint of 4 (the pre-kick value), not 8 (the post-kick value
     * that would appear if GCP were sampled after the replication group).
     */
    public void testGlobalCheckpointSampledBeforeReplicationGroupWithRealShard() throws Exception {
        final IndexShard primary = newStartedShard(true);
        try {
            final ShardId shardId = primary.shardId();
            final long primaryTerm = primary.getPendingPrimaryTerm();
            final String primaryAllocId = primary.routingEntry().allocationId().getId();

            // Index 5 docs directly (bypass replication) to advance the engine's local checkpoint to 4.
            for (int i = 0; i < 5; i++) {
                indexDoc(primary, "_doc", Integer.toString(i));
            }
            assertThat(primary.getLocalCheckpoint(), equalTo(4L));

            // Reflect the primary's new local checkpoint in the replication tracker so that GCP can be computed.
            primary.updateLocalCheckpointForShard(primaryAllocId, primary.getLocalCheckpoint());
            // GCP = 4 (sole in-sync copy is the primary itself at lc=4).

            // --- Set up R1 as in-sync with a tracked local checkpoint of 4 ---
            // R1 represents a replica that fully caught up at seqno 4 but has not since replicated anything.
            final AllocationId r1AllocId = AllocationId.newInitializing();
            final ShardRouting r1InitRouting = shardRoutingBuilder(shardId, "node-r1", false, ShardRoutingState.INITIALIZING)
                .withAllocationId(r1AllocId)
                .withRecoverySource(RecoverySource.PeerRecoverySource.INSTANCE)
                .build();

            // Register R1 in the tracker as untracked (via a cluster-state update that adds it as initializing).
            final IndexShardRoutingTable routingWithR1 = new IndexShardRoutingTable.Builder(shardId).addShard(primary.routingEntry())
                .addShard(r1InitRouting)
                .build();
            primary.updateShardState(
                primary.routingEntry(),
                primaryTerm,
                null,
                currentClusterStateVersion.incrementAndGet(),
                Set.of(primaryAllocId),
                routingWithR1
            );

            // Add a peer-recovery retention lease for R1 before initiating tracking; the ReplicationTracker
            // invariant requires that every tracked shard has a corresponding retention lease.
            primary.cloneLocalPeerRecoveryRetentionLease(r1InitRouting.currentNodeId(), ActionListener.noop());
            primary.initiateTracking(r1AllocId.getId());           // tracked=true, inSync=false
            // R1's local checkpoint (4) >= GCP (4), so markAllocationIdAsInSync completes immediately.
            primary.markAllocationIdAsInSync(r1AllocId.getId(), 4L); // inSync=true, lc=4

            // Advance the cluster state to reflect R1 as STARTED and fully in-sync.
            final ShardRouting r1StartedRouting = r1InitRouting.moveToStarted(ShardRouting.UNAVAILABLE_EXPECTED_SHARD_SIZE);
            final IndexShardRoutingTable routingWithR1Started = new IndexShardRoutingTable.Builder(shardId).addShard(primary.routingEntry())
                .addShard(r1StartedRouting)
                .build();
            primary.updateShardState(
                primary.routingEntry(),
                primaryTerm,
                null,
                currentClusterStateVersion.incrementAndGet(),
                Set.of(primaryAllocId, r1AllocId.getId()),
                routingWithR1Started
            );

            // Index 4 more docs directly (bypass replication). R1's tracked_lc on the primary stays at 4.
            for (int i = 5; i < 9; i++) {
                indexDoc(primary, "_doc", Integer.toString(i));
            }
            assertThat(primary.getLocalCheckpoint(), equalTo(8L));

            // Reflect the primary's updated local checkpoint in the tracker.
            primary.updateLocalCheckpointForShard(primaryAllocId, primary.getLocalCheckpoint());
            // GCP = min(8, 4) = 4 — R1's stale tracked entry keeps GCP from advancing beyond 4.
            assertThat(primary.getLastKnownGlobalCheckpoint(), equalTo(4L));

            // --- Set up R2 as tracked but not in-sync (simulates a shard mid-recovery) ---
            final AllocationId r2AllocId = AllocationId.newInitializing();
            final ShardRouting r2Routing = shardRoutingBuilder(shardId, "node-r2", false, ShardRoutingState.INITIALIZING).withAllocationId(
                r2AllocId
            ).withRecoverySource(RecoverySource.PeerRecoverySource.INSTANCE).build();

            final IndexShardRoutingTable routingWithR1R2 = new IndexShardRoutingTable.Builder(shardId).addShard(primary.routingEntry())
                .addShard(r1StartedRouting)
                .addShard(r2Routing)
                .build();
            primary.updateShardState(
                primary.routingEntry(),
                primaryTerm,
                null,
                currentClusterStateVersion.incrementAndGet(),
                Set.of(primaryAllocId, r1AllocId.getId()),
                routingWithR1R2
            );
            // Add a peer-recovery retention lease for R2 before initiating tracking; same invariant applies.
            primary.cloneLocalPeerRecoveryRetentionLease(r2Routing.currentNodeId(), ActionListener.noop());
            primary.initiateTracking(r2AllocId.getId()); // tracked=true, inSync=false; GCP unchanged at 4

            final long gcpBeforeKick = primary.getLastKnownGlobalCheckpoint();
            assertThat(gcpBeforeKick, equalTo(4L));

            // Routing table and in-sync set after the simulated kick (R1 removed).
            final IndexShardRoutingTable routingAfterKick = new IndexShardRoutingTable.Builder(shardId).addShard(primary.routingEntry())
                .addShard(r2Routing)
                .build();
            final Set<String> inSyncAfterKick = Set.of(primaryAllocId);

            // --- Primary wrapper that injects the race ---
            // On the first call to getReplicationGroup(), R1 is removed from the in-sync set via a real
            // IndexShard.updateShardState() call. This simulates a concurrent cluster-state update that
            // marks R1 as stale, advancing GCP from 4 to 8 (primary.lc). The wrapper then delegates to the
            // real shard to return the updated replication group.
            final AtomicBoolean kicked = new AtomicBoolean(false);
            final ReplicationOperation.Primary<
                ReplicationOperationTests.Request,
                ReplicationOperationTests.Request,
                ReplicationOperationTests.TestPrimary.Result> primaryWrapper = new ReplicationOperation.Primary<>() {

                    @Override
                    public ShardRouting routingEntry() {
                        return primary.routingEntry();
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
                        primary.updateLocalCheckpointForShard(allocationId, checkpoint);
                    }

                    @Override
                    public void updateGlobalCheckpointForShard(String allocationId, long globalCheckpoint) {
                        primary.updateGlobalCheckpointForShard(allocationId, globalCheckpoint);
                    }

                    @Override
                    public long localCheckpoint() {
                        return primary.getLocalCheckpoint();
                    }

                    @Override
                    public long globalCheckpoint() {
                        return primary.getLastSyncedGlobalCheckpoint();
                    }

                    @Override
                    public long computedGlobalCheckpoint() {
                        return primary.getLastKnownGlobalCheckpoint();
                    }

                    @Override
                    public long maxSeqNoOfUpdatesOrDeletes() {
                        return primary.getMaxSeqNoOfUpdatesOrDeletes();
                    }

                    @Override
                    public ReplicationGroup getReplicationGroup() {
                        if (kicked.compareAndSet(false, true)) {
                            // Simulate a concurrent cluster-state update that removes R1 from the in-sync set.
                            // Removing R1's constraint causes GCP to advance from 4 to primary.lc (8).
                            try {
                                primary.updateShardState(
                                    primary.routingEntry(),
                                    primaryTerm,
                                    null,
                                    currentClusterStateVersion.incrementAndGet(),
                                    inSyncAfterKick,
                                    routingAfterKick
                                );
                            } catch (IOException e) {
                                throw new UncheckedIOException(e);
                            }
                        }
                        return primary.getReplicationGroup();
                    }

                    @Override
                    public PendingReplicationActions getPendingReplicationActions() {
                        return primary.getPendingReplicationActions();
                    }
                };

            // --- Replicas proxy that captures the GCP forwarded to R2 ---
            final AtomicReference<Long> capturedGcpForR2 = new AtomicReference<>(null);
            final ReplicationOperation.Replicas<ReplicationOperationTests.Request> replicasProxy = new ReplicationOperation.Replicas<>() {
                @Override
                public void performOn(
                    ShardRouting replica,
                    ReplicationOperationTests.Request request,
                    long pTerm,
                    long globalCheckpoint,
                    long maxSeqNoOfUpdatesOrDeletes,
                    ActionListener<ReplicationOperation.ReplicaResponse> listener
                ) {
                    if (replica.allocationId().getId().equals(r2AllocId.getId())) {
                        capturedGcpForR2.set(globalCheckpoint);
                    }
                    listener.onResponse(new ReplicationOperationTests.ReplicaResponse(0L, globalCheckpoint));
                }

                @Override
                public void failShardIfNeeded(
                    ShardRouting replica,
                    long pTerm,
                    String message,
                    Exception exception,
                    ActionListener<Void> listener
                ) {
                    listener.onResponse(null);
                }

                @Override
                public void markShardCopyAsStaleIfNeeded(ShardId sId, String allocationId, long pTerm, ActionListener<Void> listener) {
                    listener.onResponse(null);
                }
            };

            // Run the ReplicationOperation.
            final ReplicationOperationTests.Request request = new ReplicationOperationTests.Request(shardId);
            final PlainActionFuture<ReplicationOperationTests.TestPrimary.Result> resultFuture = new PlainActionFuture<>();
            new ReplicationOperation<>(
                request,
                primaryWrapper,
                resultFuture,
                replicasProxy,
                logger,
                threadPool,
                "test",
                primaryTerm,
                TimeValue.timeValueMillis(50),
                TimeValue.timeValueSeconds(1)
            ).execute();
            resultFuture.actionGet();

            assertTrue("getReplicationGroup() must have triggered the kick", kicked.get());

            // The GCP forwarded to R2 must equal the pre-kick value (4), not the post-kick value (8).
            // A GCP of 8 forwarded to R2 would violate the invariant that the GCP must not exceed any
            // replica's local checkpoint: R2 has not yet replicated ops 5-8 and cannot honour GCP=8.
            assertThat("R2 must have received a replication request", capturedGcpForR2.get(), notNullValue());
            assertThat(
                "GCP forwarded to R2 must be the pre-kick value (4), captured before R1 was removed from the in-sync set",
                capturedGcpForR2.get(),
                equalTo(gcpBeforeKick)
            );
        } finally {
            closeShards(primary);
        }
    }
}
