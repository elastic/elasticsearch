/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.index.seqno;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.replication.ReplicationResponse;
import org.elasticsearch.cluster.routing.AllocationId;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.engine.SafeCommitInfo;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.IndexSettingsModule;
import org.junit.Before;

import java.util.Collections;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.elasticsearch.index.seqno.SequenceNumbers.NO_OPS_PERFORMED;
import static org.elasticsearch.index.seqno.SequenceNumbers.UNASSIGNED_SEQ_NO;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

public class PeerRecoveryRetentionLeaseExpiryTests extends ReplicationTrackerTestCase {

    private static final ActionListener<ReplicationResponse> EMPTY_LISTENER = ActionListener.wrap(() -> { });

    private ReplicationTracker replicationTracker;
    private AtomicLong currentTimeMillis;
    private Settings settings;
    private SafeCommitInfo safeCommitInfo;

    @Before
    public void setUpReplicationTracker() throws InterruptedException {
        final AllocationId primaryAllocationId = AllocationId.newInitializing();
        currentTimeMillis = new AtomicLong(randomLongBetween(0, 1024));

        if (randomBoolean()) {
            settings = Settings.builder()
                .put(IndexSettings.INDEX_SOFT_DELETES_RETENTION_LEASE_PERIOD_SETTING.getKey(),
                    TimeValue.timeValueMillis(randomLongBetween(1, TimeValue.timeValueHours(12).millis())))
                .build();
        } else {
            settings = Settings.EMPTY;
        }

        safeCommitInfo = null; // must be set in each test

        final long primaryTerm = randomLongBetween(1, Long.MAX_VALUE);
        replicationTracker = new ReplicationTracker(
            new ShardId("test", "_na", 0),
            primaryAllocationId.getId(),
            IndexSettingsModule.newIndexSettings("test", settings),
            primaryTerm,
            UNASSIGNED_SEQ_NO,
            value -> { },
            currentTimeMillis::get,
            (leases, listener) -> { },
            () -> safeCommitInfo);
        replicationTracker.updateFromMaster(1L, Collections.singleton(primaryAllocationId.getId()),
            routingTable(Collections.emptySet(), primaryAllocationId));
        replicationTracker.activatePrimaryMode(SequenceNumbers.NO_OPS_PERFORMED);

        final AllocationId replicaAllocationId = AllocationId.newInitializing();
        final IndexShardRoutingTable routingTableWithReplica
            = routingTable(Collections.singleton(replicaAllocationId), primaryAllocationId);
        replicationTracker.updateFromMaster(2L, Collections.singleton(primaryAllocationId.getId()), routingTableWithReplica);
        replicationTracker.addPeerRecoveryRetentionLease(
            routingTableWithReplica.getByAllocationId(replicaAllocationId.getId()).currentNodeId(), randomCheckpoint(),
            EMPTY_LISTENER);

        replicationTracker.initiateTracking(replicaAllocationId.getId());
        replicationTracker.markAllocationIdAsInSync(replicaAllocationId.getId(), randomCheckpoint());
    }

    private long randomCheckpoint() {
        return randomBoolean() ? SequenceNumbers.NO_OPS_PERFORMED : randomNonNegativeLong();
    }

    private void startReplica() {
        final ShardRouting replicaShardRouting = replicationTracker.routingTable.replicaShards().get(0);
        final IndexShardRoutingTable.Builder builder = new IndexShardRoutingTable.Builder(replicationTracker.routingTable);
        builder.removeShard(replicaShardRouting);
        builder.addShard(replicaShardRouting.moveToStarted());
        replicationTracker.updateFromMaster(replicationTracker.appliedClusterStateVersion + 1,
            replicationTracker.routingTable.shards().stream().map(sr -> sr.allocationId().getId()).collect(Collectors.toSet()),
            builder.build());
    }

    public void testPeerRecoveryRetentionLeasesForAssignedCopiesDoNotEverExpire() {
        if (randomBoolean()) {
            startReplica();
        }

        currentTimeMillis.set(currentTimeMillis.get() + randomLongBetween(0, Long.MAX_VALUE - currentTimeMillis.get()));
        safeCommitInfo = randomSafeCommitInfo();

        final RetentionLeases retentionLeases = replicationTracker.getRetentionLeases(true);

        final Set<String> leaseIds = retentionLeases.leases().stream().map(RetentionLease::id).collect(Collectors.toSet());
        assertThat(leaseIds, hasSize(2));
        assertThat(leaseIds, equalTo(replicationTracker.routingTable.shards().stream()
            .map(ReplicationTracker::getPeerRecoveryRetentionLeaseId).collect(Collectors.toSet())));
    }

    public void testPeerRecoveryRetentionLeasesForUnassignedCopiesDoNotExpireImmediatelyIfShardsNotAllStarted() {
        final String unknownNodeId = randomAlphaOfLength(10);
        final long globalCheckpoint = randomNonNegativeLong(); // not NO_OPS_PERFORMED since this always results in file-based recovery
        replicationTracker.addPeerRecoveryRetentionLease(unknownNodeId, globalCheckpoint, EMPTY_LISTENER);

        currentTimeMillis.set(currentTimeMillis.get()
            + randomLongBetween(0, IndexSettings.INDEX_SOFT_DELETES_RETENTION_LEASE_PERIOD_SETTING.get(settings).millis()));

        safeCommitInfo = randomSafeCommitInfoSuitableForOpsBasedRecovery(globalCheckpoint);

        final RetentionLeases retentionLeases = replicationTracker.getRetentionLeases(true);

        final Set<String> leaseIds = retentionLeases.leases().stream().map(RetentionLease::id).collect(Collectors.toSet());
        assertThat(leaseIds, hasSize(3));
        assertThat(leaseIds, equalTo(Stream.concat(Stream.of(ReplicationTracker.getPeerRecoveryRetentionLeaseId(unknownNodeId)),
            replicationTracker.routingTable.shards().stream()
                .map(ReplicationTracker::getPeerRecoveryRetentionLeaseId)).collect(Collectors.toSet())));
    }

    public void testPeerRecoveryRetentionLeasesForUnassignedCopiesExpireEventually() {
        if (randomBoolean()) {
            startReplica();
        }

        final String unknownNodeId = randomAlphaOfLength(10);
        final long globalCheckpoint = randomCheckpoint();
        replicationTracker.addPeerRecoveryRetentionLease(unknownNodeId, globalCheckpoint, EMPTY_LISTENER);

        currentTimeMillis.set(randomLongBetween(
            currentTimeMillis.get() + IndexSettings.INDEX_SOFT_DELETES_RETENTION_LEASE_PERIOD_SETTING.get(settings).millis() + 1,
            Long.MAX_VALUE));

        safeCommitInfo = randomSafeCommitInfoSuitableForOpsBasedRecovery(globalCheckpoint);

        final RetentionLeases retentionLeases = replicationTracker.getRetentionLeases(true);

        final Set<String> leaseIds = retentionLeases.leases().stream().map(RetentionLease::id).collect(Collectors.toSet());
        assertThat(leaseIds, hasSize(2));
        assertThat(leaseIds, equalTo(replicationTracker.routingTable.shards().stream()
            .map(ReplicationTracker::getPeerRecoveryRetentionLeaseId).collect(Collectors.toSet())));
    }

    public void testPeerRecoveryRetentionLeasesForUnassignedCopiesExpireImmediatelyIfShardsAllStarted() {
        final String unknownNodeId = randomAlphaOfLength(10);
        replicationTracker.addPeerRecoveryRetentionLease(unknownNodeId, randomCheckpoint(), EMPTY_LISTENER);

        startReplica();

        currentTimeMillis.set(currentTimeMillis.get() +
            (usually()
                ? randomLongBetween(0, IndexSettings.INDEX_SOFT_DELETES_RETENTION_LEASE_PERIOD_SETTING.get(settings).millis())
                : randomLongBetween(0, Long.MAX_VALUE - currentTimeMillis.get())));
        safeCommitInfo = randomSafeCommitInfo();

        final RetentionLeases retentionLeases = replicationTracker.getRetentionLeases(true);

        final Set<String> leaseIds = retentionLeases.leases().stream().map(RetentionLease::id).collect(Collectors.toSet());
        assertThat(leaseIds, hasSize(2));
        assertThat(leaseIds, equalTo(replicationTracker.routingTable.shards().stream()
            .map(ReplicationTracker::getPeerRecoveryRetentionLeaseId).collect(Collectors.toSet())));
    }

    public void testPeerRecoveryRetentionLeasesForUnassignedCopiesExpireIfRetainingTooMuchHistory() {
        if (randomBoolean()) {
            startReplica();
        }

        final String unknownNodeId = randomAlphaOfLength(10);
        final long globalCheckpoint = randomValueOtherThan(SequenceNumbers.NO_OPS_PERFORMED, this::randomCheckpoint);
        replicationTracker.addPeerRecoveryRetentionLease(unknownNodeId, globalCheckpoint, EMPTY_LISTENER);

        safeCommitInfo = randomSafeCommitInfoSuitableForFileBasedRecovery(globalCheckpoint);

        final RetentionLeases retentionLeases = replicationTracker.getRetentionLeases(true);

        final Set<String> leaseIds = retentionLeases.leases().stream().map(RetentionLease::id).collect(Collectors.toSet());
        assertThat(leaseIds, hasSize(2));
        assertThat(leaseIds, equalTo(replicationTracker.routingTable.shards().stream()
            .map(ReplicationTracker::getPeerRecoveryRetentionLeaseId).collect(Collectors.toSet())));
    }

    private SafeCommitInfo randomSafeCommitInfo() {
        return randomBoolean() ? SafeCommitInfo.EMPTY : new SafeCommitInfo(
            randomFrom(randomNonNegativeLong(), (long) randomIntBetween(0, Integer.MAX_VALUE)),
            randomIntBetween(0, Integer.MAX_VALUE));
    }

    private SafeCommitInfo randomSafeCommitInfoSuitableForOpsBasedRecovery(long globalCheckpoint) {
        // simulate a safe commit that is behind the given global checkpoint, so that no files need to be transferrred
        final long localCheckpoint = randomLongBetween(NO_OPS_PERFORMED, globalCheckpoint);
        return new SafeCommitInfo(localCheckpoint, between(0, Math.toIntExact(Math.min(localCheckpoint + 1, Integer.MAX_VALUE))));
    }

    private SafeCommitInfo randomSafeCommitInfoSuitableForFileBasedRecovery(long globalCheckpoint) {
        // simulate a later safe commit containing no documents, which is always better to transfer than any ops
        return new SafeCommitInfo(randomLongBetween(globalCheckpoint + 1, Long.MAX_VALUE), 0);
    }
}
