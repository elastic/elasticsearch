/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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
package org.elasticsearch.index.seqno;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.replication.ReplicationResponse;
import org.elasticsearch.cluster.routing.AllocationId;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.IndexSettingsModule;
import org.junit.Before;

import java.util.Collections;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.elasticsearch.index.seqno.SequenceNumbers.UNASSIGNED_SEQ_NO;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

public class PeerRecoveryRetentionLeaseExpiryTests extends ReplicationTrackerTestCase {

    private static final ActionListener<ReplicationResponse> EMPTY_LISTENER = ActionListener.wrap(() -> { });

    private ReplicationTracker replicationTracker;
    private AtomicLong currentTimeMillis;
    private Settings settings;

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

        final long primaryTerm = randomLongBetween(1, Long.MAX_VALUE);
        replicationTracker = new ReplicationTracker(
            new ShardId("test", "_na", 0),
            primaryAllocationId.getId(),
            IndexSettingsModule.newIndexSettings("test", settings),
            primaryTerm,
            UNASSIGNED_SEQ_NO,
            value -> { },
            currentTimeMillis::get,
            (leases, listener) -> { });
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

        final Tuple<Boolean, RetentionLeases> retentionLeases = replicationTracker.getRetentionLeases(true);
        assertFalse(retentionLeases.v1());

        final Set<String> leaseIds = retentionLeases.v2().leases().stream().map(RetentionLease::id).collect(Collectors.toSet());
        assertThat(leaseIds, hasSize(2));
        assertThat(leaseIds, equalTo(replicationTracker.routingTable.shards().stream()
            .map(ReplicationTracker::getPeerRecoveryRetentionLeaseId).collect(Collectors.toSet())));
    }

    public void testPeerRecoveryRetentionLeasesForUnassignedCopiesDoNotExpireImmediatelyIfShardsNotAllStarted() {
        final String unknownNodeId = randomAlphaOfLength(10);
        replicationTracker.addPeerRecoveryRetentionLease(unknownNodeId, randomCheckpoint(), EMPTY_LISTENER);

        currentTimeMillis.set(currentTimeMillis.get()
            + randomLongBetween(0, IndexSettings.INDEX_SOFT_DELETES_RETENTION_LEASE_PERIOD_SETTING.get(settings).millis()));

        final Tuple<Boolean, RetentionLeases> retentionLeases = replicationTracker.getRetentionLeases(true);
        assertFalse("should not have expired anything", retentionLeases.v1());

        final Set<String> leaseIds = retentionLeases.v2().leases().stream().map(RetentionLease::id).collect(Collectors.toSet());
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
        replicationTracker.addPeerRecoveryRetentionLease(unknownNodeId, randomCheckpoint(), EMPTY_LISTENER);

        currentTimeMillis.set(randomLongBetween(
            currentTimeMillis.get() + IndexSettings.INDEX_SOFT_DELETES_RETENTION_LEASE_PERIOD_SETTING.get(settings).millis() + 1,
            Long.MAX_VALUE));

        final Tuple<Boolean, RetentionLeases> retentionLeases = replicationTracker.getRetentionLeases(true);
        assertTrue("should have expired something", retentionLeases.v1());

        final Set<String> leaseIds = retentionLeases.v2().leases().stream().map(RetentionLease::id).collect(Collectors.toSet());
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

        final Tuple<Boolean, RetentionLeases> retentionLeases = replicationTracker.getRetentionLeases(true);
        assertTrue(retentionLeases.v1());

        final Set<String> leaseIds = retentionLeases.v2().leases().stream().map(RetentionLease::id).collect(Collectors.toSet());
        assertThat(leaseIds, hasSize(2));
        assertThat(leaseIds, equalTo(replicationTracker.routingTable.shards().stream()
            .map(ReplicationTracker::getPeerRecoveryRetentionLeaseId).collect(Collectors.toSet())));
    }
}
