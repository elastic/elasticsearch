/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.replication;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.replication.ReplicationResponse;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.seqno.RetentionLease;
import org.elasticsearch.index.seqno.RetentionLeaseSyncAction;
import org.elasticsearch.index.seqno.RetentionLeaseUtils;
import org.elasticsearch.index.seqno.RetentionLeases;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.VersionUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

public class RetentionLeasesReplicationTests extends ESIndexLevelReplicationTestCase {

    public void testSimpleSyncRetentionLeases() throws Exception {
        Settings settings = Settings.builder().put(IndexSettings.INDEX_SOFT_DELETES_SETTING.getKey(), true).build();
        try (ReplicationGroup group = createGroup(between(0, 2), settings)) {
            group.startAll();
            List<RetentionLease> leases = new ArrayList<>();
            int iterations = between(1, 100);
            CountDownLatch latch = new CountDownLatch(iterations);
            for (int i = 0; i < iterations; i++) {
                if (leases.isEmpty() == false && rarely()) {
                    RetentionLease leaseToRemove = randomFrom(leases);
                    leases.remove(leaseToRemove);
                    group.removeRetentionLease(leaseToRemove.id(), ActionListener.running(latch::countDown));
                } else {
                    RetentionLease newLease = group.addRetentionLease(
                        Integer.toString(i),
                        randomNonNegativeLong(),
                        "test-" + i,
                        ActionListener.running(latch::countDown)
                    );
                    leases.add(newLease);
                }
            }
            RetentionLeases leasesOnPrimary = group.getPrimary().getRetentionLeases();
            assertThat(leasesOnPrimary.version(), equalTo(iterations + group.getReplicas().size() + 1L));
            assertThat(leasesOnPrimary.primaryTerm(), equalTo(group.getPrimary().getOperationPrimaryTerm()));
            assertThat(
                RetentionLeaseUtils.toMapExcludingPeerRecoveryRetentionLeases(leasesOnPrimary).values(),
                containsInAnyOrder(leases.toArray(new RetentionLease[0]))
            );
            latch.await();
            for (IndexShard replica : group.getReplicas()) {
                assertThat(replica.getRetentionLeases(), equalTo(leasesOnPrimary));
            }
        }
    }

    public void testOutOfOrderRetentionLeasesRequests() throws Exception {
        Settings settings = Settings.builder().put(IndexSettings.INDEX_SOFT_DELETES_SETTING.getKey(), true).build();
        int numberOfReplicas = between(1, 2);
        IndexMetadata indexMetadata = buildIndexMetadata(numberOfReplicas, settings, indexMapping);
        try (ReplicationGroup group = new ReplicationGroup(indexMetadata) {
            @Override
            protected void syncRetentionLeases(ShardId id, RetentionLeases leases, ActionListener<ReplicationResponse> listener) {
                listener.onResponse(new SyncRetentionLeasesResponse(new RetentionLeaseSyncAction.Request(id, leases)));
            }
        }) {
            group.startAll();
            int numLeases = between(1, 10);
            List<RetentionLeaseSyncAction.Request> requests = new ArrayList<>();
            for (int i = 0; i < numLeases; i++) {
                PlainActionFuture<ReplicationResponse> future = new PlainActionFuture<>();
                group.addRetentionLease(Integer.toString(i), randomNonNegativeLong(), "test-" + i, future);
                requests.add(((SyncRetentionLeasesResponse) future.actionGet()).syncRequest);
            }
            RetentionLeases leasesOnPrimary = group.getPrimary().getRetentionLeases();
            for (IndexShard replica : group.getReplicas()) {
                Randomness.shuffle(requests);
                requests.forEach(request -> group.executeRetentionLeasesSyncRequestOnReplica(request, replica));
                assertThat(replica.getRetentionLeases(), equalTo(leasesOnPrimary));
            }
        }
    }

    public void testSyncRetentionLeasesWithPrimaryPromotion() throws Exception {
        Settings settings = Settings.builder().put(IndexSettings.INDEX_SOFT_DELETES_SETTING.getKey(), true).build();
        int numberOfReplicas = between(2, 4);
        IndexMetadata indexMetadata = buildIndexMetadata(numberOfReplicas, settings, indexMapping);
        try (ReplicationGroup group = new ReplicationGroup(indexMetadata) {
            @Override
            protected void syncRetentionLeases(ShardId id, RetentionLeases leases, ActionListener<ReplicationResponse> listener) {
                listener.onResponse(new SyncRetentionLeasesResponse(new RetentionLeaseSyncAction.Request(id, leases)));
            }
        }) {
            group.startAll();
            for (IndexShard replica : group.getReplicas()) {
                replica.updateRetentionLeasesOnReplica(group.getPrimary().getRetentionLeases());
            }
            int numLeases = between(1, 100);
            IndexShard newPrimary = randomFrom(group.getReplicas());
            RetentionLeases latestRetentionLeasesOnNewPrimary = newPrimary.getRetentionLeases();
            for (int i = 0; i < numLeases; i++) {
                PlainActionFuture<ReplicationResponse> addLeaseFuture = new PlainActionFuture<>();
                group.addRetentionLease(Integer.toString(i), randomNonNegativeLong(), "test-" + i, addLeaseFuture);
                RetentionLeaseSyncAction.Request request = ((SyncRetentionLeasesResponse) addLeaseFuture.actionGet()).syncRequest;
                for (IndexShard replica : randomSubsetOf(group.getReplicas())) {
                    group.executeRetentionLeasesSyncRequestOnReplica(request, replica);
                    if (newPrimary == replica) {
                        latestRetentionLeasesOnNewPrimary = request.getRetentionLeases();
                    }
                }
            }
            group.promoteReplicaToPrimary(newPrimary).get();
            // we need to make changes to retention leases to sync it to replicas
            // since we don't sync retention leases when promoting a new primary.
            PlainActionFuture<ReplicationResponse> newLeaseFuture = new PlainActionFuture<>();
            group.addRetentionLease("new-lease-after-promotion", randomNonNegativeLong(), "test", newLeaseFuture);
            RetentionLeases leasesOnPrimary = group.getPrimary().getRetentionLeases();
            assertThat(leasesOnPrimary.primaryTerm(), equalTo(group.getPrimary().getOperationPrimaryTerm()));
            assertThat(leasesOnPrimary.version(), equalTo(latestRetentionLeasesOnNewPrimary.version() + 1));
            assertThat(leasesOnPrimary.leases(), hasSize(latestRetentionLeasesOnNewPrimary.leases().size() + 1));
            RetentionLeaseSyncAction.Request request = ((SyncRetentionLeasesResponse) newLeaseFuture.actionGet()).syncRequest;
            for (IndexShard replica : group.getReplicas()) {
                group.executeRetentionLeasesSyncRequestOnReplica(request, replica);
            }
            for (IndexShard replica : group.getReplicas()) {
                assertThat(replica.getRetentionLeases(), equalTo(leasesOnPrimary));
            }
        }
    }

    public void testTurnOffTranslogRetentionAfterAllShardStarted() throws Exception {
        final Settings.Builder settings = Settings.builder().put(IndexSettings.INDEX_SOFT_DELETES_SETTING.getKey(), true);
        if (randomBoolean()) {
            settings.put(IndexMetadata.SETTING_VERSION_CREATED, VersionUtils.randomIndexCompatibleVersion(random()));
        }
        try (ReplicationGroup group = createGroup(between(1, 2), settings.build())) {
            group.startAll();
            group.indexDocs(randomIntBetween(1, 10));
            for (IndexShard shard : group) {
                shard.updateShardState(
                    shard.routingEntry(),
                    shard.getOperationPrimaryTerm(),
                    null,
                    1L,
                    group.getPrimary().getReplicationGroup().getInSyncAllocationIds(),
                    group.getPrimary().getReplicationGroup().getRoutingTable()
                );
            }
            group.syncGlobalCheckpoint();
            group.flush();
            assertBusy(() -> {
                // we turn off the translog retention policy using the generic threadPool
                for (IndexShard shard : group) {
                    assertThat(shard.translogStats().estimatedNumberOfOperations(), equalTo(0));
                }
            });
        }
    }

    static final class SyncRetentionLeasesResponse extends ReplicationResponse {
        final RetentionLeaseSyncAction.Request syncRequest;

        SyncRetentionLeasesResponse(RetentionLeaseSyncAction.Request syncRequest) {
            this.syncRequest = syncRequest;
        }
    }
}
