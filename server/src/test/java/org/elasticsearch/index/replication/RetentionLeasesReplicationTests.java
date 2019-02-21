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

package org.elasticsearch.index.replication;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.seqno.RetentionLease;
import org.elasticsearch.index.seqno.RetentionLeaseSyncAction;
import org.elasticsearch.index.seqno.RetentionLeases;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

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
                    group.removeRetentionLease(leaseToRemove.id(), ActionListener.wrap(latch::countDown));
                } else {
                    RetentionLease newLease = group.addRetentionLease(Integer.toString(i), randomNonNegativeLong(), "test-" + i,
                        ActionListener.wrap(latch::countDown));
                    leases.add(newLease);
                }
            }
            RetentionLeases leasesOnPrimary = group.getPrimary().getRetentionLeases();
            assertThat(leasesOnPrimary.version(), equalTo((long) iterations));
            assertThat(leasesOnPrimary.primaryTerm(), equalTo(group.getPrimary().getOperationPrimaryTerm()));
            assertThat(leasesOnPrimary.leases(), containsInAnyOrder(leases.toArray(new RetentionLease[0])));
            latch.await();
            for (IndexShard replica : group.getReplicas()) {
                assertThat(replica.getRetentionLeases(), equalTo(leasesOnPrimary));
            }
        }
    }

    public void testOutOfOrderRetentionLeasesRequests() throws Exception {
        Settings settings = Settings.builder().put(IndexSettings.INDEX_SOFT_DELETES_SETTING.getKey(), true).build();
        int numberOfReplicas = between(1, 2);
        IndexMetaData indexMetaData = buildIndexMetaData(numberOfReplicas, settings, indexMapping);
        final List<RetentionLeaseSyncAction.Request> requests = new ArrayList<>();
        try (ReplicationGroup group = new ReplicationGroup(indexMetaData) {
            @Override
            protected void syncRetentionLeases(ShardId shardId, RetentionLeases leases, ActionListener<Void> listener) {
                requests.add(new RetentionLeaseSyncAction.Request(shardId, leases));
                listener.onResponse(null);
            }
        }) {
            group.startAll();
            int numLeases = between(1, 10);
            for (int i = 0; i < numLeases; i++) {
                PlainActionFuture<Void> future = new PlainActionFuture<>();
                group.addRetentionLease(Integer.toString(i), randomNonNegativeLong(), "test-" + i, future);
                future.get();
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
        IndexMetaData indexMetaData = buildIndexMetaData(numberOfReplicas, settings, indexMapping);
        final AtomicReference<Consumer<RetentionLeaseSyncAction.Request>> onRequest = new AtomicReference<>();
        try (ReplicationGroup group = new ReplicationGroup(indexMetaData) {
            @Override
            protected void syncRetentionLeases(ShardId shardId, RetentionLeases leases, ActionListener<Void> listener) {
                onRequest.get().accept(new RetentionLeaseSyncAction.Request(shardId, leases));
                listener.onResponse(null);
            }
        }) {
            group.startAll();
            int numLeases = between(1, 100);
            final IndexShard newPrimary = randomFrom(group.getReplicas());
            final AtomicReference<RetentionLeases> latestRetentionLeasesOnNewPrimary = new AtomicReference<>(RetentionLeases.EMPTY);

            onRequest.set(request -> {
                final RetentionLeases newRetentionLeases = request.getRetentionLeases();
                for (IndexShard replica : randomSubsetOf(group.getReplicas())) {
                    group.executeRetentionLeasesSyncRequestOnReplica(request, replica);
                    if (newPrimary == replica) {
                        latestRetentionLeasesOnNewPrimary.updateAndGet(currentRetentionLeases
                            -> newRetentionLeases.supersedes(currentRetentionLeases) ? newRetentionLeases : currentRetentionLeases);
                    }
                }
            });

            for (int i = 0; i < numLeases; i++) {
                PlainActionFuture<Void> addLeaseFuture = new PlainActionFuture<>();
                group.addRetentionLease(Integer.toString(i), randomNonNegativeLong(), "test-" + i, addLeaseFuture);
                addLeaseFuture.get();
            }
            group.promoteReplicaToPrimary(newPrimary).get();

            // we need to make changes to retention leases to sync it to replicas
            // since we don't sync retention leases when promoting a new primary.

            onRequest.set(request -> {
                for (IndexShard replica : group.getReplicas()) {
                    group.executeRetentionLeasesSyncRequestOnReplica(request, replica);
                }
            });

            final PlainActionFuture<Void> newLeaseFuture = new PlainActionFuture<>();
            group.addRetentionLease("new-lease-after-promotion", randomNonNegativeLong(), "test", newLeaseFuture);
            final RetentionLeases leasesOnPrimary = group.getPrimary().getRetentionLeases();
            assertThat(leasesOnPrimary.primaryTerm(), equalTo(group.getPrimary().getOperationPrimaryTerm()));
            assertThat(leasesOnPrimary.version(), equalTo(latestRetentionLeasesOnNewPrimary.get().version() + 1L));
            assertThat(leasesOnPrimary.leases(), hasSize(latestRetentionLeasesOnNewPrimary.get().leases().size() + 1));
            newLeaseFuture.get();
            for (IndexShard replica : group.getReplicas()) {
                assertThat(replica.getRetentionLeases(), equalTo(leasesOnPrimary));
            }
        }
    }
}
