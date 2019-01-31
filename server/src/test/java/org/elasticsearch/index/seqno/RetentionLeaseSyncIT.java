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
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST)
public class RetentionLeaseSyncIT extends ESIntegTestCase  {

    public void testRetentionLeasesSyncedOnAdd() throws Exception {
        final int numberOfReplicas = 2 - scaledRandomIntBetween(0, 2);
        internalCluster().ensureAtLeastNumDataNodes(1 + numberOfReplicas);
        final Settings settings = Settings.builder()
                        .put("index.number_of_shards", 1)
                        .put("index.number_of_replicas", numberOfReplicas)
                        .build();
        createIndex("index", settings);
        ensureGreen("index");
        final String primaryShardNodeId = clusterService().state().routingTable().index("index").shard(0).primaryShard().currentNodeId();
        final String primaryShardNodeName = clusterService().state().nodes().get(primaryShardNodeId).getName();
        final IndexShard primary = internalCluster()
                .getInstance(IndicesService.class, primaryShardNodeName)
                .getShardOrNull(new ShardId(resolveIndex("index"), 0));
        // we will add multiple retention leases and expect to see them synced to all replicas
        final int length = randomIntBetween(1, 8);
        final Map<String, RetentionLease> currentRetentionLeases = new HashMap<>();
        for (int i = 0; i < length; i++) {
            final String id = randomValueOtherThanMany(currentRetentionLeases.keySet()::contains, () -> randomAlphaOfLength(8));
            final long retainingSequenceNumber = randomLongBetween(0, Long.MAX_VALUE);
            final String source = randomAlphaOfLength(8);
            final CountDownLatch latch = new CountDownLatch(1);
            final ActionListener<ReplicationResponse> listener = ActionListener.wrap(r -> latch.countDown(), e -> fail(e.toString()));
            currentRetentionLeases.put(id, primary.addRetentionLease(id, retainingSequenceNumber, source, listener));
            latch.await();

            // check retention leases have been committed on the primary
            final Collection<RetentionLease> primaryCommittedRetentionLeases = RetentionLease.decodeRetentionLeases(
                    primary.acquireLastIndexCommit(false).getIndexCommit().getUserData().get(Engine.RETENTION_LEASES));
            assertThat(currentRetentionLeases, equalTo(RetentionLease.toMap(primaryCommittedRetentionLeases)));

            // check current retention leases have been synced to all replicas
            for (final ShardRouting replicaShard : clusterService().state().routingTable().index("index").shard(0).replicaShards()) {
                final String replicaShardNodeId = replicaShard.currentNodeId();
                final String replicaShardNodeName = clusterService().state().nodes().get(replicaShardNodeId).getName();
                final IndexShard replica = internalCluster()
                        .getInstance(IndicesService.class, replicaShardNodeName)
                        .getShardOrNull(new ShardId(resolveIndex("index"), 0));
                final Map<String, RetentionLease> retentionLeasesOnReplica = RetentionLease.toMap(replica.getRetentionLeases());
                assertThat(retentionLeasesOnReplica, equalTo(currentRetentionLeases));

                // check retention leases have been committed on the replica
                final Collection<RetentionLease> replicaCommittedRetentionLeases = RetentionLease.decodeRetentionLeases(
                        replica.acquireLastIndexCommit(false).getIndexCommit().getUserData().get(Engine.RETENTION_LEASES));
                assertThat(currentRetentionLeases, equalTo(RetentionLease.toMap(replicaCommittedRetentionLeases)));
            }
        }
    }

    public void testRetentionLeasesSyncOnExpiration() throws Exception {
        final int numberOfReplicas = 2 - scaledRandomIntBetween(0, 2);
        internalCluster().ensureAtLeastNumDataNodes(1 + numberOfReplicas);
        final long estimatedTimeIntervalMillis = ThreadPool.ESTIMATED_TIME_INTERVAL_SETTING.get(Settings.EMPTY).millis();
        final TimeValue retentionLeaseTimeToLive =
                TimeValue.timeValueMillis(randomLongBetween(estimatedTimeIntervalMillis, 2 * estimatedTimeIntervalMillis));
        final Settings settings = Settings.builder()
                .put("index.number_of_shards", 1)
                .put("index.number_of_replicas", numberOfReplicas)
                .put(IndexSettings.INDEX_SOFT_DELETES_RETENTION_LEASE_SETTING.getKey(), retentionLeaseTimeToLive)
                .build();
        createIndex("index", settings);
        ensureGreen("index");
        final String primaryShardNodeId = clusterService().state().routingTable().index("index").shard(0).primaryShard().currentNodeId();
        final String primaryShardNodeName = clusterService().state().nodes().get(primaryShardNodeId).getName();
        final IndexShard primary = internalCluster()
                .getInstance(IndicesService.class, primaryShardNodeName)
                .getShardOrNull(new ShardId(resolveIndex("index"), 0));
        // we will add multiple retention leases, wait for some to expire, and assert a consistent view between the primary and the replicas
        final int length = randomIntBetween(1, 8);
        for (int i = 0; i < length; i++) {
            final String id = randomAlphaOfLength(8);
            final long retainingSequenceNumber = randomLongBetween(0, Long.MAX_VALUE);
            final String source = randomAlphaOfLength(8);
            final CountDownLatch latch = new CountDownLatch(1);
            final ActionListener<ReplicationResponse> listener = ActionListener.wrap(r -> latch.countDown(), e -> fail(e.toString()));
            final RetentionLease currentRetentionLease = primary.addRetentionLease(id, retainingSequenceNumber, source, listener);
            final long now = System.nanoTime();
            latch.await();

            // check current retention leases have been synced to all replicas
            for (final ShardRouting replicaShard : clusterService().state().routingTable().index("index").shard(0).replicaShards()) {
                final String replicaShardNodeId = replicaShard.currentNodeId();
                final String replicaShardNodeName = clusterService().state().nodes().get(replicaShardNodeId).getName();
                final IndexShard replica = internalCluster()
                        .getInstance(IndicesService.class, replicaShardNodeName)
                        .getShardOrNull(new ShardId(resolveIndex("index"), 0));
                assertThat(replica.getRetentionLeases(), hasItem(currentRetentionLease));
            }

            // sleep long enough that *possibly* the current retention lease has expired, and certainly that any previous have
            final long later = System.nanoTime();
            Thread.sleep(Math.max(0, retentionLeaseTimeToLive.millis() - TimeUnit.NANOSECONDS.toMillis(later - now)));
            final Collection<RetentionLease> currentRetentionLeases = primary.getRetentionLeases();
            assertThat(currentRetentionLeases, anyOf(empty(), contains(currentRetentionLease)));

            /*
             * Check that expiration of retention leases has been synced to all replicas. We have to assert busy since syncing happens in
             * the background.
             */
            assertBusy(() -> {
                for (final ShardRouting replicaShard : clusterService().state().routingTable().index("index").shard(0).replicaShards()) {
                    final String replicaShardNodeId = replicaShard.currentNodeId();
                    final String replicaShardNodeName = clusterService().state().nodes().get(replicaShardNodeId).getName();
                    final IndexShard replica = internalCluster()
                            .getInstance(IndicesService.class, replicaShardNodeName)
                            .getShardOrNull(new ShardId(resolveIndex("index"), 0));
                    if (currentRetentionLeases.isEmpty()) {
                        assertThat(replica.getRetentionLeases(), empty());
                    } else {
                        assertThat(replica.getRetentionLeases(), containsInAnyOrder(currentRetentionLeases.toArray(new RetentionLease[0])));
                    }
                }
            });
        }
    }

}
