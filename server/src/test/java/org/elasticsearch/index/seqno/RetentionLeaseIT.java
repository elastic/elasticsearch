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
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.replication.ReplicationResponse;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST)
public class RetentionLeaseIT extends ESIntegTestCase  {

    public static final class RetentionLeaseSyncIntervalSettingPlugin extends Plugin {

        @Override
        public List<Setting<?>> getSettings() {
            return Collections.singletonList(IndexService.RETENTION_LEASE_SYNC_INTERVAL_SETTING);
        }

    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Stream.concat(
                super.nodePlugins().stream(),
                Stream.of(RetentionLeaseSyncIntervalSettingPlugin.class))
                .collect(Collectors.toList());
    }

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
            // simulate a peer recovery which locks the soft deletes policy on the primary
            final Closeable retentionLock = randomBoolean() ? primary.acquireRetentionLockForPeerRecovery() : () -> {};
            currentRetentionLeases.put(id, primary.addRetentionLease(id, retainingSequenceNumber, source, listener));
            latch.await();
            retentionLock.close();

            // check retention leases have been committed on the primary
            final RetentionLeases primaryCommittedRetentionLeases = RetentionLeases.decodeRetentionLeases(
                    primary.acquireLastIndexCommit(false).getIndexCommit().getUserData().get(Engine.RETENTION_LEASES));
            assertThat(currentRetentionLeases, equalTo(RetentionLeases.toMap(primaryCommittedRetentionLeases)));

            // check current retention leases have been synced to all replicas
            for (final ShardRouting replicaShard : clusterService().state().routingTable().index("index").shard(0).replicaShards()) {
                final String replicaShardNodeId = replicaShard.currentNodeId();
                final String replicaShardNodeName = clusterService().state().nodes().get(replicaShardNodeId).getName();
                final IndexShard replica = internalCluster()
                        .getInstance(IndicesService.class, replicaShardNodeName)
                        .getShardOrNull(new ShardId(resolveIndex("index"), 0));
                final Map<String, RetentionLease> retentionLeasesOnReplica = RetentionLeases.toMap(replica.getRetentionLeases());
                assertThat(retentionLeasesOnReplica, equalTo(currentRetentionLeases));

                // check retention leases have been committed on the replica
                final RetentionLeases replicaCommittedRetentionLeases = RetentionLeases.decodeRetentionLeases(
                        replica.acquireLastIndexCommit(false).getIndexCommit().getUserData().get(Engine.RETENTION_LEASES));
                assertThat(currentRetentionLeases, equalTo(RetentionLeases.toMap(replicaCommittedRetentionLeases)));
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
                .put(IndexService.RETENTION_LEASE_SYNC_INTERVAL_SETTING.getKey(), TimeValue.timeValueSeconds(1))
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
            // update the index for retention leases to live a long time
            final AcknowledgedResponse longTtlResponse = client().admin()
                    .indices()
                    .prepareUpdateSettings("index")
                    .setSettings(
                            Settings.builder()
                                    .putNull(IndexSettings.INDEX_SOFT_DELETES_RETENTION_LEASE_SETTING.getKey())
                                    .build())
                    .get();
            assertTrue(longTtlResponse.isAcknowledged());

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
                assertThat(replica.getRetentionLeases().leases(), anyOf(empty(), contains(currentRetentionLease)));
            }

            // update the index for retention leases to short a long time, to force expiration
            final AcknowledgedResponse shortTtlResponse = client().admin()
                    .indices()
                    .prepareUpdateSettings("index")
                    .setSettings(
                            Settings.builder()
                                    .put(IndexSettings.INDEX_SOFT_DELETES_RETENTION_LEASE_SETTING.getKey(), retentionLeaseTimeToLive)
                                    .build())
                    .get();
            assertTrue(shortTtlResponse.isAcknowledged());

            // sleep long enough that the current retention lease has expired
            final long later = System.nanoTime();
            Thread.sleep(Math.max(0, retentionLeaseTimeToLive.millis() - TimeUnit.NANOSECONDS.toMillis(later - now)));
            assertBusy(() -> assertThat(primary.getRetentionLeases().leases(), empty()));

            // now that all retention leases are expired should have been synced to all replicas
            assertBusy(() -> {
                for (final ShardRouting replicaShard : clusterService().state().routingTable().index("index").shard(0).replicaShards()) {
                    final String replicaShardNodeId = replicaShard.currentNodeId();
                    final String replicaShardNodeName = clusterService().state().nodes().get(replicaShardNodeId).getName();
                    final IndexShard replica = internalCluster()
                            .getInstance(IndicesService.class, replicaShardNodeName)
                            .getShardOrNull(new ShardId(resolveIndex("index"), 0));
                    assertThat(replica.getRetentionLeases().leases(), empty());
                }
            });
        }
    }

    public void testBackgroundRetentionLeaseSync() throws Exception {
        final int numberOfReplicas = 2 - scaledRandomIntBetween(0, 2);
        internalCluster().ensureAtLeastNumDataNodes(1 + numberOfReplicas);
        final Settings settings = Settings.builder()
                .put("index.number_of_shards", 1)
                .put("index.number_of_replicas", numberOfReplicas)
                .put(IndexService.RETENTION_LEASE_SYNC_INTERVAL_SETTING.getKey(), "1s")
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
        final Map<String, RetentionLease> currentRetentionLeases = new HashMap<>(length);
        final List<String> ids = new ArrayList<>(length);
        for (int i = 0; i < length; i++) {
            final String id = randomValueOtherThanMany(currentRetentionLeases.keySet()::contains, () -> randomAlphaOfLength(8));
            ids.add(id);
            final long retainingSequenceNumber = randomLongBetween(0, Long.MAX_VALUE);
            final String source = randomAlphaOfLength(8);
            final CountDownLatch latch = new CountDownLatch(1);
            // put a new lease
            currentRetentionLeases.put(
                    id,
                    primary.addRetentionLease(id, retainingSequenceNumber, source, ActionListener.wrap(latch::countDown)));
            latch.await();
            // now renew all existing leases; we expect to see these synced to the replicas
            for (int j = 0; j <= i; j++) {
                currentRetentionLeases.put(
                        ids.get(j),
                        primary.renewRetentionLease(
                                ids.get(j),
                                randomLongBetween(currentRetentionLeases.get(ids.get(j)).retainingSequenceNumber(), Long.MAX_VALUE),
                                source));
            }
            assertBusy(() -> {
                // check all retention leases have been synced to all replicas
                for (final ShardRouting replicaShard : clusterService().state().routingTable().index("index").shard(0).replicaShards()) {
                    final String replicaShardNodeId = replicaShard.currentNodeId();
                    final String replicaShardNodeName = clusterService().state().nodes().get(replicaShardNodeId).getName();
                    final IndexShard replica = internalCluster()
                            .getInstance(IndicesService.class, replicaShardNodeName)
                            .getShardOrNull(new ShardId(resolveIndex("index"), 0));
                    assertThat(replica.getRetentionLeases(), equalTo(primary.getRetentionLeases()));
                }
            });
        }
    }

    public void testRetentionLeasesSyncOnRecovery() throws Exception {
        final int numberOfReplicas = 2 - scaledRandomIntBetween(0, 2);
        internalCluster().ensureAtLeastNumDataNodes(1 + numberOfReplicas);
        /*
         * We effectively disable the background sync to ensure that the retention leases are not synced in the background so that the only
         * source of retention leases on the replicas would be from the commit point and recovery.
         */
        final Settings settings = Settings.builder()
                .put("index.number_of_shards", 1)
                .put("index.number_of_replicas", 0)
                .put(IndexService.RETENTION_LEASE_SYNC_INTERVAL_SETTING.getKey(), TimeValue.timeValueHours(24))
                .build();
        // when we increase the number of replicas below we want to exclude the replicas from being allocated so that they do not recover
        assertAcked(prepareCreate("index", 1).setSettings(settings));
        ensureYellow("index");
        final AcknowledgedResponse response = client().admin()
                .indices()
                .prepareUpdateSettings("index").setSettings(Settings.builder().put("index.number_of_replicas", numberOfReplicas).build())
                .get();
        assertTrue(response.isAcknowledged());
        final String primaryShardNodeId = clusterService().state().routingTable().index("index").shard(0).primaryShard().currentNodeId();
        final String primaryShardNodeName = clusterService().state().nodes().get(primaryShardNodeId).getName();
        final IndexShard primary = internalCluster()
                .getInstance(IndicesService.class, primaryShardNodeName)
                .getShardOrNull(new ShardId(resolveIndex("index"), 0));
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
            /*
             * Now renew the leases; since we do not flush immediately on renewal, this means that the latest retention leases will not be
             * in the latest commit point and therefore not transferred during the file-copy phase of recovery.
             */
            currentRetentionLeases.put(id, primary.renewRetentionLease(id, retainingSequenceNumber, source));
        }

        // now allow the replicas to be allocated and wait for recovery to finalize
        allowNodes("index", 1 + numberOfReplicas);
        ensureGreen("index");

        // check current retention leases have been synced to all replicas
        for (final ShardRouting replicaShard : clusterService().state().routingTable().index("index").shard(0).replicaShards()) {
            final String replicaShardNodeId = replicaShard.currentNodeId();
            final String replicaShardNodeName = clusterService().state().nodes().get(replicaShardNodeId).getName();
            final IndexShard replica = internalCluster()
                    .getInstance(IndicesService.class, replicaShardNodeName)
                    .getShardOrNull(new ShardId(resolveIndex("index"), 0));
            final Map<String, RetentionLease> retentionLeasesOnReplica = RetentionLeases.toMap(replica.getRetentionLeases());
            assertThat(retentionLeasesOnReplica, equalTo(currentRetentionLeases));
        }
    }

}
