/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.seqno;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionTestUtils;
import org.elasticsearch.action.support.replication.ReplicationResponse;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.recovery.PeerRecoveryTargetService;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.AbstractSimpleTransportTestCase;
import org.elasticsearch.transport.TransportService;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static org.elasticsearch.indices.recovery.RecoverySettings.INDICES_RECOVERY_RETRY_DELAY_NETWORK_SETTING;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.emptyIterable;
import static org.hamcrest.Matchers.equalTo;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST)
public class RetentionLeaseIT extends ESIntegTestCase {

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
            Stream.of(RetentionLeaseSyncIntervalSettingPlugin.class, MockTransportService.TestPlugin.class)
        ).toList();
    }

    public void testRetentionLeasesSyncedOnAdd() throws Exception {
        final int numberOfReplicas = 2 - scaledRandomIntBetween(0, 2);
        internalCluster().ensureAtLeastNumDataNodes(1 + numberOfReplicas);
        final Settings settings = indexSettings(1, numberOfReplicas).put(IndexSettings.INDEX_SOFT_DELETES_SETTING.getKey(), true).build();
        createIndex("index", settings);
        ensureGreen("index");
        final String primaryShardNodeId = clusterService().state().routingTable().index("index").shard(0).primaryShard().currentNodeId();
        final String primaryShardNodeName = clusterService().state().nodes().get(primaryShardNodeId).getName();
        final IndexShard primary = internalCluster().getInstance(IndicesService.class, primaryShardNodeName)
            .getShardOrNull(new ShardId(resolveIndex("index"), 0));
        // we will add multiple retention leases and expect to see them synced to all replicas
        final int length = randomIntBetween(1, 8);
        final Map<String, RetentionLease> currentRetentionLeases = new LinkedHashMap<>();
        for (int i = 0; i < length; i++) {
            final String id = randomValueOtherThanMany(currentRetentionLeases.keySet()::contains, () -> randomAlphaOfLength(8));
            final long retainingSequenceNumber = randomLongBetween(0, Long.MAX_VALUE);
            final String source = randomAlphaOfLength(8);
            final CountDownLatch latch = new CountDownLatch(1);
            final ActionListener<ReplicationResponse> listener = countDownLatchListener(latch);
            // simulate a peer recovery which locks the soft deletes policy on the primary
            final Closeable retentionLock = randomBoolean() ? primary.acquireHistoryRetentionLock() : () -> {};
            currentRetentionLeases.put(id, primary.addRetentionLease(id, retainingSequenceNumber, source, listener));
            latch.await();
            retentionLock.close();

            // check retention leases have been written on the primary
            assertThat(
                currentRetentionLeases,
                equalTo(RetentionLeaseUtils.toMapExcludingPeerRecoveryRetentionLeases(primary.loadRetentionLeases()))
            );

            // check current retention leases have been synced to all replicas
            for (final ShardRouting replicaShard : clusterService().state().routingTable().index("index").shard(0).replicaShards()) {
                final String replicaShardNodeId = replicaShard.currentNodeId();
                final String replicaShardNodeName = clusterService().state().nodes().get(replicaShardNodeId).getName();
                final IndexShard replica = internalCluster().getInstance(IndicesService.class, replicaShardNodeName)
                    .getShardOrNull(new ShardId(resolveIndex("index"), 0));
                final Map<String, RetentionLease> retentionLeasesOnReplica = RetentionLeaseUtils.toMapExcludingPeerRecoveryRetentionLeases(
                    replica.getRetentionLeases()
                );
                assertThat(retentionLeasesOnReplica, equalTo(currentRetentionLeases));

                // check retention leases have been written on the replica
                assertThat(
                    currentRetentionLeases,
                    equalTo(RetentionLeaseUtils.toMapExcludingPeerRecoveryRetentionLeases(replica.loadRetentionLeases()))
                );
            }
        }
    }

    public void testRetentionLeaseSyncedOnRemove() throws Exception {
        final int numberOfReplicas = 2 - scaledRandomIntBetween(0, 2);
        internalCluster().ensureAtLeastNumDataNodes(1 + numberOfReplicas);
        createIndex("index", 1, numberOfReplicas);
        ensureGreen("index");
        final String primaryShardNodeId = clusterService().state().routingTable().index("index").shard(0).primaryShard().currentNodeId();
        final String primaryShardNodeName = clusterService().state().nodes().get(primaryShardNodeId).getName();
        final IndexShard primary = internalCluster().getInstance(IndicesService.class, primaryShardNodeName)
            .getShardOrNull(new ShardId(resolveIndex("index"), 0));
        final int length = randomIntBetween(1, 8);
        final Map<String, RetentionLease> currentRetentionLeases = new LinkedHashMap<>();
        for (int i = 0; i < length; i++) {
            final String id = randomValueOtherThanMany(currentRetentionLeases.keySet()::contains, () -> randomAlphaOfLength(8));
            final long retainingSequenceNumber = randomLongBetween(0, Long.MAX_VALUE);
            final String source = randomAlphaOfLength(8);
            final CountDownLatch latch = new CountDownLatch(1);
            final ActionListener<ReplicationResponse> listener = countDownLatchListener(latch);
            // simulate a peer recovery which locks the soft deletes policy on the primary
            final Closeable retentionLock = randomBoolean() ? primary.acquireHistoryRetentionLock() : () -> {};
            currentRetentionLeases.put(id, primary.addRetentionLease(id, retainingSequenceNumber, source, listener));
            latch.await();
            retentionLock.close();
        }

        for (int i = 0; i < length; i++) {
            final String id = randomFrom(currentRetentionLeases.keySet());
            final CountDownLatch latch = new CountDownLatch(1);
            primary.removeRetentionLease(id, countDownLatchListener(latch));
            // simulate a peer recovery which locks the soft deletes policy on the primary
            final Closeable retentionLock = randomBoolean() ? primary.acquireHistoryRetentionLock() : () -> {};
            currentRetentionLeases.remove(id);
            latch.await();
            retentionLock.close();

            // check retention leases have been written on the primary
            assertThat(
                currentRetentionLeases,
                equalTo(RetentionLeaseUtils.toMapExcludingPeerRecoveryRetentionLeases(primary.loadRetentionLeases()))
            );

            // check current retention leases have been synced to all replicas
            for (final ShardRouting replicaShard : clusterService().state().routingTable().index("index").shard(0).replicaShards()) {
                final String replicaShardNodeId = replicaShard.currentNodeId();
                final String replicaShardNodeName = clusterService().state().nodes().get(replicaShardNodeId).getName();
                final IndexShard replica = internalCluster().getInstance(IndicesService.class, replicaShardNodeName)
                    .getShardOrNull(new ShardId(resolveIndex("index"), 0));
                final Map<String, RetentionLease> retentionLeasesOnReplica = RetentionLeaseUtils.toMapExcludingPeerRecoveryRetentionLeases(
                    replica.getRetentionLeases()
                );
                assertThat(retentionLeasesOnReplica, equalTo(currentRetentionLeases));

                // check retention leases have been written on the replica
                assertThat(
                    currentRetentionLeases,
                    equalTo(RetentionLeaseUtils.toMapExcludingPeerRecoveryRetentionLeases(replica.loadRetentionLeases()))
                );
            }
        }
    }

    public void testRetentionLeasesSyncOnExpiration() throws Exception {
        final int numberOfReplicas = 2 - scaledRandomIntBetween(0, 2);
        internalCluster().ensureAtLeastNumDataNodes(1 + numberOfReplicas);
        final long estimatedTimeIntervalMillis = ThreadPool.ESTIMATED_TIME_INTERVAL_SETTING.get(Settings.EMPTY).millis();
        final TimeValue retentionLeaseTimeToLive = TimeValue.timeValueMillis(
            randomLongBetween(estimatedTimeIntervalMillis, 2 * estimatedTimeIntervalMillis)
        );
        final Settings settings = indexSettings(1, numberOfReplicas).put(
            IndexService.RETENTION_LEASE_SYNC_INTERVAL_SETTING.getKey(),
            TimeValue.timeValueSeconds(1)
        ).build();
        createIndex("index", settings);
        ensureGreen("index");
        final String primaryShardNodeId = clusterService().state().routingTable().index("index").shard(0).primaryShard().currentNodeId();
        final String primaryShardNodeName = clusterService().state().nodes().get(primaryShardNodeId).getName();
        final IndexShard primary = internalCluster().getInstance(IndicesService.class, primaryShardNodeName)
            .getShardOrNull(new ShardId(resolveIndex("index"), 0));
        // we will add multiple retention leases, wait for some to expire, and assert a consistent view between the primary and the replicas
        final int length = randomIntBetween(1, 8);
        for (int i = 0; i < length; i++) {
            // update the index for retention leases to live a long time
            updateIndexSettings(
                Settings.builder().putNull(IndexSettings.INDEX_SOFT_DELETES_RETENTION_LEASE_PERIOD_SETTING.getKey()),
                "index"
            );

            final String id = randomAlphaOfLength(8);
            final long retainingSequenceNumber = randomLongBetween(0, Long.MAX_VALUE);
            final String source = randomAlphaOfLength(8);
            final CountDownLatch latch = new CountDownLatch(1);
            final ActionListener<ReplicationResponse> listener = countDownLatchListener(latch);
            final RetentionLease currentRetentionLease = primary.addRetentionLease(id, retainingSequenceNumber, source, listener);
            final long now = System.nanoTime();
            latch.await();

            // check current retention leases have been synced to all replicas
            for (final ShardRouting replicaShard : clusterService().state().routingTable().index("index").shard(0).replicaShards()) {
                final String replicaShardNodeId = replicaShard.currentNodeId();
                final String replicaShardNodeName = clusterService().state().nodes().get(replicaShardNodeId).getName();
                final IndexShard replica = internalCluster().getInstance(IndicesService.class, replicaShardNodeName)
                    .getShardOrNull(new ShardId(resolveIndex("index"), 0));
                assertThat(
                    RetentionLeaseUtils.toMapExcludingPeerRecoveryRetentionLeases(replica.getRetentionLeases()).values(),
                    anyOf(emptyIterable(), contains(currentRetentionLease))
                );
            }

            // update the index for retention leases to short a long time, to force expiration
            updateIndexSettings(
                Settings.builder().put(IndexSettings.INDEX_SOFT_DELETES_RETENTION_LEASE_PERIOD_SETTING.getKey(), retentionLeaseTimeToLive),
                "index"
            );
            // sleep long enough that the current retention lease has expired
            final long later = System.nanoTime();
            Thread.sleep(Math.max(0, retentionLeaseTimeToLive.millis() - TimeUnit.NANOSECONDS.toMillis(later - now)));
            assertBusy(
                () -> assertThat(
                    RetentionLeaseUtils.toMapExcludingPeerRecoveryRetentionLeases(primary.getRetentionLeases()).entrySet(),
                    empty()
                )
            );

            // now that all retention leases are expired should have been synced to all replicas
            assertBusy(() -> {
                for (final ShardRouting replicaShard : clusterService().state().routingTable().index("index").shard(0).replicaShards()) {
                    final String replicaShardNodeId = replicaShard.currentNodeId();
                    final String replicaShardNodeName = clusterService().state().nodes().get(replicaShardNodeId).getName();
                    final IndexShard replica = internalCluster().getInstance(IndicesService.class, replicaShardNodeName)
                        .getShardOrNull(new ShardId(resolveIndex("index"), 0));
                    assertThat(
                        RetentionLeaseUtils.toMapExcludingPeerRecoveryRetentionLeases(replica.getRetentionLeases()).entrySet(),
                        empty()
                    );
                }
            });
        }
    }

    public void testBackgroundRetentionLeaseSync() throws Exception {
        final int numberOfReplicas = 2 - scaledRandomIntBetween(0, 2);
        internalCluster().ensureAtLeastNumDataNodes(1 + numberOfReplicas);
        final Settings settings = indexSettings(1, numberOfReplicas).put(
            IndexService.RETENTION_LEASE_SYNC_INTERVAL_SETTING.getKey(),
            TimeValue.timeValueSeconds(1)
        ).build();
        createIndex("index", settings);
        ensureGreen("index");
        final String primaryShardNodeId = clusterService().state().routingTable().index("index").shard(0).primaryShard().currentNodeId();
        final String primaryShardNodeName = clusterService().state().nodes().get(primaryShardNodeId).getName();
        final IndexShard primary = internalCluster().getInstance(IndicesService.class, primaryShardNodeName)
            .getShardOrNull(new ShardId(resolveIndex("index"), 0));
        // we will add multiple retention leases and expect to see them synced to all replicas
        final int length = randomIntBetween(1, 8);
        final Map<String, RetentionLease> currentRetentionLeases = Maps.newLinkedHashMapWithExpectedSize(length);
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
                primary.addRetentionLease(id, retainingSequenceNumber, source, ActionListener.running(latch::countDown))
            );
            latch.await();
            // now renew all existing leases; we expect to see these synced to the replicas
            for (int j = 0; j <= i; j++) {
                currentRetentionLeases.put(
                    ids.get(j),
                    primary.renewRetentionLease(
                        ids.get(j),
                        randomLongBetween(currentRetentionLeases.get(ids.get(j)).retainingSequenceNumber(), Long.MAX_VALUE),
                        source
                    )
                );
            }
            assertBusy(() -> {
                // check all retention leases have been synced to all replicas
                for (final ShardRouting replicaShard : clusterService().state().routingTable().index("index").shard(0).replicaShards()) {
                    final String replicaShardNodeId = replicaShard.currentNodeId();
                    final String replicaShardNodeName = clusterService().state().nodes().get(replicaShardNodeId).getName();
                    final IndexShard replica = internalCluster().getInstance(IndicesService.class, replicaShardNodeName)
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
         * source of retention leases on the replicas would be from recovery.
         */
        final Settings.Builder settings = indexSettings(1, 0).put(IndexSettings.INDEX_SOFT_DELETES_SETTING.getKey(), true)
            .put(IndexService.RETENTION_LEASE_SYNC_INTERVAL_SETTING.getKey(), TimeValue.timeValueHours(24));
        // when we increase the number of replicas below we want to exclude the replicas from being allocated so that they do not recover
        assertAcked(prepareCreate("index", 1, settings));
        ensureYellow("index");
        setReplicaCount(numberOfReplicas, "index");
        final String primaryShardNodeId = clusterService().state().routingTable().index("index").shard(0).primaryShard().currentNodeId();
        final String primaryShardNodeName = clusterService().state().nodes().get(primaryShardNodeId).getName();
        final IndexShard primary = internalCluster().getInstance(IndicesService.class, primaryShardNodeName)
            .getShardOrNull(new ShardId(resolveIndex("index"), 0));
        final int length = randomIntBetween(1, 8);
        final Map<String, RetentionLease> currentRetentionLeases = new LinkedHashMap<>();
        logger.info("adding retention [{}] leases", length);
        for (int i = 0; i < length; i++) {
            final String id = randomValueOtherThanMany(currentRetentionLeases.keySet()::contains, () -> randomAlphaOfLength(8));
            final long retainingSequenceNumber = randomLongBetween(0, Long.MAX_VALUE);
            final String source = randomAlphaOfLength(8);
            final CountDownLatch latch = new CountDownLatch(1);
            final ActionListener<ReplicationResponse> listener = countDownLatchListener(latch);
            currentRetentionLeases.put(id, primary.addRetentionLease(id, retainingSequenceNumber, source, listener));
            latch.await();
        }
        logger.info("finished adding [{}] retention leases", length);

        // cause some recoveries to fail to ensure that retention leases are handled properly when retrying a recovery
        updateClusterSettings(
            Settings.builder().put(INDICES_RECOVERY_RETRY_DELAY_NETWORK_SETTING.getKey(), TimeValue.timeValueMillis(100))
        );
        final Semaphore recoveriesToDisrupt = new Semaphore(scaledRandomIntBetween(0, 4));
        final var primaryTransportService = MockTransportService.getInstance(primaryShardNodeName);
        primaryTransportService.addSendBehavior((connection, requestId, action, request, options) -> {
            if (action.equals(PeerRecoveryTargetService.Actions.FINALIZE) && recoveriesToDisrupt.tryAcquire()) {
                if (randomBoolean()) {
                    // return a ConnectTransportException to the START_RECOVERY action
                    final TransportService replicaTransportService = internalCluster().getInstance(
                        TransportService.class,
                        connection.getNode().getName()
                    );
                    final DiscoveryNode primaryNode = primaryTransportService.getLocalNode();
                    replicaTransportService.disconnectFromNode(primaryNode);
                    AbstractSimpleTransportTestCase.connectToNode(replicaTransportService, primaryNode);
                } else {
                    // return an exception to the FINALIZE action
                    throw new ElasticsearchException("failing recovery for test purposes");
                }
            }
            connection.sendRequest(requestId, action, request, options);
        });

        logger.info("allow [{}] replicas to allocate", numberOfReplicas);
        // now allow the replicas to be allocated and wait for recovery to finalize
        allowNodes("index", 1 + numberOfReplicas);
        ensureGreen("index");

        // check current retention leases have been synced to all replicas
        for (final ShardRouting replicaShard : clusterService().state().routingTable().index("index").shard(0).replicaShards()) {
            final String replicaShardNodeId = replicaShard.currentNodeId();
            final String replicaShardNodeName = clusterService().state().nodes().get(replicaShardNodeId).getName();
            final IndexShard replica = internalCluster().getInstance(IndicesService.class, replicaShardNodeName)
                .getShardOrNull(new ShardId(resolveIndex("index"), 0));
            final Map<String, RetentionLease> retentionLeasesOnReplica = RetentionLeaseUtils.toMapExcludingPeerRecoveryRetentionLeases(
                replica.getRetentionLeases()
            );
            assertThat(retentionLeasesOnReplica, equalTo(currentRetentionLeases));

            // check retention leases have been written on the replica; see RecoveryTarget#finalizeRecovery
            assertThat(
                currentRetentionLeases,
                equalTo(RetentionLeaseUtils.toMapExcludingPeerRecoveryRetentionLeases(replica.loadRetentionLeases()))
            );
        }
    }

    public void testCanAddRetentionLeaseUnderBlock() throws InterruptedException {
        final String idForInitialRetentionLease = randomAlphaOfLength(8);
        runUnderBlockTest(idForInitialRetentionLease, randomLongBetween(0, Long.MAX_VALUE), (primary, listener) -> {
            final String nextId = randomValueOtherThan(idForInitialRetentionLease, () -> randomAlphaOfLength(8));
            final long nextRetainingSequenceNumber = randomLongBetween(0, Long.MAX_VALUE);
            final String nextSource = randomAlphaOfLength(8);
            primary.addRetentionLease(nextId, nextRetainingSequenceNumber, nextSource, listener);
        }, primary -> {});
    }

    public void testCanRenewRetentionLeaseUnderBlock() throws InterruptedException {
        final String idForInitialRetentionLease = randomAlphaOfLength(8);
        final long initialRetainingSequenceNumber = randomLongBetween(0, Long.MAX_VALUE);
        final AtomicReference<RetentionLease> retentionLease = new AtomicReference<>();
        runUnderBlockTest(idForInitialRetentionLease, initialRetainingSequenceNumber, (primary, listener) -> {
            final long nextRetainingSequenceNumber = randomLongBetween(initialRetainingSequenceNumber, Long.MAX_VALUE);
            final String nextSource = randomAlphaOfLength(8);
            retentionLease.set(primary.renewRetentionLease(idForInitialRetentionLease, nextRetainingSequenceNumber, nextSource));
            listener.onResponse(new ReplicationResponse());
        }, primary -> {
            try {
                /*
                 * If the background renew was able to execute, then the retention leases were persisted to disk. There is no other
                 * way for the current retention leases to end up written to disk so we assume that if they are written to disk, it
                 * implies that the background sync was able to execute under a block.
                 */
                assertBusy(
                    () -> assertThat(
                        RetentionLeaseUtils.toMapExcludingPeerRecoveryRetentionLeases(primary.loadRetentionLeases()).values(),
                        contains(retentionLease.get())
                    )
                );
            } catch (final Exception e) {
                fail(e);
            }
        });

    }

    public void testCanRemoveRetentionLeasesUnderBlock() throws InterruptedException {
        final String idForInitialRetentionLease = randomAlphaOfLength(8);
        runUnderBlockTest(
            idForInitialRetentionLease,
            randomLongBetween(0, Long.MAX_VALUE),
            (primary, listener) -> primary.removeRetentionLease(idForInitialRetentionLease, listener),
            indexShard -> {}
        );
    }

    private void runUnderBlockTest(
        final String idForInitialRetentionLease,
        final long initialRetainingSequenceNumber,
        final BiConsumer<IndexShard, ActionListener<ReplicationResponse>> primaryConsumer,
        final Consumer<IndexShard> afterSync
    ) throws InterruptedException {
        final Settings settings = indexSettings(1, 0).put(IndexSettings.INDEX_SOFT_DELETES_SETTING.getKey(), true)
            .put(IndexService.RETENTION_LEASE_SYNC_INTERVAL_SETTING.getKey(), TimeValue.timeValueSeconds(1))
            .build();
        assertAcked(prepareCreate("index").setSettings(settings));
        ensureGreen("index");

        final String primaryShardNodeId = clusterService().state().routingTable().index("index").shard(0).primaryShard().currentNodeId();
        final String primaryShardNodeName = clusterService().state().nodes().get(primaryShardNodeId).getName();
        final IndexShard primary = internalCluster().getInstance(IndicesService.class, primaryShardNodeName)
            .getShardOrNull(new ShardId(resolveIndex("index"), 0));

        final String source = randomAlphaOfLength(8);
        final CountDownLatch latch = new CountDownLatch(1);
        final ActionListener<ReplicationResponse> listener = countDownLatchListener(latch);
        primary.addRetentionLease(idForInitialRetentionLease, initialRetainingSequenceNumber, source, listener);
        latch.await();

        final String block = randomFrom("read_only", "read_only_allow_delete", "read", "write", "metadata");
        updateIndexSettings(Settings.builder().put("index.blocks." + block, true), "index");

        try {
            final CountDownLatch actionLatch = new CountDownLatch(1);
            final AtomicBoolean success = new AtomicBoolean();

            primaryConsumer.accept(primary, new ActionListener<ReplicationResponse>() {

                @Override
                public void onResponse(final ReplicationResponse replicationResponse) {
                    success.set(true);
                    actionLatch.countDown();
                }

                @Override
                public void onFailure(final Exception e) {
                    fail(e);
                }

            });
            actionLatch.await();
            assertTrue(success.get());
            afterSync.accept(primary);
        } finally {
            updateIndexSettings(Settings.builder().putNull("index.blocks." + block), "index");
        }
    }

    public void testCanAddRetentionLeaseWithoutWaitingForShards() throws InterruptedException {
        final String idForInitialRetentionLease = randomAlphaOfLength(8);
        runWaitForShardsTest(idForInitialRetentionLease, randomLongBetween(0, Long.MAX_VALUE), (primary, listener) -> {
            final String nextId = randomValueOtherThan(idForInitialRetentionLease, () -> randomAlphaOfLength(8));
            final long nextRetainingSequenceNumber = randomLongBetween(0, Long.MAX_VALUE);
            final String nextSource = randomAlphaOfLength(8);
            primary.addRetentionLease(nextId, nextRetainingSequenceNumber, nextSource, listener);
        }, primary -> {});
    }

    public void testCanRenewRetentionLeaseWithoutWaitingForShards() throws InterruptedException {
        final String idForInitialRetentionLease = randomAlphaOfLength(8);
        final long initialRetainingSequenceNumber = randomLongBetween(0, Long.MAX_VALUE);
        final AtomicReference<RetentionLease> retentionLease = new AtomicReference<>();
        runWaitForShardsTest(idForInitialRetentionLease, initialRetainingSequenceNumber, (primary, listener) -> {
            final long nextRetainingSequenceNumber = randomLongBetween(initialRetainingSequenceNumber, Long.MAX_VALUE);
            final String nextSource = randomAlphaOfLength(8);
            retentionLease.set(primary.renewRetentionLease(idForInitialRetentionLease, nextRetainingSequenceNumber, nextSource));
            listener.onResponse(new ReplicationResponse());
        }, primary -> {
            try {
                /*
                 * If the background renew was able to execute, then the retention leases were persisted to disk. There is no other
                 * way for the current retention leases to end up written to disk so we assume that if they are written to disk, it
                 * implies that the background sync was able to execute despite wait for shards being set on the index.
                 */
                assertBusy(
                    () -> assertThat(
                        RetentionLeaseUtils.toMapExcludingPeerRecoveryRetentionLeases(primary.loadRetentionLeases()).values(),
                        contains(retentionLease.get())
                    )
                );
            } catch (final Exception e) {
                fail(e);
            }
        });

    }

    public void testCanRemoveRetentionLeasesWithoutWaitingForShards() throws InterruptedException {
        final String idForInitialRetentionLease = randomAlphaOfLength(8);
        runWaitForShardsTest(
            idForInitialRetentionLease,
            randomLongBetween(0, Long.MAX_VALUE),
            (primary, listener) -> primary.removeRetentionLease(idForInitialRetentionLease, listener),
            primary -> {}
        );
    }

    private void runWaitForShardsTest(
        final String idForInitialRetentionLease,
        final long initialRetainingSequenceNumber,
        final BiConsumer<IndexShard, ActionListener<ReplicationResponse>> primaryConsumer,
        final Consumer<IndexShard> afterSync
    ) throws InterruptedException {
        final int numDataNodes = internalCluster().numDataNodes();
        final Settings settings = indexSettings(1, numDataNodes).put(IndexSettings.INDEX_SOFT_DELETES_SETTING.getKey(), true)
            .put(IndexService.RETENTION_LEASE_SYNC_INTERVAL_SETTING.getKey(), TimeValue.timeValueSeconds(1))
            .build();
        assertAcked(prepareCreate("index").setSettings(settings));
        ensureYellowAndNoInitializingShards("index");
        assertFalse(clusterAdmin().prepareHealth(TEST_REQUEST_TIMEOUT, "index").setWaitForActiveShards(numDataNodes).get().isTimedOut());

        final String primaryShardNodeId = clusterService().state().routingTable().index("index").shard(0).primaryShard().currentNodeId();
        final String primaryShardNodeName = clusterService().state().nodes().get(primaryShardNodeId).getName();
        final IndexShard primary = internalCluster().getInstance(IndicesService.class, primaryShardNodeName)
            .getShardOrNull(new ShardId(resolveIndex("index"), 0));

        final String source = randomAlphaOfLength(8);
        final CountDownLatch latch = new CountDownLatch(1);
        final ActionListener<ReplicationResponse> listener = countDownLatchListener(latch);
        primary.addRetentionLease(idForInitialRetentionLease, initialRetainingSequenceNumber, source, listener);
        latch.await();

        final String waitForActiveValue = randomBoolean() ? "all" : Integer.toString(numDataNodes + 1);
        updateIndexSettings(Settings.builder().put("index.write.wait_for_active_shards", waitForActiveValue), "index");

        final CountDownLatch actionLatch = new CountDownLatch(1);
        final AtomicBoolean success = new AtomicBoolean();

        primaryConsumer.accept(primary, ActionTestUtils.assertNoFailureListener(ignored -> {
            success.set(true);
            actionLatch.countDown();
        }));
        actionLatch.await();
        assertTrue(success.get());
        afterSync.accept(primary);
    }

    private static ActionListener<ReplicationResponse> countDownLatchListener(CountDownLatch latch) {
        return ActionTestUtils.assertNoFailureListener(r -> latch.countDown());
    }

}
