/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ccr;

import com.carrotsearch.hppc.cursors.ObjectCursor;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotRequest;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.action.admin.indices.close.CloseIndexRequest;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsRequest;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.action.admin.indices.stats.ShardStats;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.seqno.RetentionLease;
import org.elasticsearch.index.seqno.RetentionLeaseActions;
import org.elasticsearch.index.seqno.RetentionLeases;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardClosedException;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.snapshots.RestoreInfo;
import org.elasticsearch.snapshots.RestoreService;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.transport.ConnectTransportException;
import org.elasticsearch.transport.TransportActionProxy;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.CcrIntegTestCase;
import org.elasticsearch.xpack.ccr.action.repositories.ClearCcrRestoreSessionAction;
import org.elasticsearch.xpack.ccr.repository.CcrRepository;
import org.elasticsearch.xpack.core.ccr.action.PutFollowAction;
import org.elasticsearch.xpack.core.ccr.action.UnfollowAction;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static java.util.Collections.singletonMap;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.ccr.CcrRetentionLeases.retentionLeaseId;
import static org.hamcrest.Matchers.arrayWithSize;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;

public class CcrRetentionLeaseIT extends CcrIntegTestCase {

    public static final class RetentionLeaseRenewIntervalSettingPlugin extends Plugin {

        @Override
        public List<Setting<?>> getSettings() {
            return Collections.singletonList(CcrRepository.RETENTION_LEASE_RENEW_INTERVAL_SETTING);
        }

    }

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
                Stream.of(RetentionLeaseRenewIntervalSettingPlugin.class, RetentionLeaseSyncIntervalSettingPlugin.class))
                .collect(Collectors.toList());
    }

    private final IndicesOptions indicesOptions = IndicesOptions.strictSingleIndexNoExpandForbidClosed();

    private RestoreSnapshotRequest setUpRestoreSnapshotRequest(
            final String leaderIndex,
            final int numberOfShards,
            final int numberOfReplicas,
            final String followerIndex,
            final int numberOfDocuments) throws IOException {
        final ClusterUpdateSettingsRequest settingsRequest = new ClusterUpdateSettingsRequest();
        final String chunkSize = new ByteSizeValue(randomFrom(4, 128, 1024), ByteSizeUnit.KB).getStringRep();
        settingsRequest.persistentSettings(Settings.builder().put(CcrSettings.RECOVERY_CHUNK_SIZE.getKey(), chunkSize));
        assertAcked(followerClient().admin().cluster().updateSettings(settingsRequest).actionGet());

        final String leaderClusterRepoName = CcrRepository.NAME_PREFIX + "leader_cluster";

        final Map<String, String> additionalSettings = new HashMap<>();
        additionalSettings.put(IndexSettings.INDEX_SOFT_DELETES_SETTING.getKey(), "true");
        additionalSettings.put(IndexService.RETENTION_LEASE_SYNC_INTERVAL_SETTING.getKey(), TimeValue.timeValueMillis(200).getStringRep());
        final String leaderIndexSettings = getIndexSettings(numberOfShards, numberOfReplicas, additionalSettings);
        assertAcked(leaderClient().admin().indices().prepareCreate(leaderIndex).setSource(leaderIndexSettings, XContentType.JSON));
        ensureLeaderGreen(leaderIndex);

        logger.info("indexing [{}] docs", numberOfDocuments);
        for (int i = 0; i < numberOfDocuments; i++) {
            final String source = String.format(Locale.ROOT, "{\"f\":%d}", i);
            leaderClient().prepareIndex(leaderIndex, "doc", Integer.toString(i)).setSource(source, XContentType.JSON).get();
            if (rarely()) {
                leaderClient().admin().indices().prepareFlush(leaderIndex).setForce(true).setWaitIfOngoing(true).get();
            }
        }

        leaderClient().admin().indices().prepareFlush(leaderIndex).setForce(true).setWaitIfOngoing(true).get();

        final Settings.Builder settingsBuilder = Settings.builder()
                .put(IndexMetaData.SETTING_INDEX_PROVIDED_NAME, followerIndex)
                .put(CcrRepository.RETENTION_LEASE_RENEW_INTERVAL_SETTING.getKey(), TimeValue.timeValueMillis(200))
                .put(CcrSettings.CCR_FOLLOWING_INDEX_SETTING.getKey(), true);
        return new RestoreSnapshotRequest(leaderClusterRepoName, CcrRepository.LATEST)
                .indexSettings(settingsBuilder)
                .indices(leaderIndex)
                .indicesOptions(indicesOptions)
                .renamePattern("^(.*)$")
                .renameReplacement(followerIndex)
                .masterNodeTimeout(new TimeValue(1L, TimeUnit.HOURS));
    }

    public void testRetentionLeaseIsTakenAtTheStartOfRecovery() throws Exception {
        final String leaderIndex = "leader";
        final int numberOfShards = randomIntBetween(1, 3);
        final int numberOfReplicas = between(0, 1);
        final String followerIndex = "follower";
        final int numberOfDocuments = scaledRandomIntBetween(1, 8192);
        final RestoreSnapshotRequest restoreRequest =
                setUpRestoreSnapshotRequest(leaderIndex, numberOfShards, numberOfReplicas, followerIndex, numberOfDocuments);
        final RestoreService restoreService = getFollowerCluster().getCurrentMasterNodeInstance(RestoreService.class);
        final ClusterService clusterService = getFollowerCluster().getCurrentMasterNodeInstance(ClusterService.class);

        final PlainActionFuture<RestoreInfo> future = PlainActionFuture.newFuture();
        restoreService.restoreSnapshot(restoreRequest, waitForRestore(clusterService, future));

        // ensure that a retention lease has been put in place on each shard
        assertBusy(() -> {
            final IndicesStatsResponse stats =
                    leaderClient().admin().indices().stats(new IndicesStatsRequest().clear().indices(leaderIndex)).actionGet();
            assertNotNull(stats.getShards());
            assertThat(stats.getShards(), arrayWithSize(numberOfShards * (1 + numberOfReplicas)));
            final List<ShardStats> shardsStats = getShardsStats(stats);
            for (int i = 0; i < numberOfShards * (1 + numberOfReplicas); i++) {
                final RetentionLeases currentRetentionLeases = shardsStats.get(i).getRetentionLeaseStats().retentionLeases();
                assertThat(currentRetentionLeases.leases(), hasSize(1));
                final RetentionLease retentionLease =
                        currentRetentionLeases.leases().iterator().next();
                assertThat(retentionLease.id(), equalTo(getRetentionLeaseId(followerIndex, leaderIndex)));
            }
        });

        final RestoreInfo restoreInfo = future.actionGet();

        assertEquals(restoreInfo.totalShards(), restoreInfo.successfulShards());
        assertEquals(0, restoreInfo.failedShards());
        for (int i = 0; i < numberOfDocuments; ++i) {
            assertExpectedDocument(followerIndex, i);
        }

    }

    public void testRetentionLeaseIsRenewedDuringRecovery() throws Exception {
        final String leaderIndex = "leader";
        final int numberOfShards = randomIntBetween(1, 3);
        final int numberOfReplicas = between(0, 1);
        final String followerIndex = "follower";
        final int numberOfDocuments = scaledRandomIntBetween(1, 8192);
        final RestoreSnapshotRequest restoreRequest =
                setUpRestoreSnapshotRequest(leaderIndex, numberOfShards, numberOfReplicas, followerIndex, numberOfDocuments);
        final RestoreService restoreService = getFollowerCluster().getCurrentMasterNodeInstance(RestoreService.class);
        final ClusterService clusterService = getFollowerCluster().getCurrentMasterNodeInstance(ClusterService.class);

        final CountDownLatch latch = new CountDownLatch(1);

        // block the recovery from completing; this ensures the background sync is still running
        final ClusterStateResponse followerClusterState = followerClient().admin().cluster().prepareState().clear().setNodes(true).get();
        for (final ObjectCursor<DiscoveryNode> senderNode : followerClusterState.getState().nodes().getNodes().values()) {
            final MockTransportService senderTransportService =
                    (MockTransportService) getFollowerCluster().getInstance(TransportService.class, senderNode.value.getName());
            senderTransportService.addSendBehavior(
                    (connection, requestId, action, request, options) -> {
                        if (ClearCcrRestoreSessionAction.NAME.equals(action)
                                || TransportActionProxy.getProxyAction(ClearCcrRestoreSessionAction.NAME).equals(action)) {
                            try {
                                latch.await();
                            } catch (final InterruptedException e) {
                                fail(e.toString());
                            }
                        }
                        connection.sendRequest(requestId, action, request, options);
                    });
        }

        final PlainActionFuture<RestoreInfo> future = PlainActionFuture.newFuture();
        restoreService.restoreSnapshot(restoreRequest, waitForRestore(clusterService, future));

        try {
            // ensure that a retention lease has been put in place on each shard, and grab a copy of them
            final List<RetentionLeases> retentionLeases = new ArrayList<>();
            assertBusy(() -> {
                retentionLeases.clear();
                final IndicesStatsResponse stats =
                        leaderClient().admin().indices().stats(new IndicesStatsRequest().clear().indices(leaderIndex)).actionGet();
                assertNotNull(stats.getShards());
                assertThat(stats.getShards(), arrayWithSize(numberOfShards * (1 + numberOfReplicas)));
                final List<ShardStats> shardsStats = getShardsStats(stats);
                for (int i = 0; i < numberOfShards * (1 + numberOfReplicas); i++) {
                    final RetentionLeases currentRetentionLeases = shardsStats.get(i).getRetentionLeaseStats().retentionLeases();
                    assertThat(currentRetentionLeases.leases(), hasSize(1));
                    final RetentionLease retentionLease =
                            currentRetentionLeases.leases().iterator().next();
                    assertThat(retentionLease.id(), equalTo(getRetentionLeaseId(followerIndex, leaderIndex)));
                    retentionLeases.add(currentRetentionLeases);
                }
            });

            // now ensure that the retention leases are being renewed
            assertBusy(() -> {
                final IndicesStatsResponse stats =
                        leaderClient().admin().indices().stats(new IndicesStatsRequest().clear().indices(leaderIndex)).actionGet();
                assertNotNull(stats.getShards());
                assertThat(stats.getShards(), arrayWithSize(numberOfShards * (1 + numberOfReplicas)));
                final List<ShardStats> shardsStats = getShardsStats(stats);
                for (int i = 0; i < numberOfShards * (1 + numberOfReplicas); i++) {
                    final RetentionLeases currentRetentionLeases = shardsStats.get(i).getRetentionLeaseStats().retentionLeases();
                    assertThat(currentRetentionLeases.leases(), hasSize(1));
                    final RetentionLease retentionLease =
                            currentRetentionLeases.leases().iterator().next();
                    assertThat(retentionLease.id(), equalTo(getRetentionLeaseId(followerIndex, leaderIndex)));
                    // we assert that retention leases are being renewed by an increase in the timestamp
                    assertThat(retentionLease.timestamp(), greaterThan(retentionLeases.get(i).leases().iterator().next().timestamp()));
                }
            });
            latch.countDown();
        } finally {
            for (final ObjectCursor<DiscoveryNode> senderNode : followerClusterState.getState().nodes().getDataNodes().values()) {
                final MockTransportService senderTransportService =
                        (MockTransportService) getFollowerCluster().getInstance(TransportService.class, senderNode.value.getName());
                senderTransportService.clearAllRules();
            }
        }

        final RestoreInfo restoreInfo = future.actionGet();

        assertEquals(restoreInfo.totalShards(), restoreInfo.

                successfulShards());

        assertEquals(0, restoreInfo.failedShards());
        for (int i = 0; i < numberOfDocuments; i++) {
            assertExpectedDocument(followerIndex, i);
        }

    }

    public void testRetentionLeasesAreNotBeingRenewedAfterRecoveryCompletes() throws Exception {
        final String leaderIndex = "leader";
        final int numberOfShards = randomIntBetween(1, 3);
        final int numberOfReplicas = between(0, 1);
        final String followerIndex = "follower";
        final int numberOfDocuments = scaledRandomIntBetween(1, 8192);
        final RestoreSnapshotRequest restoreRequest =
                setUpRestoreSnapshotRequest(leaderIndex, numberOfShards, numberOfReplicas, followerIndex, numberOfDocuments);
        final RestoreService restoreService = getFollowerCluster().getCurrentMasterNodeInstance(RestoreService.class);
        final ClusterService clusterService = getFollowerCluster().getCurrentMasterNodeInstance(ClusterService.class);

        final PlainActionFuture<RestoreInfo> future = PlainActionFuture.newFuture();
        restoreService.restoreSnapshot(restoreRequest, waitForRestore(clusterService, future));

        final RestoreInfo restoreInfo = future.actionGet();
        final long start = System.nanoTime();

        /*
         * We want to ensure that the retention leases have been synced to all shard copies, as otherwise they might sync between the two
         * times that we sample the retention leases, which would cause our check to fail.
         */
        final TimeValue syncIntervalSetting = IndexService.RETENTION_LEASE_SYNC_INTERVAL_SETTING.get(
                leaderClient()
                        .admin()
                        .indices()
                        .prepareGetSettings(leaderIndex)
                        .get()
                        .getIndexToSettings()
                        .get(leaderIndex));
        final long syncEnd = System.nanoTime();
        Thread.sleep(Math.max(0, randomIntBetween(2, 4) * syncIntervalSetting.millis() - TimeUnit.NANOSECONDS.toMillis(syncEnd - start)));

        final ClusterStateResponse leaderIndexClusterState =
                leaderClient().admin().cluster().prepareState().clear().setMetaData(true).setIndices(leaderIndex).get();
        final String leaderUUID = leaderIndexClusterState.getState().metaData().index(leaderIndex).getIndexUUID();

        // sample the leases after recovery
        final List<RetentionLeases> retentionLeases = new ArrayList<>();
        assertBusy(() -> {
            retentionLeases.clear();
            final IndicesStatsResponse stats =
                    leaderClient().admin().indices().stats(new IndicesStatsRequest().clear().indices(leaderIndex)).actionGet();
            assertNotNull(stats.getShards());
            assertThat(stats.getShards(), arrayWithSize(numberOfShards * (1 + numberOfReplicas)));
            final List<ShardStats> shardsStats = getShardsStats(stats);
            for (int i = 0; i < numberOfShards * (1 + numberOfReplicas); i++) {
                final RetentionLeases currentRetentionLeases = shardsStats.get(i).getRetentionLeaseStats().retentionLeases();
                assertThat(currentRetentionLeases.leases(), hasSize(1));
                final ClusterStateResponse followerIndexClusterState =
                        followerClient().admin().cluster().prepareState().clear().setMetaData(true).setIndices(followerIndex).get();
                final String followerUUID = followerIndexClusterState.getState().metaData().index(followerIndex).getIndexUUID();
                final RetentionLease retentionLease =
                        currentRetentionLeases.leases().iterator().next();
                final String expectedRetentionLeaseId = retentionLeaseId(
                        getFollowerCluster().getClusterName(),
                        new Index(followerIndex, followerUUID),
                        getLeaderCluster().getClusterName(),
                        new Index(leaderIndex, leaderUUID));
                assertThat(retentionLease.id(), equalTo(expectedRetentionLeaseId));
                retentionLeases.add(currentRetentionLeases);
            }
        });

        /*
         * We want to ensure that the background renewal is cancelled at the end of recovery. To do this, we will sleep a small multiple
         * of the renew interval. If the renews are not cancelled, we expect that a renewal would have been sent while we were sleeping.
         * After we wake up, it should be the case that the retention leases are the same (same timestamp) as that indicates that they were
         * not renewed while we were sleeping.
         */
        final TimeValue renewIntervalSetting = CcrRepository.RETENTION_LEASE_RENEW_INTERVAL_SETTING.get(
                followerClient()
                        .admin()
                        .indices()
                        .prepareGetSettings(followerIndex)
                        .get()
                        .getIndexToSettings()
                        .get(followerIndex));

        final long renewEnd = System.nanoTime();
        Thread.sleep(Math.max(0, randomIntBetween(2, 4) * renewIntervalSetting.millis() - TimeUnit.NANOSECONDS.toMillis(renewEnd - start)));

        // now ensure that the retention leases are the same
        assertBusy(() -> {
            final IndicesStatsResponse stats =
                    leaderClient().admin().indices().stats(new IndicesStatsRequest().clear().indices(leaderIndex)).actionGet();
            assertNotNull(stats.getShards());
            assertThat(stats.getShards(), arrayWithSize(numberOfShards * (1 + numberOfReplicas)));
            final List<ShardStats> shardsStats = getShardsStats(stats);
            for (int i = 0; i < numberOfShards * (1 + numberOfReplicas); i++) {
                if (shardsStats.get(i).getShardRouting().primary() == false) {
                    continue;
                }
                final RetentionLeases currentRetentionLeases = shardsStats.get(i).getRetentionLeaseStats().retentionLeases();
                assertThat(currentRetentionLeases.leases(), hasSize(1));
                final ClusterStateResponse followerIndexClusterState =
                        followerClient().admin().cluster().prepareState().clear().setMetaData(true).setIndices(followerIndex).get();
                final String followerUUID = followerIndexClusterState.getState().metaData().index(followerIndex).getIndexUUID();
                final RetentionLease retentionLease =
                        currentRetentionLeases.leases().iterator().next();
                assertThat(retentionLease.id(), equalTo(getRetentionLeaseId(followerIndex, followerUUID, leaderIndex, leaderUUID)));
                // we assert that retention leases are being renewed by an increase in the timestamp
                assertThat(retentionLease.timestamp(), equalTo(retentionLeases.get(i).leases().iterator().next().timestamp()));
            }
        });

        assertEquals(restoreInfo.totalShards(), restoreInfo.successfulShards());
        assertEquals(0, restoreInfo.failedShards());
        for (int i = 0; i < numberOfDocuments; ++i) {
            assertExpectedDocument(followerIndex, i);
        }
    }

    public void testUnfollowRemovesRetentionLeases() throws Exception {
        final String leaderIndex = "leader";
        final String followerIndex = "follower";
        final int numberOfShards = randomIntBetween(1, 4);
        final String leaderIndexSettings =
                getIndexSettings(numberOfShards, 0, singletonMap(IndexSettings.INDEX_SOFT_DELETES_SETTING.getKey(), "true"));
        assertAcked(leaderClient().admin().indices().prepareCreate(leaderIndex).setSource(leaderIndexSettings, XContentType.JSON).get());
        final PutFollowAction.Request followRequest = putFollow(leaderIndex, followerIndex);
        followerClient().execute(PutFollowAction.INSTANCE, followRequest).get();

        ensureFollowerGreen(true, followerIndex);

        final String retentionLeaseId = getRetentionLeaseId(followerIndex, leaderIndex);

        final IndicesStatsResponse stats =
                leaderClient().admin().indices().stats(new IndicesStatsRequest().clear().indices(leaderIndex)).actionGet();
        final List<ShardStats> shardsStats = getShardsStats(stats);
        for (final ShardStats shardStats : shardsStats) {
            assertThat(shardStats.getRetentionLeaseStats().retentionLeases().leases(), hasSize(1));
            assertThat(
                    shardStats.getRetentionLeaseStats().retentionLeases().leases().iterator().next().id(),
                    equalTo(retentionLeaseId));
        }

        // we will sometimes fake that some of the retention leases are already removed on the leader shard
        final Set<Integer> shardIds =
                new HashSet<>(randomSubsetOf(
                        randomIntBetween(0, numberOfShards),
                        IntStream.range(0, numberOfShards).boxed().collect(Collectors.toSet())));

        final ClusterStateResponse followerClusterState = followerClient().admin().cluster().prepareState().clear().setNodes(true).get();
        try {
            for (final ObjectCursor<DiscoveryNode> senderNode : followerClusterState.getState().nodes().getNodes().values()) {
                final MockTransportService senderTransportService =
                        (MockTransportService) getFollowerCluster().getInstance(TransportService.class, senderNode.value.getName());
                senderTransportService.addSendBehavior(
                        (connection, requestId, action, request, options) -> {
                            if (RetentionLeaseActions.Remove.ACTION_NAME.equals(action)
                                    || TransportActionProxy.getProxyAction(RetentionLeaseActions.Remove.ACTION_NAME).equals(action)) {
                                final RetentionLeaseActions.RemoveRequest removeRequest = (RetentionLeaseActions.RemoveRequest) request;
                                if (shardIds.contains(removeRequest.getShardId().id())) {
                                    final String primaryShardNodeId =
                                            getLeaderCluster()
                                                    .clusterService()
                                                    .state()
                                                    .routingTable()
                                                    .index(leaderIndex)
                                                    .shard(removeRequest.getShardId().id())
                                                    .primaryShard()
                                                    .currentNodeId();
                                    final String primaryShardNodeName =
                                            getLeaderCluster().clusterService().state().nodes().get(primaryShardNodeId).getName();
                                    final IndexShard primary =
                                            getLeaderCluster()
                                                    .getInstance(IndicesService.class, primaryShardNodeName)
                                                    .getShardOrNull(removeRequest.getShardId());
                                    final CountDownLatch latch = new CountDownLatch(1);
                                    primary.removeRetentionLease(
                                            retentionLeaseId,
                                            ActionListener.wrap(r -> latch.countDown(), e -> fail(e.toString())));
                                    try {
                                        latch.await();
                                    } catch (final InterruptedException e) {
                                        Thread.currentThread().interrupt();
                                        fail(e.toString());
                                    }
                                }
                            }
                            connection.sendRequest(requestId, action, request, options);
                        });
            }




            pauseFollow(followerIndex);
            followerClient().admin().indices().close(new CloseIndexRequest(followerIndex)).actionGet();
            assertAcked(followerClient().execute(UnfollowAction.INSTANCE, new UnfollowAction.Request(followerIndex)).actionGet());

            final IndicesStatsResponse afterUnfollowStats =
                    leaderClient().admin().indices().stats(new IndicesStatsRequest().clear().indices(leaderIndex)).actionGet();
            final List<ShardStats> afterUnfollowShardsStats = getShardsStats(afterUnfollowStats);
            for (final ShardStats shardStats : afterUnfollowShardsStats) {
                assertThat(shardStats.getRetentionLeaseStats().retentionLeases().leases(), empty());
            }
        } finally {
            for (final ObjectCursor<DiscoveryNode> senderNode : followerClusterState.getState().nodes().getDataNodes().values()) {
                final MockTransportService senderTransportService =
                        (MockTransportService) getFollowerCluster().getInstance(TransportService.class, senderNode.value.getName());
                senderTransportService.clearAllRules();
            }
        }
    }

    public void testUnfollowFailsToRemoveRetentionLeases() throws Exception {
        final String leaderIndex = "leader";
        final String followerIndex = "follower";
        final int numberOfShards = randomIntBetween(1, 4);
        final String leaderIndexSettings =
                getIndexSettings(numberOfShards, 0, singletonMap(IndexSettings.INDEX_SOFT_DELETES_SETTING.getKey(), "true"));
        assertAcked(leaderClient().admin().indices().prepareCreate(leaderIndex).setSource(leaderIndexSettings, XContentType.JSON).get());
        final PutFollowAction.Request followRequest = putFollow(leaderIndex, followerIndex);
        followerClient().execute(PutFollowAction.INSTANCE, followRequest).get();

        ensureFollowerGreen(true, followerIndex);

        pauseFollow(followerIndex);
        followerClient().admin().indices().close(new CloseIndexRequest(followerIndex)).actionGet();

        // we will disrupt requests to remove retention leases for these random shards
        final Set<Integer> shardIds =
                new HashSet<>(randomSubsetOf(
                                randomIntBetween(1, numberOfShards),
                                IntStream.range(0, numberOfShards).boxed().collect(Collectors.toSet())));

        final ClusterStateResponse followerClusterState = followerClient().admin().cluster().prepareState().clear().setNodes(true).get();
        try {
            for (final ObjectCursor<DiscoveryNode> senderNode : followerClusterState.getState().nodes().getNodes().values()) {
                final MockTransportService senderTransportService =
                        (MockTransportService) getFollowerCluster().getInstance(TransportService.class, senderNode.value.getName());
                senderTransportService.addSendBehavior(
                        (connection, requestId, action, request, options) -> {
                            if (RetentionLeaseActions.Remove.ACTION_NAME.equals(action)
                                    || TransportActionProxy.getProxyAction(RetentionLeaseActions.Remove.ACTION_NAME).equals(action)) {
                                final RetentionLeaseActions.RemoveRequest removeRequest = (RetentionLeaseActions.RemoveRequest) request;
                                if (shardIds.contains(removeRequest.getShardId().id())) {
                                    throw randomBoolean()
                                            ? new ConnectTransportException(connection.getNode(), "connection failed")
                                            : new IndexShardClosedException(removeRequest.getShardId());
                                }
                            }
                            connection.sendRequest(requestId, action, request, options);
                        });
            }

            final ElasticsearchException e = expectThrows(
                    ElasticsearchException.class,
                    () -> followerClient().execute(UnfollowAction.INSTANCE, new UnfollowAction.Request(followerIndex)).actionGet());

            final ClusterStateResponse followerIndexClusterState =
                    followerClient().admin().cluster().prepareState().clear().setMetaData(true).setIndices(followerIndex).get();
            final String followerUUID = followerIndexClusterState.getState().metaData().index(followerIndex).getIndexUUID();

            final ClusterStateResponse leaderIndexClusterState =
                    leaderClient().admin().cluster().prepareState().clear().setMetaData(true).setIndices(leaderIndex).get();
            final String leaderUUID = leaderIndexClusterState.getState().metaData().index(leaderIndex).getIndexUUID();

            assertThat(
                    e.getMetadata("es.failed_to_remove_retention_leases"),
                    contains(retentionLeaseId(
                            getFollowerCluster().getClusterName(),
                            new Index(followerIndex, followerUUID),
                            getLeaderCluster().getClusterName(),
                            new Index(leaderIndex, leaderUUID))));
        } finally {
            for (final ObjectCursor<DiscoveryNode> senderNode : followerClusterState.getState().nodes().getDataNodes().values()) {
                final MockTransportService senderTransportService =
                        (MockTransportService) getFollowerCluster().getInstance(TransportService.class, senderNode.value.getName());
                senderTransportService.clearAllRules();
            }
        }
    }

    /**
     * Extract the shard stats from an indices stats response, with the stats ordered by shard ID with primaries first. This is to have a
     * consistent ordering when comparing two responses.
     *
     * @param stats the indices stats
     * @return the shard stats in sorted order with (shard ID, primary) as the sort key
     */
    private List<ShardStats> getShardsStats(final IndicesStatsResponse stats) {
        return Arrays.stream(stats.getShards())
                .sorted((s, t) -> {
                    if (s.getShardRouting().shardId().id() == t.getShardRouting().shardId().id()) {
                        return -Boolean.compare(s.getShardRouting().primary(), t.getShardRouting().primary());
                    } else {
                        return Integer.compare(s.getShardRouting().shardId().id(), t.getShardRouting().shardId().id());
                    }
                })
                .collect(Collectors.toList());
    }

    private String getRetentionLeaseId(final String followerIndex, final String leaderIndex) {
        final ClusterStateResponse followerIndexClusterState =
                followerClient().admin().cluster().prepareState().clear().setMetaData(true).setIndices(followerIndex).get();
        final String followerUUID = followerIndexClusterState.getState().metaData().index(followerIndex).getIndexUUID();

        final ClusterStateResponse leaderIndexClusterState =
                leaderClient().admin().cluster().prepareState().clear().setMetaData(true).setIndices(leaderIndex).get();
        final String leaderUUID = leaderIndexClusterState.getState().metaData().index(leaderIndex).getIndexUUID();

        return getRetentionLeaseId(followerIndex, followerUUID, leaderIndex, leaderUUID);
    }

    private String getRetentionLeaseId(String followerIndex, String followerUUID, String leaderIndex, String leaderUUID) {
        return retentionLeaseId(
                            getFollowerCluster().getClusterName(),
                            new Index(followerIndex, followerUUID),
                            getLeaderCluster().getClusterName(),
                            new Index(leaderIndex, leaderUUID));
    }

    private void assertExpectedDocument(final String followerIndex, final int value) {
        final GetResponse getResponse = followerClient().prepareGet(followerIndex, "doc", Integer.toString(value)).get();
        assertTrue("doc with id [" + value + "] is missing", getResponse.isExists());
        assertTrue((getResponse.getSource().containsKey("f")));
        assertThat(getResponse.getSource().get("f"), equalTo(value));
    }

}
