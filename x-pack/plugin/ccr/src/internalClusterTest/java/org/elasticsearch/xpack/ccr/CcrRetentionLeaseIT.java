/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ccr;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotRequest;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.action.admin.indices.close.CloseIndexRequest;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsRequest;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.action.admin.indices.stats.ShardStats;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.support.ActionTestUtils;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.broadcast.BroadcastResponse;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.seqno.RetentionLease;
import org.elasticsearch.index.seqno.RetentionLeaseActions;
import org.elasticsearch.index.seqno.RetentionLeaseNotFoundException;
import org.elasticsearch.index.seqno.RetentionLeaseUtils;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardClosedException;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.snapshots.RestoreInfo;
import org.elasticsearch.snapshots.RestoreService;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.transport.ConnectTransportException;
import org.elasticsearch.transport.RemoteTransportException;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportActionProxy;
import org.elasticsearch.transport.TransportMessageListener;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.CcrIntegTestCase;
import org.elasticsearch.xpack.ccr.action.repositories.ClearCcrRestoreSessionAction;
import org.elasticsearch.xpack.ccr.repository.CcrRepository;
import org.elasticsearch.xpack.core.ccr.action.ForgetFollowerAction;
import org.elasticsearch.xpack.core.ccr.action.PutFollowAction;
import org.elasticsearch.xpack.core.ccr.action.ResumeFollowAction;
import org.elasticsearch.xpack.core.ccr.action.UnfollowAction;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.ccr.CcrRetentionLeases.retentionLeaseId;
import static org.hamcrest.Matchers.arrayWithSize;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.emptyArray;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;

public class CcrRetentionLeaseIT extends CcrIntegTestCase {

    public static final class RetentionLeaseRenewIntervalSettingPlugin extends Plugin {

        @Override
        public List<Setting<?>> getSettings() {
            return Collections.singletonList(CcrRetentionLeases.RETENTION_LEASE_RENEW_INTERVAL_SETTING);
        }

    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Stream.concat(super.nodePlugins().stream(), Stream.of(RetentionLeaseRenewIntervalSettingPlugin.class))
            .collect(Collectors.toList());
    }

    @Override
    protected Settings followerClusterSettings() {
        return Settings.builder()
            .put(super.followerClusterSettings())
            .put(CcrRetentionLeases.RETENTION_LEASE_RENEW_INTERVAL_SETTING.getKey(), TimeValue.timeValueMillis(200))
            .build();
    }

    private final IndicesOptions indicesOptions = IndicesOptions.strictSingleIndexNoExpandForbidClosed();

    private RestoreSnapshotRequest setUpRestoreSnapshotRequest(
        final String leaderIndex,
        final int numberOfShards,
        final int numberOfReplicas,
        final String followerIndex,
        final int numberOfDocuments
    ) throws IOException {
        final ClusterUpdateSettingsRequest settingsRequest = new ClusterUpdateSettingsRequest(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT)
            .masterNodeTimeout(TimeValue.MAX_VALUE);
        final String chunkSize = ByteSizeValue.of(randomFrom(4, 128, 1024), ByteSizeUnit.KB).getStringRep();
        settingsRequest.persistentSettings(Settings.builder().put(CcrSettings.RECOVERY_CHUNK_SIZE.getKey(), chunkSize));
        assertAcked(followerClient().admin().cluster().updateSettings(settingsRequest).actionGet());

        final String leaderClusterRepoName = CcrRepository.NAME_PREFIX + "leader_cluster";

        final Map<String, String> additionalSettings = new HashMap<>();
        additionalSettings.put(IndexService.RETENTION_LEASE_SYNC_INTERVAL_SETTING.getKey(), TimeValue.timeValueMillis(200).getStringRep());
        final String leaderIndexSettings = getIndexSettings(numberOfShards, numberOfReplicas, additionalSettings);
        assertAcked(
            leaderClient().admin()
                .indices()
                .prepareCreate(leaderIndex)
                .setMasterNodeTimeout(TimeValue.MAX_VALUE)
                .setSource(leaderIndexSettings, XContentType.JSON)
        );
        ensureLeaderGreen(leaderIndex);

        logger.info("indexing [{}] docs", numberOfDocuments);
        for (int i = 0; i < numberOfDocuments; i++) {
            final String source = Strings.format("{\"f\":%d}", i);
            leaderClient().prepareIndex(leaderIndex).setId(Integer.toString(i)).setSource(source, XContentType.JSON).get();
            if (rarely()) {
                leaderClient().admin().indices().prepareFlush(leaderIndex).setForce(true).setWaitIfOngoing(true).get();
            }
        }

        leaderClient().admin().indices().prepareFlush(leaderIndex).setForce(true).setWaitIfOngoing(true).get();

        final Settings.Builder settingsBuilder = Settings.builder()
            .put(IndexMetadata.SETTING_INDEX_PROVIDED_NAME, followerIndex)
            .put(CcrSettings.CCR_FOLLOWING_INDEX_SETTING.getKey(), true);
        return new RestoreSnapshotRequest(TEST_REQUEST_TIMEOUT, leaderClusterRepoName, CcrRepository.LATEST).indexSettings(settingsBuilder)
            .indices(leaderIndex)
            .indicesOptions(indicesOptions)
            .renamePattern("^(.*)$")
            .renameReplacement(followerIndex)
            .masterNodeTimeout(TimeValue.MAX_VALUE)
            .quiet(true);
    }

    public void testRetentionLeaseIsTakenAtTheStartOfRecovery() throws Exception {
        final String leaderIndex = "leader";
        final int numberOfShards = randomIntBetween(1, 3);
        final int numberOfReplicas = between(0, 1);
        final String followerIndex = "follower";
        final int numberOfDocuments = scaledRandomIntBetween(1, 8192);
        final RestoreSnapshotRequest restoreRequest = setUpRestoreSnapshotRequest(
            leaderIndex,
            numberOfShards,
            numberOfReplicas,
            followerIndex,
            numberOfDocuments
        );
        final RestoreService restoreService = getFollowerCluster().getCurrentMasterNodeInstance(RestoreService.class);
        final ClusterService clusterService = getFollowerCluster().getCurrentMasterNodeInstance(ClusterService.class);

        final PlainActionFuture<RestoreInfo> future = startRestore(clusterService, restoreService, restoreRequest);

        // ensure that a retention lease has been put in place on each shard
        assertBusy(() -> {
            final IndicesStatsResponse stats = leaderClient().admin()
                .indices()
                .stats(new IndicesStatsRequest().clear().indices(leaderIndex))
                .actionGet();
            assertNotNull(stats.getShards());
            assertThat(stats.getShards(), arrayWithSize(numberOfShards * (1 + numberOfReplicas)));
            final List<ShardStats> shardsStats = getShardsStats(stats);
            for (int i = 0; i < numberOfShards * (1 + numberOfReplicas); i++) {
                assertNotNull(shardsStats.get(i).getRetentionLeaseStats());
                final Map<String, RetentionLease> currentRetentionLeases = RetentionLeaseUtils.toMapExcludingPeerRecoveryRetentionLeases(
                    shardsStats.get(i).getRetentionLeaseStats().retentionLeases()
                );
                assertThat(Strings.toString(shardsStats.get(i)), currentRetentionLeases.values(), hasSize(1));
                final RetentionLease retentionLease = currentRetentionLeases.values().iterator().next();
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
        final RestoreSnapshotRequest restoreRequest = setUpRestoreSnapshotRequest(
            leaderIndex,
            numberOfShards,
            numberOfReplicas,
            followerIndex,
            numberOfDocuments
        );
        final RestoreService restoreService = getFollowerCluster().getCurrentMasterNodeInstance(RestoreService.class);
        final ClusterService clusterService = getFollowerCluster().getCurrentMasterNodeInstance(ClusterService.class);

        final CountDownLatch latch = new CountDownLatch(1);

        // block the recovery from completing; this ensures the background sync is still running
        final ClusterStateResponse followerClusterState = followerClient().admin()
            .cluster()
            .prepareState(TEST_REQUEST_TIMEOUT)
            .clear()
            .setNodes(true)
            .get();
        for (final DiscoveryNode senderNode : followerClusterState.getState().nodes()) {
            final MockTransportService senderTransportService = (MockTransportService) getFollowerCluster().getInstance(
                TransportService.class,
                senderNode.getName()
            );
            senderTransportService.addSendBehavior((connection, requestId, action, request, options) -> {
                if (ClearCcrRestoreSessionAction.INTERNAL_NAME.equals(action)
                    || TransportActionProxy.getProxyAction(ClearCcrRestoreSessionAction.INTERNAL_NAME).equals(action)) {
                    try {
                        latch.await();
                    } catch (final InterruptedException e) {
                        fail(e.toString());
                    }
                }
                connection.sendRequest(requestId, action, request, options);
            });
        }

        final PlainActionFuture<RestoreInfo> future = startRestore(clusterService, restoreService, restoreRequest);

        try {
            assertRetentionLeaseRenewal(numberOfShards, numberOfReplicas, followerIndex, leaderIndex);
            latch.countDown();
        } finally {
            for (final DiscoveryNode senderNode : followerClusterState.getState().nodes()) {
                final MockTransportService senderTransportService = (MockTransportService) getFollowerCluster().getInstance(
                    TransportService.class,
                    senderNode.getName()
                );
                senderTransportService.clearAllRules();
            }
        }

        final RestoreInfo restoreInfo = future.actionGet();

        assertEquals(restoreInfo.totalShards(), restoreInfo.successfulShards());

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
        final RestoreSnapshotRequest restoreRequest = setUpRestoreSnapshotRequest(
            leaderIndex,
            numberOfShards,
            numberOfReplicas,
            followerIndex,
            numberOfDocuments
        );
        final RestoreService restoreService = getFollowerCluster().getCurrentMasterNodeInstance(RestoreService.class);
        final ClusterService clusterService = getFollowerCluster().getCurrentMasterNodeInstance(ClusterService.class);

        final RestoreInfo restoreInfo = startRestore(clusterService, restoreService, restoreRequest).actionGet();
        final long start = System.nanoTime();

        /*
         * We want to ensure that the retention leases have been synced to all shard copies, as otherwise they might sync between the two
         * times that we sample the retention leases, which would cause our check to fail.
         */
        final TimeValue syncIntervalSetting = IndexService.RETENTION_LEASE_SYNC_INTERVAL_SETTING.get(
            leaderClient().admin()
                .indices()
                .prepareGetSettings(TEST_REQUEST_TIMEOUT, leaderIndex)
                .get()
                .getIndexToSettings()
                .get(leaderIndex)
        );
        final long syncEnd = System.nanoTime();
        Thread.sleep(Math.max(0, randomIntBetween(2, 4) * syncIntervalSetting.millis() - TimeUnit.NANOSECONDS.toMillis(syncEnd - start)));

        final ClusterStateResponse leaderIndexClusterState = leaderClient().admin()
            .cluster()
            .prepareState(TEST_REQUEST_TIMEOUT)
            .clear()
            .setMetadata(true)
            .setIndices(leaderIndex)
            .get();
        final String leaderUUID = leaderIndexClusterState.getState().metadata().getProject().index(leaderIndex).getIndexUUID();

        /*
         * We want to ensure that the background renewal is cancelled at the end of recovery. To do this, we will sleep a small multiple
         * of the renew interval. If the renews are not cancelled, we expect that a renewal would have been sent while we were sleeping.
         * After we wake up, it should be the case that the retention leases are the same (same timestamp) as that indicates that they were
         * not renewed while we were sleeping.
         */
        assertBusy(() -> {
            // sample the leases after recovery
            final List<Map<String, RetentionLease>> retentionLeases = new ArrayList<>();
            assertBusy(() -> {
                retentionLeases.clear();
                final IndicesStatsResponse stats = leaderClient().admin()
                    .indices()
                    .stats(new IndicesStatsRequest().clear().indices(leaderIndex))
                    .actionGet();
                assertNotNull(stats.getShards());
                assertThat(stats.getShards(), arrayWithSize(numberOfShards * (1 + numberOfReplicas)));
                final List<ShardStats> shardsStats = getShardsStats(stats);
                for (int i = 0; i < numberOfShards * (1 + numberOfReplicas); i++) {
                    assertNotNull(shardsStats.get(i).getRetentionLeaseStats());
                    final Map<String, RetentionLease> currentRetentionLeases = RetentionLeaseUtils
                        .toMapExcludingPeerRecoveryRetentionLeases(shardsStats.get(i).getRetentionLeaseStats().retentionLeases());
                    assertThat(Strings.toString(shardsStats.get(i)), currentRetentionLeases.values(), hasSize(1));
                    final ClusterStateResponse followerIndexClusterState = followerClient().admin()
                        .cluster()
                        .prepareState(TEST_REQUEST_TIMEOUT)
                        .clear()
                        .setMetadata(true)
                        .setIndices(followerIndex)
                        .get();
                    final String followerUUID = followerIndexClusterState.getState()
                        .metadata()
                        .getProject()
                        .index(followerIndex)
                        .getIndexUUID();
                    final RetentionLease retentionLease = currentRetentionLeases.values().iterator().next();
                    final String expectedRetentionLeaseId = retentionLeaseId(
                        getFollowerCluster().getClusterName(),
                        new Index(followerIndex, followerUUID),
                        getLeaderCluster().getClusterName(),
                        new Index(leaderIndex, leaderUUID)
                    );
                    assertThat(retentionLease.id(), equalTo(expectedRetentionLeaseId));
                    retentionLeases.add(currentRetentionLeases);
                }
            });
            // sleep a small multiple of the renew interval
            final TimeValue renewIntervalSetting = CcrRetentionLeases.RETENTION_LEASE_RENEW_INTERVAL_SETTING.get(followerClusterSettings());
            final long renewEnd = System.nanoTime();
            Thread.sleep(
                Math.max(0, randomIntBetween(2, 4) * renewIntervalSetting.millis() - TimeUnit.NANOSECONDS.toMillis(renewEnd - start))
            );

            // now ensure that the retention leases are the same
            final IndicesStatsResponse stats = leaderClient().admin()
                .indices()
                .stats(new IndicesStatsRequest().clear().indices(leaderIndex))
                .actionGet();
            assertNotNull(stats.getShards());
            assertThat(stats.getShards(), arrayWithSize(numberOfShards * (1 + numberOfReplicas)));
            final List<ShardStats> shardsStats = getShardsStats(stats);
            for (int i = 0; i < numberOfShards * (1 + numberOfReplicas); i++) {
                if (shardsStats.get(i).getShardRouting().primary() == false) {
                    continue;
                }
                assertNotNull(shardsStats.get(i).getRetentionLeaseStats());
                final Map<String, RetentionLease> currentRetentionLeases = RetentionLeaseUtils.toMapExcludingPeerRecoveryRetentionLeases(
                    shardsStats.get(i).getRetentionLeaseStats().retentionLeases()
                );
                assertThat(Strings.toString(shardsStats.get(i)), currentRetentionLeases.values(), hasSize(1));
                final ClusterStateResponse followerIndexClusterState = followerClient().admin()
                    .cluster()
                    .prepareState(TEST_REQUEST_TIMEOUT)
                    .clear()
                    .setMetadata(true)
                    .setIndices(followerIndex)
                    .get();
                final String followerUUID = followerIndexClusterState.getState()
                    .metadata()
                    .getProject()
                    .index(followerIndex)
                    .getIndexUUID();
                final RetentionLease retentionLease = currentRetentionLeases.values().iterator().next();
                assertThat(retentionLease.id(), equalTo(getRetentionLeaseId(followerIndex, followerUUID, leaderIndex, leaderUUID)));
                // we assert that retention leases are being renewed by an increase in the timestamp
                assertThat(retentionLease.timestamp(), equalTo(retentionLeases.get(i).values().iterator().next().timestamp()));
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
        final String leaderIndexSettings = getIndexSettings(numberOfShards, 0);
        assertAcked(leaderClient().admin().indices().prepareCreate(leaderIndex).setSource(leaderIndexSettings, XContentType.JSON).get());
        ensureLeaderYellow(leaderIndex);
        final PutFollowAction.Request followRequest = putFollow(leaderIndex, followerIndex);
        followerClient().execute(PutFollowAction.INSTANCE, followRequest).get();

        ensureFollowerGreen(true, followerIndex);

        final String retentionLeaseId = getRetentionLeaseId(followerIndex, leaderIndex);

        final IndicesStatsResponse stats = leaderClient().admin()
            .indices()
            .stats(new IndicesStatsRequest().clear().indices(leaderIndex))
            .actionGet();
        final List<ShardStats> shardsStats = getShardsStats(stats);
        for (final ShardStats shardStats : shardsStats) {
            final Map<String, RetentionLease> retentionLeases = RetentionLeaseUtils.toMapExcludingPeerRecoveryRetentionLeases(
                shardStats.getRetentionLeaseStats().retentionLeases()
            );
            assertThat(Strings.toString(shardStats), retentionLeases.values(), hasSize(1));
            assertThat(retentionLeases.values().iterator().next().id(), equalTo(retentionLeaseId));
        }

        // we will sometimes fake that some of the retention leases are already removed on the leader shard
        final Set<Integer> shardIds = new HashSet<>(
            randomSubsetOf(randomIntBetween(0, numberOfShards), IntStream.range(0, numberOfShards).boxed().collect(Collectors.toSet()))
        );

        final ClusterStateResponse followerClusterState = followerClient().admin()
            .cluster()
            .prepareState(TEST_REQUEST_TIMEOUT)
            .clear()
            .setNodes(true)
            .get();
        try {
            for (final DiscoveryNode senderNode : followerClusterState.getState().nodes()) {
                final MockTransportService senderTransportService = (MockTransportService) getFollowerCluster().getInstance(
                    TransportService.class,
                    senderNode.getName()
                );
                senderTransportService.addSendBehavior((connection, requestId, action, request, options) -> {
                    if (RetentionLeaseActions.REMOVE.name().equals(action)
                        || TransportActionProxy.getProxyAction(RetentionLeaseActions.REMOVE.name()).equals(action)) {
                        final RetentionLeaseActions.RemoveRequest removeRequest = (RetentionLeaseActions.RemoveRequest) request;
                        if (shardIds.contains(removeRequest.getShardId().id())) {
                            final String primaryShardNodeId = getLeaderCluster().clusterService()
                                .state()
                                .routingTable()
                                .index(leaderIndex)
                                .shard(removeRequest.getShardId().id())
                                .primaryShard()
                                .currentNodeId();
                            final String primaryShardNodeName = getLeaderCluster().clusterService()
                                .state()
                                .nodes()
                                .get(primaryShardNodeId)
                                .getName();
                            final IndexShard primary = getLeaderCluster().getInstance(IndicesService.class, primaryShardNodeName)
                                .getShardOrNull(removeRequest.getShardId());
                            final CountDownLatch latch = new CountDownLatch(1);
                            primary.removeRetentionLease(retentionLeaseId, ActionTestUtils.assertNoFailureListener(r -> latch.countDown()));
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
            assertAcked(followerClient().admin().indices().close(new CloseIndexRequest(followerIndex)).actionGet());
            assertAcked(
                followerClient().execute(
                    UnfollowAction.INSTANCE,
                    new UnfollowAction.Request(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, followerIndex)
                ).actionGet()
            );

            final IndicesStatsResponse afterUnfollowStats = leaderClient().admin()
                .indices()
                .stats(new IndicesStatsRequest().clear().indices(leaderIndex))
                .actionGet();
            final List<ShardStats> afterUnfollowShardsStats = getShardsStats(afterUnfollowStats);
            for (final ShardStats shardStats : afterUnfollowShardsStats) {
                assertThat(
                    Strings.toString(shardStats),
                    RetentionLeaseUtils.toMapExcludingPeerRecoveryRetentionLeases(shardStats.getRetentionLeaseStats().retentionLeases())
                        .values(),
                    empty()
                );
            }
        } finally {
            for (final DiscoveryNode senderNode : followerClusterState.getState().nodes()) {
                final MockTransportService senderTransportService = (MockTransportService) getFollowerCluster().getInstance(
                    TransportService.class,
                    senderNode.getName()
                );
                senderTransportService.clearAllRules();
            }
        }
    }

    public void testUnfollowFailsToRemoveRetentionLeases() throws Exception {
        final String leaderIndex = "leader";
        final String followerIndex = "follower";
        final int numberOfShards = randomIntBetween(1, 4);
        final String leaderIndexSettings = getIndexSettings(numberOfShards, 0);
        assertAcked(leaderClient().admin().indices().prepareCreate(leaderIndex).setSource(leaderIndexSettings, XContentType.JSON).get());
        ensureLeaderYellow(leaderIndex);
        final PutFollowAction.Request followRequest = putFollow(leaderIndex, followerIndex);
        followerClient().execute(PutFollowAction.INSTANCE, followRequest).get();

        ensureFollowerGreen(true, followerIndex);

        pauseFollow(followerIndex);
        followerClient().admin().indices().close(new CloseIndexRequest(followerIndex).masterNodeTimeout(TimeValue.MAX_VALUE)).actionGet();

        // we will disrupt requests to remove retention leases for these random shards
        final Set<Integer> shardIds = new HashSet<>(
            randomSubsetOf(randomIntBetween(1, numberOfShards), IntStream.range(0, numberOfShards).boxed().collect(Collectors.toSet()))
        );

        final ClusterStateResponse followerClusterState = followerClient().admin()
            .cluster()
            .prepareState(TEST_REQUEST_TIMEOUT)
            .clear()
            .setNodes(true)
            .get();
        try {
            for (final DiscoveryNode senderNode : followerClusterState.getState().nodes()) {
                final MockTransportService senderTransportService = (MockTransportService) getFollowerCluster().getInstance(
                    TransportService.class,
                    senderNode.getName()
                );
                senderTransportService.addSendBehavior((connection, requestId, action, request, options) -> {
                    if (RetentionLeaseActions.REMOVE.name().equals(action)
                        || TransportActionProxy.getProxyAction(RetentionLeaseActions.REMOVE.name()).equals(action)) {
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
                () -> followerClient().execute(
                    UnfollowAction.INSTANCE,
                    new UnfollowAction.Request(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, followerIndex)
                ).actionGet()
            );

            final ClusterStateResponse followerIndexClusterState = followerClient().admin()
                .cluster()
                .prepareState(TEST_REQUEST_TIMEOUT)
                .clear()
                .setMetadata(true)
                .setIndices(followerIndex)
                .get();
            final String followerUUID = followerIndexClusterState.getState().metadata().getProject().index(followerIndex).getIndexUUID();

            final ClusterStateResponse leaderIndexClusterState = leaderClient().admin()
                .cluster()
                .prepareState(TEST_REQUEST_TIMEOUT)
                .clear()
                .setMetadata(true)
                .setIndices(leaderIndex)
                .get();
            final String leaderUUID = leaderIndexClusterState.getState().metadata().getProject().index(leaderIndex).getIndexUUID();

            assertThat(
                e.getMetadata("es.failed_to_remove_retention_leases"),
                contains(
                    retentionLeaseId(
                        getFollowerCluster().getClusterName(),
                        new Index(followerIndex, followerUUID),
                        getLeaderCluster().getClusterName(),
                        new Index(leaderIndex, leaderUUID)
                    )
                )
            );
        } finally {
            for (final DiscoveryNode senderNode : followerClusterState.getState().nodes()) {
                final MockTransportService senderTransportService = (MockTransportService) getFollowerCluster().getInstance(
                    TransportService.class,
                    senderNode.getName()
                );
                senderTransportService.clearAllRules();
            }
        }
    }

    public void testRetentionLeaseRenewedWhileFollowing() throws Exception {
        final String leaderIndex = "leader";
        final String followerIndex = "follower";
        final int numberOfShards = randomIntBetween(1, 4);
        final int numberOfReplicas = randomIntBetween(0, 1);
        final Map<String, String> additionalIndexSettings = new HashMap<>();
        additionalIndexSettings.put(
            IndexService.RETENTION_LEASE_SYNC_INTERVAL_SETTING.getKey(),
            TimeValue.timeValueMillis(200).getStringRep()
        );
        final String leaderIndexSettings = getIndexSettings(numberOfShards, numberOfReplicas, additionalIndexSettings);
        assertAcked(leaderClient().admin().indices().prepareCreate(leaderIndex).setSource(leaderIndexSettings, XContentType.JSON).get());
        ensureLeaderYellow(leaderIndex);
        final PutFollowAction.Request followRequest = putFollow(leaderIndex, followerIndex);
        followerClient().execute(PutFollowAction.INSTANCE, followRequest).get();

        ensureFollowerGreen(true, followerIndex);
        assertRetentionLeaseRenewal(numberOfShards, numberOfReplicas, followerIndex, leaderIndex);
    }

    public void testRetentionLeaseAdvancesWhileFollowing() throws Exception {
        final String leaderIndex = "leader";
        final String followerIndex = "follower";
        final int numberOfShards = randomIntBetween(1, 4);
        final int numberOfReplicas = randomIntBetween(0, 1);
        final Map<String, String> additionalIndexSettings = new HashMap<>();
        additionalIndexSettings.put(
            IndexService.RETENTION_LEASE_SYNC_INTERVAL_SETTING.getKey(),
            TimeValue.timeValueMillis(200).getStringRep()
        );
        final String leaderIndexSettings = getIndexSettings(numberOfShards, numberOfReplicas, additionalIndexSettings);
        assertAcked(leaderClient().admin().indices().prepareCreate(leaderIndex).setSource(leaderIndexSettings, XContentType.JSON).get());
        ensureLeaderYellow(leaderIndex);
        final PutFollowAction.Request followRequest = putFollow(leaderIndex, followerIndex);
        followerClient().execute(PutFollowAction.INSTANCE, followRequest).get();

        ensureFollowerGreen(true, followerIndex);

        final int numberOfDocuments = randomIntBetween(128, 2048);
        logger.debug("indexing [{}] docs", numberOfDocuments);
        for (int i = 0; i < numberOfDocuments; i++) {
            final String source = Strings.format("{\"f\":%d}", i);
            leaderClient().prepareIndex(leaderIndex).setId(Integer.toString(i)).setSource(source, XContentType.JSON).get();
            if (rarely()) {
                leaderClient().admin().indices().prepareFlush(leaderIndex).setForce(true).setWaitIfOngoing(true).get();
            }
        }

        // wait until the follower global checkpoints have caught up to the leader
        assertIndexFullyReplicatedToFollower(leaderIndex, followerIndex);

        final List<ShardStats> leaderShardsStats = getShardsStats(leaderClient().admin().indices().prepareStats(leaderIndex).get());
        final Map<Integer, Long> leaderGlobalCheckpoints = new HashMap<>();
        for (final ShardStats leaderShardStats : leaderShardsStats) {
            final ShardRouting routing = leaderShardStats.getShardRouting();
            if (routing.primary() == false) {
                continue;
            }
            leaderGlobalCheckpoints.put(routing.id(), leaderShardStats.getSeqNoStats().getGlobalCheckpoint());
        }

        // now assert that the retention leases have advanced to the global checkpoints
        assertBusy(() -> {
            final IndicesStatsResponse stats = leaderClient().admin()
                .indices()
                .stats(new IndicesStatsRequest().clear().indices(leaderIndex))
                .actionGet();
            assertNotNull(stats.getShards());
            assertThat(stats.getShards(), arrayWithSize(numberOfShards * (1 + numberOfReplicas)));
            final List<ShardStats> shardsStats = getShardsStats(stats);
            for (int i = 0; i < numberOfShards * (1 + numberOfReplicas); i++) {
                assertNotNull(shardsStats.get(i).getRetentionLeaseStats());
                final Map<String, RetentionLease> currentRetentionLeases = RetentionLeaseUtils.toMapExcludingPeerRecoveryRetentionLeases(
                    shardsStats.get(i).getRetentionLeaseStats().retentionLeases()
                );
                assertThat(Strings.toString(shardsStats.get(i)), currentRetentionLeases.values(), hasSize(1));
                final RetentionLease retentionLease = currentRetentionLeases.values().iterator().next();
                assertThat(retentionLease.id(), equalTo(getRetentionLeaseId(followerIndex, leaderIndex)));
                // we assert that retention leases are being advanced
                assertThat(
                    retentionLease.retainingSequenceNumber(),
                    equalTo(leaderGlobalCheckpoints.get(shardsStats.get(i).getShardRouting().id()) + 1)
                );
            }
        });
    }

    public void testRetentionLeaseRenewalIsCancelledWhenFollowingIsPaused() throws Exception {
        final String leaderIndex = "leader";
        final String followerIndex = "follower";
        final int numberOfShards = randomIntBetween(1, 4);
        final int numberOfReplicas = randomIntBetween(0, 1);
        final Map<String, String> additionalIndexSettings = new HashMap<>();
        additionalIndexSettings.put(
            IndexService.RETENTION_LEASE_SYNC_INTERVAL_SETTING.getKey(),
            TimeValue.timeValueMillis(200).getStringRep()
        );
        final String leaderIndexSettings = getIndexSettings(numberOfShards, numberOfReplicas, additionalIndexSettings);
        assertAcked(leaderClient().admin().indices().prepareCreate(leaderIndex).setSource(leaderIndexSettings, XContentType.JSON).get());
        ensureLeaderYellow(leaderIndex);
        final PutFollowAction.Request followRequest = putFollow(leaderIndex, followerIndex);
        followerClient().execute(PutFollowAction.INSTANCE, followRequest).get();

        ensureFollowerGreen(true, followerIndex);

        final long start = System.nanoTime();
        pauseFollow(followerIndex);

        /*
         * We want to ensure that the retention leases have been synced to all shard copies, as otherwise they might sync between the two
         * times that we sample the retention leases, which would cause our check to fail.
         */
        final TimeValue syncIntervalSetting = IndexService.RETENTION_LEASE_SYNC_INTERVAL_SETTING.get(
            leaderClient().admin()
                .indices()
                .prepareGetSettings(TEST_REQUEST_TIMEOUT, leaderIndex)
                .get()
                .getIndexToSettings()
                .get(leaderIndex)
        );
        final long syncEnd = System.nanoTime();
        Thread.sleep(Math.max(0, randomIntBetween(2, 4) * syncIntervalSetting.millis() - TimeUnit.NANOSECONDS.toMillis(syncEnd - start)));

        final ClusterStateResponse leaderIndexClusterState = leaderClient().admin()
            .cluster()
            .prepareState(TEST_REQUEST_TIMEOUT)
            .clear()
            .setMetadata(true)
            .setIndices(leaderIndex)
            .get();
        final String leaderUUID = leaderIndexClusterState.getState().metadata().getProject().index(leaderIndex).getIndexUUID();
        /*
         * We want to ensure that the background renewal is cancelled after pausing. To do this, we will sleep a small multiple of the renew
         * interval. If the renews are not cancelled, we expect that a renewal would have been sent while we were sleeping. After we wake
         * up, it should be the case that the retention leases are the same (same timestamp) as that indicates that they were not renewed
         * while we were sleeping.
         */
        assertBusy(() -> {
            // sample the leases after pausing
            final List<Map<String, RetentionLease>> retentionLeases = new ArrayList<>();
            assertBusy(() -> {
                retentionLeases.clear();
                final IndicesStatsResponse stats = leaderClient().admin()
                    .indices()
                    .stats(new IndicesStatsRequest().clear().indices(leaderIndex))
                    .actionGet();
                assertNotNull(stats.getShards());
                assertThat(stats.getShards(), arrayWithSize(numberOfShards * (1 + numberOfReplicas)));
                final List<ShardStats> shardsStats = getShardsStats(stats);
                for (int i = 0; i < numberOfShards * (1 + numberOfReplicas); i++) {
                    assertNotNull(shardsStats.get(i).getRetentionLeaseStats());
                    final Map<String, RetentionLease> currentRetentionLeases = RetentionLeaseUtils
                        .toMapExcludingPeerRecoveryRetentionLeases(shardsStats.get(i).getRetentionLeaseStats().retentionLeases());
                    assertThat(Strings.toString(shardsStats.get(i)), currentRetentionLeases.values(), hasSize(1));
                    final ClusterStateResponse followerIndexClusterState = followerClient().admin()
                        .cluster()
                        .prepareState(TEST_REQUEST_TIMEOUT)
                        .clear()
                        .setMetadata(true)
                        .setIndices(followerIndex)
                        .get();
                    final String followerUUID = followerIndexClusterState.getState()
                        .metadata()
                        .getProject()
                        .index(followerIndex)
                        .getIndexUUID();
                    final RetentionLease retentionLease = currentRetentionLeases.values().iterator().next();
                    final String expectedRetentionLeaseId = retentionLeaseId(
                        getFollowerCluster().getClusterName(),
                        new Index(followerIndex, followerUUID),
                        getLeaderCluster().getClusterName(),
                        new Index(leaderIndex, leaderUUID)
                    );
                    assertThat(retentionLease.id(), equalTo(expectedRetentionLeaseId));
                    retentionLeases.add(currentRetentionLeases);
                }
            });
            // sleep a small multiple of the renew interval
            final TimeValue renewIntervalSetting = CcrRetentionLeases.RETENTION_LEASE_RENEW_INTERVAL_SETTING.get(followerClusterSettings());
            final long renewEnd = System.nanoTime();
            Thread.sleep(
                Math.max(0, randomIntBetween(2, 4) * renewIntervalSetting.millis() - TimeUnit.NANOSECONDS.toMillis(renewEnd - start))
            );

            // now ensure that the retention leases are the same
            final IndicesStatsResponse stats = leaderClient().admin()
                .indices()
                .stats(new IndicesStatsRequest().clear().indices(leaderIndex))
                .actionGet();
            assertNotNull(stats.getShards());
            assertThat(stats.getShards(), arrayWithSize(numberOfShards * (1 + numberOfReplicas)));
            final List<ShardStats> shardsStats = getShardsStats(stats);
            for (int i = 0; i < numberOfShards * (1 + numberOfReplicas); i++) {
                if (shardsStats.get(i).getShardRouting().primary() == false) {
                    continue;
                }
                assertNotNull(shardsStats.get(i).getRetentionLeaseStats());
                final Map<String, RetentionLease> currentRetentionLeases = RetentionLeaseUtils.toMapExcludingPeerRecoveryRetentionLeases(
                    shardsStats.get(i).getRetentionLeaseStats().retentionLeases()
                );
                assertThat(Strings.toString(shardsStats.get(i)), currentRetentionLeases.values(), hasSize(1));
                final ClusterStateResponse followerIndexClusterState = followerClient().admin()
                    .cluster()
                    .prepareState(TEST_REQUEST_TIMEOUT)
                    .clear()
                    .setMetadata(true)
                    .setIndices(followerIndex)
                    .get();
                final String followerUUID = followerIndexClusterState.getState()
                    .metadata()
                    .getProject()
                    .index(followerIndex)
                    .getIndexUUID();
                final RetentionLease retentionLease = currentRetentionLeases.values().iterator().next();
                assertThat(retentionLease.id(), equalTo(getRetentionLeaseId(followerIndex, followerUUID, leaderIndex, leaderUUID)));
                // we assert that retention leases are not being renewed by an unchanged timestamp
                assertThat(retentionLease.timestamp(), equalTo(retentionLeases.get(i).values().iterator().next().timestamp()));
            }
        });
    }

    public void testRetentionLeaseRenewalIsResumedWhenFollowingIsResumed() throws Exception {
        final String leaderIndex = "leader";
        final String followerIndex = "follower";
        final int numberOfShards = randomIntBetween(1, 4);
        final int numberOfReplicas = randomIntBetween(0, 1);
        final Map<String, String> additionalIndexSettings = new HashMap<>();
        additionalIndexSettings.put(
            IndexService.RETENTION_LEASE_SYNC_INTERVAL_SETTING.getKey(),
            TimeValue.timeValueMillis(200).getStringRep()
        );
        final String leaderIndexSettings = getIndexSettings(numberOfShards, numberOfReplicas, additionalIndexSettings);
        assertAcked(leaderClient().admin().indices().prepareCreate(leaderIndex).setSource(leaderIndexSettings, XContentType.JSON).get());
        ensureLeaderYellow(leaderIndex);
        final PutFollowAction.Request followRequest = putFollow(leaderIndex, followerIndex);
        followerClient().execute(PutFollowAction.INSTANCE, followRequest).get();

        ensureFollowerGreen(true, followerIndex);

        pauseFollow(followerIndex);

        followerClient().execute(ResumeFollowAction.INSTANCE, resumeFollow(followerIndex)).actionGet();

        ensureFollowerGreen(true, followerIndex);

        assertRetentionLeaseRenewal(numberOfShards, numberOfReplicas, followerIndex, leaderIndex);
    }

    public void testRetentionLeaseIsAddedIfItDisappearsWhileFollowing() throws Exception {
        final String leaderIndex = "leader";
        final String followerIndex = "follower";
        final int numberOfShards = 1;
        final int numberOfReplicas = 1;
        final Map<String, String> additionalIndexSettings = new HashMap<>();
        additionalIndexSettings.put(
            IndexService.RETENTION_LEASE_SYNC_INTERVAL_SETTING.getKey(),
            TimeValue.timeValueMillis(200).getStringRep()
        );
        final String leaderIndexSettings = getIndexSettings(numberOfShards, numberOfReplicas, additionalIndexSettings);
        assertAcked(leaderClient().admin().indices().prepareCreate(leaderIndex).setSource(leaderIndexSettings, XContentType.JSON).get());
        ensureLeaderYellow(leaderIndex);
        final PutFollowAction.Request followRequest = putFollow(leaderIndex, followerIndex);
        followerClient().execute(PutFollowAction.INSTANCE, followRequest).get();

        ensureFollowerGreen(true, followerIndex);

        final CountDownLatch latch = new CountDownLatch(1);

        final ClusterStateResponse followerClusterState = followerClient().admin()
            .cluster()
            .prepareState(TEST_REQUEST_TIMEOUT)
            .clear()
            .setNodes(true)
            .get();
        try {
            for (final DiscoveryNode senderNode : followerClusterState.getState().nodes()) {
                final MockTransportService senderTransportService = (MockTransportService) getFollowerCluster().getInstance(
                    TransportService.class,
                    senderNode.getName()
                );
                senderTransportService.addSendBehavior((connection, requestId, action, request, options) -> {
                    if (RetentionLeaseActions.RENEW.name().equals(action)
                        || TransportActionProxy.getProxyAction(RetentionLeaseActions.RENEW.name()).equals(action)) {
                        final RetentionLeaseActions.RenewRequest renewRequest = (RetentionLeaseActions.RenewRequest) request;
                        final String retentionLeaseId = getRetentionLeaseId(followerIndex, leaderIndex);
                        if (retentionLeaseId.equals(renewRequest.getId())) {
                            logger.info("--> intercepting renewal request for retention lease [{}]", retentionLeaseId);
                            senderTransportService.clearAllRules();
                            final String primaryShardNodeId = getLeaderCluster().clusterService()
                                .state()
                                .routingTable()
                                .index(leaderIndex)
                                .shard(renewRequest.getShardId().id())
                                .primaryShard()
                                .currentNodeId();
                            final String primaryShardNodeName = getLeaderCluster().clusterService()
                                .state()
                                .nodes()
                                .get(primaryShardNodeId)
                                .getName();
                            final IndexShard primary = getLeaderCluster().getInstance(IndicesService.class, primaryShardNodeName)
                                .getShardOrNull(renewRequest.getShardId());
                            final CountDownLatch innerLatch = new CountDownLatch(1);
                            try {
                                // this forces the background renewal from following to face a retention lease not found exception
                                logger.info("--> removing retention lease [{}] on the leader", retentionLeaseId);
                                primary.removeRetentionLease(
                                    retentionLeaseId,
                                    ActionTestUtils.assertNoFailureListener(r -> innerLatch.countDown())
                                );
                                logger.info(
                                    "--> waiting for the removed retention lease [{}] to be synced on the leader",
                                    retentionLeaseId
                                );
                                innerLatch.await();
                                logger.info("--> removed retention lease [{}] on the leader", retentionLeaseId);
                            } catch (final Exception e) {
                                throw new AssertionError("failed to remove retention lease [" + retentionLeaseId + "] on the leader");
                            } finally {
                                latch.countDown();
                            }
                        }
                    }
                    connection.sendRequest(requestId, action, request, options);
                });
            }

            latch.await();

            assertRetentionLeaseRenewal(numberOfShards, numberOfReplicas, followerIndex, leaderIndex);
        } finally {
            for (final DiscoveryNode senderNode : followerClusterState.getState().nodes()) {
                final MockTransportService senderTransportService = (MockTransportService) getFollowerCluster().getInstance(
                    TransportService.class,
                    senderNode.getName()
                );
                senderTransportService.clearAllRules();
            }
        }
    }

    /**
     * This test is fairly evil. This test is to ensure that we are protected against a race condition when unfollowing and a background
     * renewal fires. The action of unfollowing will remove retention leases from the leader. If a background renewal is firing at that
     * time, it means that we will be met with a retention lease not found exception. That will in turn trigger behavior to attempt to
     * re-add the retention lease, which means we are left in a situation where we have unfollowed, but the retention lease still remains
     * on the leader. However, we have a guard against this in the callback after the retention lease not found exception is thrown, which
     * checks if the shard follow node task is cancelled or completed.
     *
     * To test this this behavior is correct, we capture the call to renew the retention lease. Then, we will step in between and execute
     * an unfollow request. This will remove the retention lease on the leader. At this point, we can unlatch the renew call, which will
     * now be met with a retention lease not found exception. We will cheat and wait for that response to come back, and then synchronously
     * trigger the listener which will check to see if the shard follow node task is cancelled or completed, and if not, add the retention
     * lease back. After that listener returns, we can check to see if a retention lease exists on the leader.
     *
     * Note, this done mean that listener will fire twice, once in our onResponseReceived hook, and once after our onResponseReceived
     * callback returns. 🤷‍♀️
     *
     * @throws Exception if an exception occurs in the main test thread
     */
    public void testPeriodicRenewalDoesNotAddRetentionLeaseAfterUnfollow() throws Exception {
        final String leaderIndex = "leader";
        final String followerIndex = "follower";
        final int numberOfShards = 1;
        final int numberOfReplicas = 1;
        final Map<String, String> additionalIndexSettings = new HashMap<>();
        additionalIndexSettings.put(
            IndexService.RETENTION_LEASE_SYNC_INTERVAL_SETTING.getKey(),
            TimeValue.timeValueMillis(200).getStringRep()
        );
        final String leaderIndexSettings = getIndexSettings(numberOfShards, numberOfReplicas, additionalIndexSettings);
        assertAcked(leaderClient().admin().indices().prepareCreate(leaderIndex).setSource(leaderIndexSettings, XContentType.JSON).get());
        ensureLeaderYellow(leaderIndex);
        final PutFollowAction.Request followRequest = putFollow(leaderIndex, followerIndex);
        followerClient().execute(PutFollowAction.INSTANCE, followRequest).get();

        ensureFollowerGreen(true, followerIndex);

        final CountDownLatch removeLeaseLatch = new CountDownLatch(1);
        final CountDownLatch unfollowLatch = new CountDownLatch(1);
        final CountDownLatch responseLatch = new CountDownLatch(1);

        final ClusterStateResponse followerClusterState = followerClient().admin()
            .cluster()
            .prepareState(TEST_REQUEST_TIMEOUT)
            .clear()
            .setNodes(true)
            .get();

        try {
            for (final DiscoveryNode senderNode : followerClusterState.getState().nodes()) {
                final MockTransportService senderTransportService = (MockTransportService) getFollowerCluster().getInstance(
                    TransportService.class,
                    senderNode.getName()
                );
                senderTransportService.addSendBehavior((connection, requestId, action, request, options) -> {
                    if (RetentionLeaseActions.RENEW.name().equals(action)
                        || TransportActionProxy.getProxyAction(RetentionLeaseActions.RENEW.name()).equals(action)) {
                        final String retentionLeaseId = getRetentionLeaseId(followerIndex, leaderIndex);
                        logger.info("--> blocking renewal request for retention lease [{}] until unfollowed", retentionLeaseId);
                        try {
                            removeLeaseLatch.countDown();
                            unfollowLatch.await();

                            senderTransportService.addMessageListener(new TransportMessageListener() {

                                @SuppressWarnings("rawtypes")
                                @Override
                                public void onResponseReceived(final long responseRequestId, final Transport.ResponseContext context) {
                                    if (requestId == responseRequestId) {
                                        final RetentionLeaseNotFoundException e = new RetentionLeaseNotFoundException(retentionLeaseId);
                                        context.handler().handleException(new RemoteTransportException(e.getMessage(), e));
                                        responseLatch.countDown();
                                        senderTransportService.removeMessageListener(this);
                                    }
                                }

                            });

                        } catch (final InterruptedException e) {
                            Thread.currentThread().interrupt();
                            fail(e.toString());
                        }
                    }
                    connection.sendRequest(requestId, action, request, options);
                });
            }

            removeLeaseLatch.await();

            pauseFollow(followerIndex);
            assertAcked(followerClient().admin().indices().close(new CloseIndexRequest(followerIndex)).actionGet());
            assertAcked(
                followerClient().execute(
                    UnfollowAction.INSTANCE,
                    new UnfollowAction.Request(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, followerIndex)
                ).actionGet()
            );

            unfollowLatch.countDown();

            responseLatch.await();

            final IndicesStatsResponse afterUnfollowStats = leaderClient().admin()
                .indices()
                .stats(new IndicesStatsRequest().clear().indices(leaderIndex))
                .actionGet();
            final List<ShardStats> afterUnfollowShardsStats = getShardsStats(afterUnfollowStats);
            for (final ShardStats shardStats : afterUnfollowShardsStats) {
                assertNotNull(shardStats.getRetentionLeaseStats());
                assertThat(
                    Strings.toString(shardStats),
                    RetentionLeaseUtils.toMapExcludingPeerRecoveryRetentionLeases(shardStats.getRetentionLeaseStats().retentionLeases())
                        .values(),
                    empty()
                );
            }
        } finally {
            for (final DiscoveryNode senderNode : followerClusterState.getState().nodes()) {
                final MockTransportService senderTransportService = (MockTransportService) getFollowerCluster().getInstance(
                    TransportService.class,
                    senderNode.getName()
                );
                senderTransportService.clearAllRules();
            }
        }
    }

    public void testForgetFollower() throws Exception {
        final String leaderIndex = "leader";
        final String followerIndex = "follower";
        final int numberOfShards = randomIntBetween(1, 4);
        final String leaderIndexSettings = getIndexSettings(numberOfShards, 0);
        assertAcked(leaderClient().admin().indices().prepareCreate(leaderIndex).setSource(leaderIndexSettings, XContentType.JSON).get());
        ensureLeaderYellow(leaderIndex);
        final PutFollowAction.Request followRequest = putFollow(leaderIndex, followerIndex);
        followerClient().execute(PutFollowAction.INSTANCE, followRequest).get();

        ensureFollowerGreen(true, followerIndex);

        pauseFollow(followerIndex);
        followerClient().admin().indices().close(new CloseIndexRequest(followerIndex).masterNodeTimeout(TimeValue.MAX_VALUE)).actionGet();

        final ClusterStateResponse followerIndexClusterState = followerClient().admin()
            .cluster()
            .prepareState(TEST_REQUEST_TIMEOUT)
            .clear()
            .setMetadata(true)
            .setIndices(followerIndex)
            .get();
        final String followerUUID = followerIndexClusterState.getState().metadata().getProject().index(followerIndex).getIndexUUID();

        final BroadcastResponse forgetFollowerResponse = leaderClient().execute(
            ForgetFollowerAction.INSTANCE,
            new ForgetFollowerAction.Request(
                getFollowerCluster().getClusterName(),
                followerIndex,
                followerUUID,
                "leader_cluster",
                leaderIndex
            )
        ).actionGet();

        logger.info(Strings.toString(forgetFollowerResponse));
        assertThat(forgetFollowerResponse.getTotalShards(), equalTo(numberOfShards));
        assertThat(forgetFollowerResponse.getSuccessfulShards(), equalTo(numberOfShards));
        assertThat(forgetFollowerResponse.getFailedShards(), equalTo(0));
        assertThat(forgetFollowerResponse.getShardFailures(), emptyArray());

        final IndicesStatsResponse afterForgetFollowerStats = leaderClient().admin()
            .indices()
            .stats(new IndicesStatsRequest().clear().indices(leaderIndex))
            .actionGet();
        final List<ShardStats> afterForgetFollowerShardsStats = getShardsStats(afterForgetFollowerStats);
        for (final ShardStats shardStats : afterForgetFollowerShardsStats) {
            assertNotNull(shardStats.getRetentionLeaseStats());
            assertThat(
                Strings.toString(shardStats),
                RetentionLeaseUtils.toMapExcludingPeerRecoveryRetentionLeases(shardStats.getRetentionLeaseStats().retentionLeases())
                    .values(),
                empty()
            );
        }
    }

    private void assertRetentionLeaseRenewal(
        final int numberOfShards,
        final int numberOfReplicas,
        final String followerIndex,
        final String leaderIndex
    ) throws Exception {
        // ensure that a retention lease has been put in place on each shard, and grab a copy of them
        final List<Map<String, RetentionLease>> retentionLeases = new ArrayList<>();
        assertBusy(() -> {
            retentionLeases.clear();
            final IndicesStatsResponse stats = leaderClient().admin()
                .indices()
                .stats(new IndicesStatsRequest().clear().indices(leaderIndex))
                .actionGet();
            assertNotNull(stats.getShards());
            assertThat(stats.getShards(), arrayWithSize(numberOfShards * (1 + numberOfReplicas)));
            final List<ShardStats> shardsStats = getShardsStats(stats);
            for (int i = 0; i < numberOfShards * (1 + numberOfReplicas); i++) {
                assertNotNull(shardsStats.get(i).getRetentionLeaseStats());
                final Map<String, RetentionLease> currentRetentionLeases = RetentionLeaseUtils.toMapExcludingPeerRecoveryRetentionLeases(
                    shardsStats.get(i).getRetentionLeaseStats().retentionLeases()
                );
                assertThat(Strings.toString(shardsStats.get(i)), currentRetentionLeases.values(), hasSize(1));
                final RetentionLease retentionLease = currentRetentionLeases.values().iterator().next();
                assertThat(retentionLease.id(), equalTo(getRetentionLeaseId(followerIndex, leaderIndex)));
                retentionLeases.add(currentRetentionLeases);
            }
        });

        // now ensure that the retention leases are being renewed
        assertBusy(() -> {
            final IndicesStatsResponse stats = leaderClient().admin()
                .indices()
                .stats(new IndicesStatsRequest().clear().indices(leaderIndex))
                .actionGet();
            assertNotNull(stats.getShards());
            assertThat(stats.getShards(), arrayWithSize(numberOfShards * (1 + numberOfReplicas)));
            final List<ShardStats> shardsStats = getShardsStats(stats);
            for (int i = 0; i < numberOfShards * (1 + numberOfReplicas); i++) {
                assertNotNull(shardsStats.get(i).getRetentionLeaseStats());
                final Map<String, RetentionLease> currentRetentionLeases = RetentionLeaseUtils.toMapExcludingPeerRecoveryRetentionLeases(
                    shardsStats.get(i).getRetentionLeaseStats().retentionLeases()
                );
                assertThat(Strings.toString(shardsStats.get(i)), currentRetentionLeases.values(), hasSize(1));
                final RetentionLease retentionLease = currentRetentionLeases.values().iterator().next();
                assertThat(retentionLease.id(), equalTo(getRetentionLeaseId(followerIndex, leaderIndex)));
                // we assert that retention leases are being renewed by an increase in the timestamp
                assertThat(retentionLease.timestamp(), greaterThan(retentionLeases.get(i).values().iterator().next().timestamp()));
            }
        });
    }

    /**
     * Extract the shard stats from an indices stats response, with the stats ordered by shard ID with primaries first. This is to have a
     * consistent ordering when comparing two responses.
     *
     * @param stats the indices stats
     * @return the shard stats in sorted order with (shard ID, primary) as the sort key
     */
    private List<ShardStats> getShardsStats(final IndicesStatsResponse stats) {
        return Arrays.stream(stats.getShards()).sorted((s, t) -> {
            if (s.getShardRouting().shardId().id() == t.getShardRouting().shardId().id()) {
                return -Boolean.compare(s.getShardRouting().primary(), t.getShardRouting().primary());
            } else {
                return Integer.compare(s.getShardRouting().shardId().id(), t.getShardRouting().shardId().id());
            }
        }).collect(Collectors.toList());
    }

    private String getRetentionLeaseId(final String followerIndex, final String leaderIndex) {
        final ClusterStateResponse followerIndexClusterState = followerClient().admin()
            .cluster()
            .prepareState(TEST_REQUEST_TIMEOUT)
            .clear()
            .setMetadata(true)
            .setIndices(followerIndex)
            .get();
        final String followerUUID = followerIndexClusterState.getState().metadata().getProject().index(followerIndex).getIndexUUID();

        final ClusterStateResponse leaderIndexClusterState = leaderClient().admin()
            .cluster()
            .prepareState(TEST_REQUEST_TIMEOUT)
            .clear()
            .setMetadata(true)
            .setIndices(leaderIndex)
            .get();
        final String leaderUUID = leaderIndexClusterState.getState().metadata().getProject().index(leaderIndex).getIndexUUID();

        return getRetentionLeaseId(followerIndex, followerUUID, leaderIndex, leaderUUID);
    }

    private String getRetentionLeaseId(String followerIndex, String followerUUID, String leaderIndex, String leaderUUID) {
        return retentionLeaseId(
            getFollowerCluster().getClusterName(),
            new Index(followerIndex, followerUUID),
            getLeaderCluster().getClusterName(),
            new Index(leaderIndex, leaderUUID)
        );
    }

    private void assertExpectedDocument(final String followerIndex, final int value) {
        final GetResponse getResponse = followerClient().prepareGet(followerIndex, Integer.toString(value)).get();
        assertTrue("doc with id [" + value + "] is missing", getResponse.isExists());
        if (sourceEnabled) {
            assertTrue((getResponse.getSource().containsKey("f")));
            assertThat(getResponse.getSource().get("f"), equalTo(value));
        }
    }

}
