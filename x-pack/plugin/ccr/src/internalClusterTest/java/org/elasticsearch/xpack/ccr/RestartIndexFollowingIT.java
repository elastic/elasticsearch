/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ccr;

import org.elasticsearch.exception.ElasticsearchException;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.admin.cluster.remote.RemoteInfoRequest;
import org.elasticsearch.action.admin.cluster.remote.TransportRemoteInfoAction;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Strings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.transport.RemoteConnectionInfo;
import org.elasticsearch.transport.RemoteConnectionStrategy;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.CcrIntegTestCase;
import org.elasticsearch.xpack.core.ccr.action.FollowStatsAction;
import org.elasticsearch.xpack.core.ccr.action.PauseFollowAction;
import org.elasticsearch.xpack.core.ccr.action.PutFollowAction;
import org.elasticsearch.xpack.core.ccr.action.ResumeFollowAction;
import org.elasticsearch.xpack.core.ccr.action.UnfollowAction;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;

public class RestartIndexFollowingIT extends CcrIntegTestCase {

    @Override
    protected int numberOfNodesPerCluster() {
        return 1;
    }

    @Override
    protected boolean configureRemoteClusterViaNodeSettings() {
        return false;
    }

    @Override
    protected Settings followerClusterSettings() {
        final Settings.Builder settings = Settings.builder().put(super.followerClusterSettings());
        if (randomBoolean()) {
            settings.put(RemoteConnectionStrategy.REMOTE_MAX_PENDING_CONNECTION_LISTENERS.getKey(), 1);
        }
        return settings.build();
    }

    public void testFollowIndex() throws Exception {
        final String leaderIndexSettings = getIndexSettings(randomIntBetween(1, 10), 0);
        assertAcked(leaderClient().admin().indices().prepareCreate("index1").setSource(leaderIndexSettings, XContentType.JSON));
        ensureLeaderGreen("index1");
        setupRemoteCluster();

        final PutFollowAction.Request followRequest = putFollow("index1", "index2");
        if (randomBoolean()) {
            followRequest.getParameters().setMaxReadRequestOperationCount(randomIntBetween(5, 10));
        }
        followerClient().execute(PutFollowAction.INSTANCE, followRequest).get();

        final long firstBatchNumDocs = randomIntBetween(10, 200);
        logger.info("Indexing [{}] docs as first batch", firstBatchNumDocs);
        for (int i = 0; i < firstBatchNumDocs; i++) {
            final String source = Strings.format("{\"f\":%d}", i);
            leaderClient().prepareIndex("index1").setId(Integer.toString(i)).setSource(source, XContentType.JSON).get();
        }

        assertBusy(() -> assertHitCount(followerClient().prepareSearch("index2"), firstBatchNumDocs));

        getFollowerCluster().fullRestart();
        ensureFollowerGreen("index2");

        final long secondBatchNumDocs = randomIntBetween(10, 200);
        logger.info("Indexing [{}] docs as second batch", secondBatchNumDocs);
        for (int i = 0; i < secondBatchNumDocs; i++) {
            leaderClient().prepareIndex("index1").setSource("{}", XContentType.JSON).get();
        }

        getLeaderCluster().fullRestart();
        ensureLeaderGreen("index1");

        final long thirdBatchNumDocs = randomIntBetween(10, 200);
        logger.info("Indexing [{}] docs as third batch", thirdBatchNumDocs);
        for (int i = 0; i < thirdBatchNumDocs; i++) {
            leaderClient().prepareIndex("index1").setSource("{}", XContentType.JSON).get();
        }

        var totalDocs = firstBatchNumDocs + secondBatchNumDocs + thirdBatchNumDocs;
        final AtomicBoolean resumeAfterDisconnectionOnce = new AtomicBoolean(false);
        assertBusy(() -> {
            if (resumeAfterDisconnectionOnce.get() == false && isFollowerStoppedBecauseOfRemoteClusterDisconnection("index2")) {
                assertTrue(resumeAfterDisconnectionOnce.compareAndSet(false, true));
                if (randomBoolean()) {
                    logger.info("shard follow task has been stopped because of remote cluster disconnection, resuming");
                    pauseFollow("index2");
                    assertAcked(followerClient().execute(ResumeFollowAction.INSTANCE, resumeFollow("index2")).actionGet());
                } else {
                    logger.info("shard follow task has been stopped because of remote cluster disconnection, recreating");
                    assertAcked(followerClient().admin().indices().prepareDelete("index2"));
                    followerClient().execute(PutFollowAction.INSTANCE, putFollow("index1", "index2", ActiveShardCount.ALL)).actionGet();
                }
            }
            assertHitCount(followerClient().prepareSearch("index2"), totalDocs);
        }, 30L, TimeUnit.SECONDS);

        cleanRemoteCluster();
        assertAcked(
            followerClient().execute(PauseFollowAction.INSTANCE, new PauseFollowAction.Request(TEST_REQUEST_TIMEOUT, "index2")).actionGet()
        );
        assertAcked(followerClient().admin().indices().prepareClose("index2"));

        final ActionFuture<AcknowledgedResponse> unfollowFuture = followerClient().execute(
            UnfollowAction.INSTANCE,
            new UnfollowAction.Request(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, "index2")
        );
        final ElasticsearchException elasticsearchException = expectThrows(ElasticsearchException.class, unfollowFuture::actionGet);
        assertThat(elasticsearchException.getMessage(), containsString("no such remote cluster"));
        assertThat(elasticsearchException.getMetadataKeys(), hasItem("es.failed_to_remove_retention_leases"));
    }

    private void setupRemoteCluster() throws Exception {
        var remoteMaxPendingConnectionListeners = getRemoteMaxPendingConnectionListeners();

        ClusterUpdateSettingsRequest updateSettingsRequest = new ClusterUpdateSettingsRequest(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT)
            .masterNodeTimeout(TimeValue.MAX_VALUE);
        String address = getLeaderCluster().getAnyMasterNodeInstance(TransportService.class).boundAddress().publishAddress().toString();
        updateSettingsRequest.persistentSettings(Settings.builder().put("cluster.remote.leader_cluster.seeds", address));
        assertAcked(followerClient().admin().cluster().updateSettings(updateSettingsRequest).actionGet());

        if (remoteMaxPendingConnectionListeners == 1) {
            // if queue is limited then update settings might fail to add a listener to wait for a connection
            // however remote should be connected eventually by a concurrent task
            assertBusy(this::isRemoteConnected);
        } else {
            assertTrue(isRemoteConnected());
        }
    }

    private boolean isRemoteConnected() throws Exception {
        var infos = followerClient().execute(TransportRemoteInfoAction.TYPE, new RemoteInfoRequest()).get().getInfos();
        return infos.size() == 1 && infos.get(0).isConnected();
    }

    private Integer getRemoteMaxPendingConnectionListeners() {
        var response = followerClient().admin().cluster().prepareNodesInfo("_local").clear().setSettings(true).get();
        var settings = response.getNodes().get(0).getSettings();
        return RemoteConnectionStrategy.REMOTE_MAX_PENDING_CONNECTION_LISTENERS.get(settings);
    }

    private void cleanRemoteCluster() throws Exception {
        ClusterUpdateSettingsRequest updateSettingsRequest = new ClusterUpdateSettingsRequest(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT)
            .masterNodeTimeout(TimeValue.MAX_VALUE);
        updateSettingsRequest.persistentSettings(Settings.builder().put("cluster.remote.leader_cluster.seeds", (String) null));
        assertAcked(followerClient().admin().cluster().updateSettings(updateSettingsRequest).actionGet());

        assertBusy(() -> {
            List<RemoteConnectionInfo> infos = followerClient().execute(TransportRemoteInfoAction.TYPE, new RemoteInfoRequest())
                .get()
                .getInfos();
            assertThat(infos.size(), equalTo(0));
        });
    }

    private boolean isFollowerStoppedBecauseOfRemoteClusterDisconnection(String indexName) {
        var request = new FollowStatsAction.StatsRequest();
        request.setIndices(new String[] { indexName });
        var response = followerClient().execute(FollowStatsAction.INSTANCE, request).actionGet();
        return response.getStatsResponses().stream().map(r -> r.status().getFatalException()).filter(Objects::nonNull).anyMatch(e -> {
            if (e.getCause() instanceof IllegalStateException ise) {
                return ise.getMessage().contains("Unable to open any connections to remote cluster");
            }
            return false;
        });
    }
}
