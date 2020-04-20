/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ccr;

import org.elasticsearch.action.admin.cluster.remote.RemoteInfoAction;
import org.elasticsearch.action.admin.cluster.remote.RemoteInfoRequest;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.transport.RemoteConnectionInfo;
import org.elasticsearch.transport.RemoteConnectionStrategy;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.CcrIntegTestCase;
import org.elasticsearch.xpack.core.ccr.action.PutFollowAction;

import java.util.List;
import java.util.Locale;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;

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
            final String source = String.format(Locale.ROOT, "{\"f\":%d}", i);
            leaderClient().prepareIndex("index1").setId(Integer.toString(i)).setSource(source, XContentType.JSON).get();
        }

        assertBusy(() -> {
            assertThat(followerClient().prepareSearch("index2").get().getHits().getTotalHits().value, equalTo(firstBatchNumDocs));
        });

        getFollowerCluster().fullRestart();
        ensureFollowerGreen("index2");

        final long secondBatchNumDocs = randomIntBetween(10, 200);
        for (int i = 0; i < secondBatchNumDocs; i++) {
            leaderClient().prepareIndex("index1").setSource("{}", XContentType.JSON).get();
        }

        cleanRemoteCluster();
        getLeaderCluster().fullRestart();
        ensureLeaderGreen("index1");
        // Remote connection needs to be re-configured, because all the nodes in leader cluster have been restarted:
        setupRemoteCluster();

        final long thirdBatchNumDocs = randomIntBetween(10, 200);
        for (int i = 0; i < thirdBatchNumDocs; i++) {
            leaderClient().prepareIndex("index1").setSource("{}", XContentType.JSON).get();
        }

        assertBusy(() -> {
            assertThat(followerClient().prepareSearch("index2").get().getHits().getTotalHits().value,
                equalTo(firstBatchNumDocs + secondBatchNumDocs + thirdBatchNumDocs));
        });
    }

    private void setupRemoteCluster() throws Exception {
        ClusterUpdateSettingsRequest updateSettingsRequest = new ClusterUpdateSettingsRequest();
        String address = getLeaderCluster().getMasterNodeInstance(TransportService.class).boundAddress().publishAddress().toString();
        updateSettingsRequest.persistentSettings(Settings.builder().put("cluster.remote.leader_cluster.seeds", address));
        assertAcked(followerClient().admin().cluster().updateSettings(updateSettingsRequest).actionGet());
        List<RemoteConnectionInfo> infos =
            followerClient().execute(RemoteInfoAction.INSTANCE, new RemoteInfoRequest()).get().getInfos();
        assertThat(infos.size(), equalTo(1));
        assertTrue(infos.get(0).isConnected());
    }

    private void cleanRemoteCluster() throws Exception {
        ClusterUpdateSettingsRequest updateSettingsRequest = new ClusterUpdateSettingsRequest();
        updateSettingsRequest.persistentSettings(Settings.builder().put("cluster.remote.leader_cluster.seeds", (String) null));
        assertAcked(followerClient().admin().cluster().updateSettings(updateSettingsRequest).actionGet());

        assertBusy(() -> {
            List<RemoteConnectionInfo> infos =
                followerClient().execute(RemoteInfoAction.INSTANCE, new RemoteInfoRequest()).get().getInfos();
            assertThat(infos.size(), equalTo(0));
        });
    }

}
