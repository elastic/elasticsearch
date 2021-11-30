/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ccr;

import org.elasticsearch.action.admin.cluster.repositories.put.PutRepositoryRequest;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotRequest;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotRequest;
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.InternalTestCluster;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.CcrIntegTestCase;
import org.elasticsearch.xpack.core.ccr.AutoFollowMetadata;
import org.elasticsearch.xpack.core.ccr.action.DeleteAutoFollowPatternAction;
import org.elasticsearch.xpack.core.ccr.action.GetAutoFollowPatternAction;
import org.elasticsearch.xpack.core.ccr.action.PutAutoFollowPatternAction;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class AutoFollowRestoreGlobalStateIT extends CcrIntegTestCase {

    private static final TimeValue TIMEOUT = TimeValue.MAX_VALUE;
    private static final String REPO_NAME = "test-repository";

    @Override
    protected boolean configureRemoteClusterViaNodeSettings() {
        return false;
    }

    public void testRestorePreviousAutoFollowConfigurationFromSnapshot() {

        var snapshotName = randomName();
        createRepository(followerClient());

        // create remote and auto-follower
        var remote = getAddress(getLeaderCluster());
        updateClusterSettings(followerClient(), Settings.builder().put("cluster.remote.leader_cluster.seeds", remote));
        createAutoFollower(followerClient(), "pattern-1");

        assertThat(getClusterSettings(followerClient()).get("cluster.remote.leader_cluster.seeds"), equalTo(remote));
        assertThat(getAutoFollowerPatterns(followerClient()).keySet(), equalTo(Set.of("pattern-1")));

        // take a snapshot with global state with above configuration
        takeSnapshot(followerClient(), snapshotName);

        // update auto-followers
        deleteAutoFollower(followerClient(), "pattern-1");
        createAutoFollower(followerClient(), "pattern-2");

        assertThat(getAutoFollowerPatterns(followerClient()).keySet(), equalTo(Set.of("pattern-2")));

        // restore previous auto-follower configuration
        restoreSnapshot(followerClient(), snapshotName);

        assertThat(getAutoFollowerPatterns(followerClient()).keySet(), equalTo(Set.of("pattern-1")));
    }

    public void testRestoreDeletedAutoFollowConfigurationFromSnapshot() {

        var snapshotName = randomName();
        createRepository(followerClient());

        // create remote and auto-follower
        var remote = getAddress(getLeaderCluster());
        updateClusterSettings(followerClient(), Settings.builder().put("cluster.remote.leader_cluster.seeds", remote));
        createAutoFollower(followerClient(), "pattern-1");

        assertThat(getClusterSettings(followerClient()).get("cluster.remote.leader_cluster.seeds"), equalTo(remote));
        assertThat(getAutoFollowerPatterns(followerClient()).keySet(), equalTo(Set.of("pattern-1")));

        // take a snapshot with global state with above configuration
        takeSnapshot(followerClient(), snapshotName);

        // delete auto-followers
        deleteAutoFollower(followerClient(), "pattern-1");
        updateClusterSettings(followerClient(), Settings.builder().putNull("cluster.remote.leader_cluster.seeds"));

        assertThat(getClusterSettings(followerClient()).get("cluster.remote.leader_cluster.seeds"), nullValue());
        assertThat(getAutoFollowerPatterns(followerClient()).keySet(), equalTo(Set.of()));

        // restore previous auto-follower configuration
        restoreSnapshot(followerClient(), snapshotName);

        assertThat(getClusterSettings(followerClient()).get("cluster.remote.leader_cluster.seeds"), equalTo(remote));
        assertThat(getAutoFollowerPatterns(followerClient()).keySet(), equalTo(Set.of("pattern-1")));
    }

    public void testRestoreDeletesAutoFollowConfigurationIfNotPresentInSnapshot() {

        var snapshotName = randomName();
        createRepository(followerClient());

        // take a snapshot with global state without auto-followers
        updateClusterSettings(followerClient(), Settings.builder().putNull("cluster.remote.leader_cluster.seeds"));

        takeSnapshot(followerClient(), snapshotName);

        assertThat(getClusterSettings(followerClient()).get("cluster.remote.leader_cluster.seeds"), nullValue());
        assertThat(getAutoFollowerPatterns(followerClient()).keySet(), equalTo(Set.of()));

        // create remote and auto-follower
        var remote = getAddress(getLeaderCluster());
        updateClusterSettings(followerClient(), Settings.builder().put("cluster.remote.leader_cluster.seeds", remote));
        createAutoFollower(followerClient(), "pattern-1");

        assertThat(getClusterSettings(followerClient()).get("cluster.remote.leader_cluster.seeds"), equalTo(remote));
        assertThat(getAutoFollowerPatterns(followerClient()).keySet(), equalTo(Set.of("pattern-1")));

        // restore snapshot without remote and auto-follower
        restoreSnapshot(followerClient(), snapshotName);

        assertThat(getClusterSettings(followerClient()).get("cluster.remote.leader_cluster.seeds"), nullValue());
        assertThat(getAutoFollowerPatterns(followerClient()).keySet(), equalTo(Set.of()));
    }

    private void createRepository(Client client) {
        client.admin()
            .cluster()
            .putRepository(
                new PutRepositoryRequest(REPO_NAME).type("fs")
                    .settings(Settings.builder().put("location", UUIDs.randomBase64UUID()))
                    .masterNodeTimeout(TIMEOUT)
            )
            .actionGet();
    }

    private void takeSnapshot(Client client, String snapshotName) {
        client.admin()
            .cluster()
            .createSnapshot(
                new CreateSnapshotRequest(REPO_NAME, snapshotName).includeGlobalState(true)
                    .waitForCompletion(true)
                    .masterNodeTimeout(TIMEOUT)
            )
            .actionGet();
    }

    private void restoreSnapshot(Client client, String snapshotName) {
        client.admin()
            .cluster()
            .restoreSnapshot(
                new RestoreSnapshotRequest(REPO_NAME, snapshotName).includeGlobalState(true)
                    .waitForCompletion(true)
                    .masterNodeTimeout(TIMEOUT)
            )
            .actionGet();
    }

    private void updateClusterSettings(Client client, Settings.Builder settings) {
        client.admin()
            .cluster()
            .updateSettings(new ClusterUpdateSettingsRequest().persistentSettings(settings).masterNodeTimeout(TIMEOUT))
            .actionGet();
    }

    private Settings getClusterSettings(Client client) {
        var response = client.admin().cluster().state(new ClusterStateRequest().masterNodeTimeout(TIMEOUT)).actionGet();
        return response.getState().metadata().persistentSettings();
    }

    private void createAutoFollower(Client client, String pattern) {
        var putAutoFollowerRequest = new PutAutoFollowPatternAction.Request();
        putAutoFollowerRequest.setName(pattern);
        putAutoFollowerRequest.setRemoteCluster("leader_cluster");
        putAutoFollowerRequest.setLeaderIndexPatterns(List.of("logs-*"));
        putAutoFollowerRequest.setLeaderIndexExclusionPatterns(List.of());
        putAutoFollowerRequest.setFollowIndexNamePattern("copy-{{leader_index}}");
        assertTrue(client.execute(PutAutoFollowPatternAction.INSTANCE, putAutoFollowerRequest).actionGet().isAcknowledged());
    }

    private void deleteAutoFollower(Client client, String pattern) {
        var deleteAutoFollowerRequest = new DeleteAutoFollowPatternAction.Request(pattern);
        assertTrue(client.execute(DeleteAutoFollowPatternAction.INSTANCE, deleteAutoFollowerRequest).actionGet().isAcknowledged());
    }

    private Map<String, AutoFollowMetadata.AutoFollowPattern> getAutoFollowerPatterns(Client client) {
        var response = client.execute(GetAutoFollowPatternAction.INSTANCE, new GetAutoFollowPatternAction.Request()).actionGet();
        return response.getAutoFollowPatterns();
    }

    private String getAddress(InternalTestCluster cluster) {
        return cluster.getMasterNodeInstance(TransportService.class).boundAddress().publishAddress().toString();
    }

    private static String randomName() {
        return randomAlphaOfLengthBetween(5, 25).toLowerCase();
    }
}
