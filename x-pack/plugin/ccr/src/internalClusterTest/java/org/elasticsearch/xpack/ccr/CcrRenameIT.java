/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ccr;

import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.rename.RenameIndexAction;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.CcrIntegTestCase;
import org.elasticsearch.xpack.core.ccr.AutoFollowMetadata;
import org.elasticsearch.xpack.core.ccr.action.FollowStatsAction;
import org.elasticsearch.xpack.core.ccr.action.PauseFollowAction;
import org.elasticsearch.xpack.core.ccr.action.PutAutoFollowPatternAction;
import org.elasticsearch.xpack.core.ccr.action.PutFollowAction;
import org.elasticsearch.xpack.core.ccr.action.ResumeFollowAction;
import org.elasticsearch.xpack.core.ccr.action.UnfollowAction;

import java.util.List;
import java.util.Map;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.nullValue;

/**
 * Renaming a leader or a follower index keeps its UUID, which is what cross-cluster replication identifies indices by, so
 * replication must carry on across a rename on either side, and the follower must stay manageable under its new name.
 */
public class CcrRenameIT extends CcrIntegTestCase {

    public void testRenameLeaderWhileFollowing() throws Exception {
        final String leaderIndexSettings = getIndexSettings(1, 0);
        assertAcked(leaderClient().admin().indices().prepareCreate("leader").setSource(leaderIndexSettings, XContentType.JSON));
        ensureLeaderYellow("leader");
        final PutFollowAction.Request followRequest = putFollow("leader", "follower");
        followerClient().execute(PutFollowAction.INSTANCE, followRequest).get();
        ensureFollowerGreen("follower");

        final int firstBatch = randomIntBetween(1, 16);
        for (int i = 0; i < firstBatch; i++) {
            leaderClient().prepareIndex("leader").setSource("{\"f\": " + i + "}", XContentType.JSON).get();
        }
        assertIndexFullyReplicatedToFollower("leader", "follower");

        rename(leaderClient(), "leader", "leader-renamed");

        // writes, a mapping change, a settings change and an alias change after the rename must all reach the follower
        final int secondBatch = randomIntBetween(1, 16);
        for (int i = 0; i < secondBatch; i++) {
            leaderClient().prepareIndex("leader-renamed").setSource("{\"f\": " + i + ", \"g\": \"new-field\"}", XContentType.JSON).get();
        }
        assertAcked(
            leaderClient().admin()
                .indices()
                .updateSettings(
                    new UpdateSettingsRequest("leader-renamed").settings(Settings.builder().put("index.max_result_window", 12345))
                )
                .get()
        );
        assertAcked(
            leaderClient().admin()
                .indices()
                .aliases(
                    new IndicesAliasesRequest(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT).addAliasAction(
                        IndicesAliasesRequest.AliasActions.add().index("leader-renamed").alias("leader-alias")
                    )
                )
                .get()
        );

        assertIndexFullyReplicatedToFollower("leader-renamed", "follower");
        assertBusy(() -> {
            final var followerSettings = followerClient().admin().indices().prepareGetSettings(TEST_REQUEST_TIMEOUT, "follower").get();
            assertThat(followerSettings.getSetting("follower", "index.max_result_window"), equalTo("12345"));
            final var followerMappings = followerClient().admin().indices().prepareGetMappings(TEST_REQUEST_TIMEOUT, "follower").get();
            final Map<?, ?> properties = (Map<?, ?>) followerMappings.mappings().get("follower").getSourceAsMap().get("properties");
            assertTrue("mapping update for field [g] was not replicated: " + properties, properties.containsKey("g"));
            final var followerAliases = followerClient().admin().indices().prepareGetAliases(TEST_REQUEST_TIMEOUT, "leader-alias").get();
            assertThat(followerAliases.getAliases().get("follower"), hasSize(1));
        });
        assertNoFatalFollowError();

        // the follower is still manageable
        pauseFollow("follower");
    }

    public void testRenameFollowerWhileFollowing() throws Exception {
        final String leaderIndexSettings = getIndexSettings(1, 0);
        assertAcked(leaderClient().admin().indices().prepareCreate("leader").setSource(leaderIndexSettings, XContentType.JSON));
        ensureLeaderYellow("leader");
        followerClient().execute(PutFollowAction.INSTANCE, putFollow("leader", "follower")).get();
        ensureFollowerGreen("follower");
        leaderClient().prepareIndex("leader").setSource("{\"f\": 0}", XContentType.JSON).get();
        assertIndexFullyReplicatedToFollower("leader", "follower");

        rename(followerClient(), "follower", "follower-renamed");

        final int docs = randomIntBetween(1, 16);
        for (int i = 1; i <= docs; i++) {
            leaderClient().prepareIndex("leader").setSource("{\"f\": " + i + "}", XContentType.JSON).get();
        }
        assertIndexFullyReplicatedToFollower("leader", "follower-renamed");
        assertNoFatalFollowError();

        // stats, pause, resume and unfollow all address the follower by its new name
        final FollowStatsAction.StatsResponses stats = followerClient().execute(
            FollowStatsAction.INSTANCE,
            new FollowStatsAction.StatsRequest()
        ).get();
        assertThat(stats.getStatsResponses(), hasSize(1));
        assertThat(stats.getStatsResponses().get(0).status().followerIndex(), equalTo("follower-renamed"));

        assertAcked(
            followerClient().execute(PauseFollowAction.INSTANCE, new PauseFollowAction.Request(TEST_REQUEST_TIMEOUT, "follower-renamed"))
                .get()
        );
        final ResumeFollowAction.Request resumeRequest = new ResumeFollowAction.Request(TEST_REQUEST_TIMEOUT);
        resumeRequest.setFollowerIndex("follower-renamed");
        assertAcked(followerClient().execute(ResumeFollowAction.INSTANCE, resumeRequest).get());
        leaderClient().prepareIndex("leader")
            .setSource("{\"f\": -1}", XContentType.JSON)
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .get();
        assertIndexFullyReplicatedToFollower("leader", "follower-renamed");

        assertAcked(
            followerClient().execute(PauseFollowAction.INSTANCE, new PauseFollowAction.Request(TEST_REQUEST_TIMEOUT, "follower-renamed"))
                .get()
        );
        assertAcked(followerClient().admin().indices().prepareClose("follower-renamed").get());
        assertAcked(
            followerClient().execute(UnfollowAction.INSTANCE, new UnfollowAction.Request(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, "follower-renamed")).get()
        );
        assertAcked(followerClient().admin().indices().prepareOpen("follower-renamed").get());
        ensureFollowerGreen("follower-renamed");
        followerClient().prepareIndex("follower-renamed").setSource("{\"f\": 100}", XContentType.JSON).get();
    }

    /**
     * Auto-follow records followed leader indices by UUID, so a renamed leader that still matches the pattern must not be
     * followed a second time.
     */
    public void testAutoFollowDoesNotFollowRenamedLeaderAgain() throws Exception {
        final PutAutoFollowPatternAction.Request request = new PutAutoFollowPatternAction.Request(
            TEST_REQUEST_TIMEOUT,
            TEST_REQUEST_TIMEOUT
        );
        request.setName("rename-pattern");
        request.setRemoteCluster("leader_cluster");
        request.setLeaderIndexPatterns(List.of("logs-*"));
        request.setFollowIndexNamePattern("copy-{{leader_index}}");
        assertAcked(followerClient().execute(PutAutoFollowPatternAction.INSTANCE, request).get());

        final String leaderIndexSettings = getIndexSettings(1, 0);
        assertAcked(leaderClient().admin().indices().prepareCreate("logs-1").setSource(leaderIndexSettings, XContentType.JSON));
        assertBusy(() -> assertTrue(followerIndexExists("copy-logs-1")));

        rename(leaderClient(), "logs-1", "logs-2");

        // a fresh index proves the coordinator keeps running after the rename
        assertAcked(leaderClient().admin().indices().prepareCreate("logs-3").setSource(leaderIndexSettings, XContentType.JSON));
        assertBusy(() -> assertTrue(followerIndexExists("copy-logs-3")));
        assertFalse(followerIndexExists("copy-logs-2"));
        final AutoFollowMetadata autoFollowMetadata = followerClient().admin()
            .cluster()
            .prepareState(TEST_REQUEST_TIMEOUT)
            .get()
            .getState()
            .metadata()
            .getProject()
            .custom(AutoFollowMetadata.TYPE);
        assertThat(autoFollowMetadata.getFollowedLeaderIndexUUIDs().get("rename-pattern"), hasSize(2));

        // and the existing follower of the renamed leader keeps receiving writes
        leaderClient().prepareIndex("logs-2").setSource("{\"f\": 1}", XContentType.JSON).get();
        assertIndexFullyReplicatedToFollower("logs-2", "copy-logs-1");
        assertNoFatalFollowError();
    }

    private static void rename(org.elasticsearch.client.internal.Client client, String source, String target) {
        final var response = client.execute(
            RenameIndexAction.INSTANCE,
            new RenameIndexAction.Request(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, source, target)
        ).actionGet(TimeValue.timeValueSeconds(30));
        assertTrue(response.isAcknowledged());
    }

    private boolean followerIndexExists(String index) {
        final Metadata metadata = followerClient().admin().cluster().prepareState(TEST_REQUEST_TIMEOUT).get().getState().metadata();
        return metadata.getProject().hasIndex(index);
    }

    private void assertNoFatalFollowError() throws Exception {
        final FollowStatsAction.StatsResponses stats = followerClient().execute(
            FollowStatsAction.INSTANCE,
            new FollowStatsAction.StatsRequest()
        ).get();
        for (FollowStatsAction.StatsResponse statsResponse : stats.getStatsResponses()) {
            assertThat(
                statsResponse.status().followerIndex() + " has a fatal error",
                statsResponse.status().getFatalException(),
                nullValue()
            );
        }
    }
}
