/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ccr;

import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.alias.TransportIndicesAliasesAction;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.junit.annotations.TestLogging;
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
import java.util.Set;
import java.util.stream.Collectors;

import static org.elasticsearch.action.support.WriteRequest.RefreshPolicy.IMMEDIATE;
import static org.elasticsearch.test.ESIntegTestCase.renameIndexRequest;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.nullValue;

/**
 * Renaming a leader or a follower index keeps its UUID, which is what cross-cluster replication identifies indices by, so
 * replication must carry on across a rename on either side, and the follower must stay manageable under its new name.
 */
@TestLogging(value = "org.elasticsearch.xpack.ccr:DEBUG", reason = "follow task activity around the rename")
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
                    new UpdateSettingsRequest("leader-renamed").settings(Settings.builder().put("index.mapping.total_fields.limit", 1234))
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
            assertThat(followerSettings.getSetting("follower", "index.mapping.total_fields.limit"), equalTo("1234")); // a replicated
                                                                                                                      // setting;
                                                                                                                      // max_result_window
                                                                                                                      // is deliberately not
                                                                                                                      // replicated
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
            followerClient().execute(
                UnfollowAction.INSTANCE,
                new UnfollowAction.Request(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, "follower-renamed")
            ).get()
        );
        assertAcked(followerClient().admin().indices().prepareOpen("follower-renamed").get());
        ensureFollowerGreen("follower-renamed");
        followerClient().prepareIndex("follower-renamed").setSource("{\"f\": 100}", XContentType.JSON).get();
    }

    /**
     * The follow task applies the leader's mapping, dynamic settings and alias changes to the follower with requests addressed by
     * name, so after a rename they must address the follower's current name, found by UUID. An unrelated index that has since been
     * given the follower's old name must be left alone.
     */
    public void testLeaderMetadataChangesReachRenamedFollower() throws Exception {
        assertAcked(leaderClient().admin().indices().prepareCreate("leader").setSource(getIndexSettings(1, 0), XContentType.JSON));
        ensureLeaderYellow("leader");
        followerClient().execute(PutFollowAction.INSTANCE, putFollow("leader", "follower")).get();
        ensureFollowerGreen("follower");
        leaderClient().prepareIndex("leader").setSource("{\"f\": 0}", XContentType.JSON).get();
        assertIndexFullyReplicatedToFollower("leader", "follower");

        rename(followerClient(), "follower", "follower-renamed");
        createUnrelatedIndexNamedLikeTheOldFollower();

        // a new field (mapping), a dynamic setting and an alias on the leader, each applied by a request addressed by name
        leaderClient().prepareIndex("leader").setSource("{\"f\": 1, \"g\": \"new-field\"}", XContentType.JSON).get();
        assertAcked(
            leaderClient().admin()
                .indices()
                .updateSettings(
                    new UpdateSettingsRequest("leader").settings(Settings.builder().put("index.mapping.total_fields.limit", 1234))
                )
                .get()
        );
        assertAcked(
            leaderClient().admin()
                .indices()
                .aliases(
                    new IndicesAliasesRequest(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT).addAliasAction(
                        IndicesAliasesRequest.AliasActions.add().index("leader").alias("leader-alias")
                    )
                )
                .get()
        );

        assertBusy(() -> {
            final var settings = followerClient().admin().indices().prepareGetSettings(TEST_REQUEST_TIMEOUT, "follower-renamed").get();
            assertThat(settings.getSetting("follower-renamed", "index.mapping.total_fields.limit"), equalTo("1234"));
            assertThat(fieldNamesOf("follower-renamed"), hasItems("f", "g"));
            final var aliases = followerClient().admin().indices().prepareGetAliases(TEST_REQUEST_TIMEOUT, "leader-alias").get();
            assertThat(aliases.getAliases().get("follower-renamed"), hasSize(1));
        });
        assertIndexFullyReplicatedToFollower("leader", "follower-renamed");
        assertNoFatalFollowError();

        assertUnrelatedIndexUntouched();
        pauseFollow("follower-renamed");
    }

    /**
     * Non-dynamic leader settings are applied by closing the follower, updating its settings and opening it again, three requests
     * addressed by name. After a rename all three must address the follower's current name, so an unrelated index with the old name
     * is never closed, changed or reopened.
     */
    public void testNonDynamicSettingsReachRenamedFollower() throws Exception {
        assertAcked(leaderClient().admin().indices().prepareCreate("leader").setSource(getIndexSettings(1, 0), XContentType.JSON));
        ensureLeaderYellow("leader");
        followerClient().execute(PutFollowAction.INSTANCE, putFollow("leader", "follower")).get();
        ensureFollowerGreen("follower");
        leaderClient().prepareIndex("leader").setSource("{\"f\": 0}", XContentType.JSON).get();
        assertIndexFullyReplicatedToFollower("leader", "follower");

        rename(followerClient(), "follower", "follower-renamed");
        createUnrelatedIndexNamedLikeTheOldFollower();

        assertAcked(leaderClient().admin().indices().prepareClose("leader").get());
        assertAcked(
            leaderClient().admin()
                .indices()
                .updateSettings(
                    new UpdateSettingsRequest("leader").settings(
                        Settings.builder()
                            .put("index.analysis.analyzer.my_analyzer.type", "custom")
                            .put("index.analysis.analyzer.my_analyzer.tokenizer", "keyword")
                    )
                )
                .get()
        );
        assertAcked(leaderClient().admin().indices().prepareOpen("leader").get());
        ensureLeaderGreen("leader");
        leaderClient().prepareIndex("leader").setSource("{\"f\": 1}", XContentType.JSON).get();

        assertBusy(() -> {
            final var settings = followerClient().admin().indices().prepareGetSettings(TEST_REQUEST_TIMEOUT, "follower-renamed").get();
            assertThat(settings.getSetting("follower-renamed", "index.analysis.analyzer.my_analyzer.type"), equalTo("custom"));
            assertThat(settings.getSetting("follower-renamed", "index.analysis.analyzer.my_analyzer.tokenizer"), equalTo("keyword"));
            assertThat(followerIndexState("follower-renamed"), equalTo(IndexMetadata.State.OPEN));
        });
        ensureFollowerGreen("follower-renamed");
        assertIndexFullyReplicatedToFollower("leader", "follower-renamed");
        assertNoFatalFollowError();

        assertUnrelatedIndexUntouched();
        pauseFollow("follower-renamed");
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
        // let the follower finish bootstrapping from the leader before renaming it
        ensureFollowerGreen("copy-logs-1");
        leaderClient().prepareIndex("logs-1").setSource("{\"f\": 0}", XContentType.JSON).get();
        assertIndexFullyReplicatedToFollower("logs-1", "copy-logs-1");

        rename(leaderClient(), "logs-1", "logs-2");

        // a fresh index proves the coordinator keeps running after the rename
        assertAcked(leaderClient().admin().indices().prepareCreate("logs-3").setSource(leaderIndexSettings, XContentType.JSON));
        assertBusy(() -> assertTrue(followerIndexExists("copy-logs-3")));
        // the followed uuid is recorded once the follow request completes, which is after the follower index appears
        assertBusy(() -> {
            final AutoFollowMetadata autoFollowMetadata = getFollowerCluster().clusterService()
                .state()
                .metadata()
                .getProject()
                .custom(AutoFollowMetadata.TYPE);
            assertThat(autoFollowMetadata.getFollowedLeaderIndexUUIDs().get("rename-pattern"), hasSize(2));
        });
        assertFalse(followerIndexExists("copy-logs-2"));

        // and the existing follower of the renamed leader keeps receiving writes
        leaderClient().prepareIndex("logs-2").setSource("{\"f\": 1}", XContentType.JSON).get();
        assertIndexFullyReplicatedToFollower("logs-2", "copy-logs-1");
        assertNoFatalFollowError();
    }

    private static void rename(org.elasticsearch.client.internal.Client client, String source, String target) {
        final var response = client.execute(TransportIndicesAliasesAction.TYPE, renameIndexRequest(source, target))
            .actionGet(TimeValue.timeValueSeconds(30));
        assertTrue(response.isAcknowledged());
    }

    private void createUnrelatedIndexNamedLikeTheOldFollower() throws Exception {
        assertAcked(
            followerClient().admin()
                .indices()
                .prepareCreate("follower")
                .setSettings(Settings.builder().put("index.number_of_shards", 1).put("index.number_of_replicas", 0))
        );
        followerClient().prepareIndex("follower").setSource("{\"u\": 1}", XContentType.JSON).setRefreshPolicy(IMMEDIATE).get();
        ensureFollowerGreen("follower");
    }

    /** The index that took the follower's old name has neither the leader's mapping, settings or alias, and was never closed. */
    private void assertUnrelatedIndexUntouched() throws Exception {
        assertThat(followerIndexState("follower"), equalTo(IndexMetadata.State.OPEN));
        assertThat(fieldNamesOf("follower"), equalTo(Set.of("u")));
        final var settings = followerClient().admin().indices().prepareGetSettings(TEST_REQUEST_TIMEOUT, "follower").get();
        assertThat(settings.getSetting("follower", "index.mapping.total_fields.limit"), nullValue());
        assertThat(settings.getSetting("follower", "index.analysis.analyzer.my_analyzer.type"), nullValue());
        final var aliases = followerClient().admin().indices().prepareGetAliases(TEST_REQUEST_TIMEOUT, "leader-alias").get();
        assertThat(aliases.getAliases().get("follower"), nullValue());
    }

    private Set<String> fieldNamesOf(String followerIndex) {
        final var mappings = followerClient().admin().indices().prepareGetMappings(TEST_REQUEST_TIMEOUT, followerIndex).get();
        final Map<?, ?> properties = (Map<?, ?>) mappings.mappings().get(followerIndex).getSourceAsMap().get("properties");
        return properties == null ? Set.of() : properties.keySet().stream().map(String::valueOf).collect(Collectors.toSet());
    }

    private IndexMetadata.State followerIndexState(String index) {
        final Metadata metadata = followerClient().admin().cluster().prepareState(TEST_REQUEST_TIMEOUT).get().getState().metadata();
        return metadata.getProject().index(index).getState();
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
