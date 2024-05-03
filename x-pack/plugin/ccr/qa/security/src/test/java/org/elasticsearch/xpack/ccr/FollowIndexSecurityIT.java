/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ccr;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.WarningsHandler;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.core.CheckedRunnable;
import org.elasticsearch.core.Strings;
import org.elasticsearch.health.node.selection.HealthNode;
import org.elasticsearch.index.seqno.ReplicationTracker;
import org.elasticsearch.test.rest.ObjectPath;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

public class FollowIndexSecurityIT extends ESCCRRestTestCase {

    @Override
    protected Settings restClientSettings() {
        String token = basicAuthHeaderValue("test_ccr", new SecureString("x-pack-test-password".toCharArray()));
        return Settings.builder().put(ThreadContext.PREFIX + ".Authorization", token).build();
    }

    @Override
    protected Settings restAdminSettings() {
        String token = basicAuthHeaderValue("test_admin", new SecureString("x-pack-test-password".toCharArray()));
        return Settings.builder().put(ThreadContext.PREFIX + ".Authorization", token).build();
    }

    public void testFollowIndex() throws Exception {
        final int numDocs = 16;
        final String allowedIndex = "allowed-index";
        final String unallowedIndex = "unallowed-index";
        if ("leader".equals(targetCluster)) {
            logger.info("Running against leader cluster");
            createIndex(adminClient(), allowedIndex, Settings.EMPTY);
            createIndex(adminClient(), unallowedIndex, Settings.EMPTY);
            for (int i = 0; i < numDocs; i++) {
                logger.info("Indexing doc [{}]", i);
                index(allowedIndex, Integer.toString(i), "field", i);
            }
            for (int i = 0; i < numDocs; i++) {
                logger.info("Indexing doc [{}]", i);
                index(unallowedIndex, Integer.toString(i), "field", i);
            }
            refresh(adminClient(), allowedIndex);
            verifyDocuments(allowedIndex, numDocs, "*:*");
        } else {
            followIndex("leader_cluster", allowedIndex, allowedIndex);
            assertBusy(() -> verifyDocuments(allowedIndex, numDocs, "*:*"));
            assertThat(getCcrNodeTasks(), contains(new CcrNodeTask("leader_cluster", allowedIndex, allowedIndex, 0)));

            withMonitoring(logger, () -> { assertBusy(() -> verifyCcrMonitoring(allowedIndex, allowedIndex), 120L, TimeUnit.SECONDS); });

            pauseFollow(allowedIndex);
            // Make sure that there are no other ccr relates operations running:
            assertBusy(() -> {
                assertNoPendingPersistentTasks();
                assertThat(getCcrNodeTasks(), empty());
            });

            resumeFollow(allowedIndex);
            assertThat(getCcrNodeTasks(), contains(new CcrNodeTask("leader_cluster", allowedIndex, allowedIndex, 0)));
            pauseFollow(allowedIndex);
            // Make sure that there are no other ccr relates operations running:
            assertBusy(() -> {
                assertNoPendingPersistentTasks();
                assertThat(getCcrNodeTasks(), empty());
            });

            closeIndex(allowedIndex);
            unfollow(allowedIndex);
            Exception e = expectThrows(ResponseException.class, () -> resumeFollow(allowedIndex));
            assertThat(e.getMessage(), containsString("follow index [" + allowedIndex + "] does not have ccr metadata"));

            // User does not have manage_follow_index index privilege for 'unallowedIndex':
            e = expectThrows(ResponseException.class, () -> followIndex(client(), "leader_cluster", unallowedIndex, unallowedIndex));
            assertThat(e.getMessage(), containsString("action [indices:admin/xpack/ccr/put_follow] is unauthorized for user [test_ccr]"));
            // Verify that the follow index has not been created and no node tasks are running
            assertThat(indexExists(unallowedIndex), is(false));
            assertBusy(() -> assertThat(getCcrNodeTasks(), empty()));

            // User does have manage_follow_index index privilege on 'allowed' index,
            // but not read / monitor roles on 'disallowed' index:
            e = expectThrows(ResponseException.class, () -> followIndex(client(), "leader_cluster", unallowedIndex, allowedIndex));
            assertThat(
                e.getMessage(),
                containsString(
                    "insufficient privileges to follow index [unallowed-index], "
                        + "privilege for action [indices:data/read/xpack/ccr/shard_changes] is missing, "
                        + "privilege for action [indices:monitor/stats] is missing"
                )
            );
            // Verify that the follow index has not been created and no node tasks are running
            assertThat(indexExists(unallowedIndex), is(false));
            assertBusy(() -> assertThat(getCcrNodeTasks(), empty()));

            followIndex(adminClient(), "leader_cluster", unallowedIndex, unallowedIndex);
            pauseFollow(adminClient(), unallowedIndex);

            e = expectThrows(ResponseException.class, () -> resumeFollow(unallowedIndex));
            assertThat(
                e.getMessage(),
                containsString(
                    "insufficient privileges to follow index [unallowed-index], "
                        + "privilege for action [indices:data/read/xpack/ccr/shard_changes] is missing, "
                        + "privilege for action [indices:monitor/stats] is missing"
                )
            );
            assertBusy(() -> assertThat(getCcrNodeTasks(), empty()));

            e = expectThrows(
                ResponseException.class,
                () -> client().performRequest(new Request("POST", "/" + unallowedIndex + "/_ccr/unfollow"))
            );
            assertThat(e.getMessage(), containsString("action [indices:admin/xpack/ccr/unfollow] is unauthorized for user [test_ccr]"));
            assertOK(adminClient().performRequest(new Request("POST", "/" + unallowedIndex + "/_close")));
            assertOK(adminClient().performRequest(new Request("POST", "/" + unallowedIndex + "/_ccr/unfollow")));
            assertBusy(() -> assertThat(getCcrNodeTasks(), empty()));
        }
    }

    public void testAutoFollowPatterns() throws Exception {
        assumeTrue("Test should only run with target_cluster=follow", "follow".equals(targetCluster));

        final String prefix = getTestName().toLowerCase(Locale.ROOT);
        String allowedIndex = prefix + "-eu_20190101";
        String disallowedIndex = prefix + "-us_20190101";

        final String pattern = "pattern_" + prefix;
        {
            Request request = new Request("PUT", "/_ccr/auto_follow/" + pattern);
            request.setJsonEntity("""
                {"leader_index_patterns": ["testautofollowpatterns-*"], "remote_cluster": "leader_cluster"}""");
            Exception e = expectThrows(ResponseException.class, () -> assertOK(client().performRequest(request)));
            assertThat(e.getMessage(), containsString("insufficient privileges to follow index [testautofollowpatterns-*]"));
        }

        Request request = new Request("PUT", "/_ccr/auto_follow/" + pattern);
        request.setJsonEntity("""
            {"leader_index_patterns": ["testautofollowpatterns-eu*"], "remote_cluster": "leader_cluster"}""");
        assertOK(client().performRequest(request));

        try (RestClient leaderClient = buildLeaderClient()) {
            for (String index : new String[] { allowedIndex, disallowedIndex }) {
                String requestBody = """
                    {"mappings": {"properties": {"field": {"type": "keyword"}}}}""";
                request = new Request("PUT", "/" + index);
                request.setJsonEntity(requestBody);
                assertOK(leaderClient.performRequest(request));

                for (int i = 0; i < 5; i++) {
                    String id = Integer.toString(i);
                    index(leaderClient, index, id, "field", i, "filtered_field", "true");
                }
            }
        }

        try {
            assertBusy(() -> ensureYellow(allowedIndex), 30, TimeUnit.SECONDS);
            assertBusy(() -> verifyDocuments(allowedIndex, 5, "*:*"), 30, TimeUnit.SECONDS);
            assertThat(indexExists(disallowedIndex), is(false));
            withMonitoring(logger, () -> {
                assertBusy(() -> verifyCcrMonitoring(allowedIndex, allowedIndex), 120L, TimeUnit.SECONDS);
                assertBusy(ESCCRRestTestCase::verifyAutoFollowMonitoring, 120L, TimeUnit.SECONDS);
            });
        } finally {
            // Cleanup by deleting auto follow pattern and pause following:
            try {
                deleteAutoFollowPattern(pattern);
                pauseFollow(allowedIndex);
            } catch (Throwable e) {
                logger.warn("Failed to cleanup after the test", e);
            }
        }
    }

    public void testForgetFollower() throws IOException {
        final String forgetLeader = "forget-leader";
        final String forgetFollower = "forget-follower";
        if ("leader".equals(targetCluster)) {
            logger.info("running against leader cluster");
            createIndex(adminClient(), forgetLeader, indexSettings(1, 0).build());
        } else {
            logger.info("running against follower cluster");
            followIndex(client(), "leader_cluster", forgetLeader, forgetFollower);

            final Response response = client().performRequest(new Request("GET", "/" + forgetFollower + "/_stats"));
            final String followerIndexUUID = ObjectPath.createFromResponse(response).evaluate("indices." + forgetFollower + ".uuid");

            pauseFollow(forgetFollower);

            try (RestClient leaderClient = buildLeaderClient(restAdminSettings())) {
                final Request request = new Request("POST", "/" + forgetLeader + "/_ccr/forget_follower");
                final String requestBody = Strings.format("""
                    {
                      "follower_cluster": "follow-cluster",
                      "follower_index": "%s",
                      "follower_index_uuid": "%s",
                      "leader_remote_cluster": "leader_cluster"
                    }""", forgetFollower, followerIndexUUID);
                request.setJsonEntity(requestBody);
                final Response forgetFollowerResponse = leaderClient.performRequest(request);
                assertOK(forgetFollowerResponse);
                final Map<?, ?> shards = ObjectPath.createFromResponse(forgetFollowerResponse).evaluate("_shards");
                assertNull(shards.get("failures"));
                assertThat(shards.get("total"), equalTo(1));
                assertThat(shards.get("successful"), equalTo(1));
                assertThat(shards.get("failed"), equalTo(0));

                final Request retentionLeasesRequest = new Request("GET", "/" + forgetLeader + "/_stats");
                retentionLeasesRequest.addParameter("level", "shards");
                final Response retentionLeasesResponse = leaderClient.performRequest(retentionLeasesRequest);
                final ArrayList<Object> shardsStats = ObjectPath.createFromResponse(retentionLeasesResponse)
                    .evaluate("indices." + forgetLeader + ".shards.0");
                assertThat(shardsStats, hasSize(1));
                final Map<?, ?> shardStatsAsMap = (Map<?, ?>) shardsStats.get(0);
                final Map<?, ?> retentionLeasesStats = (Map<?, ?>) shardStatsAsMap.get("retention_leases");
                final List<?> leases = (List<?>) retentionLeasesStats.get("leases");
                for (final Object lease : leases) {
                    assertThat(((Map<?, ?>) lease).get("source"), equalTo(ReplicationTracker.PEER_RECOVERY_RETENTION_LEASE_SOURCE));
                }
            }
        }
    }

    public void testCleanShardFollowTaskAfterDeleteFollower() throws Exception {
        final String cleanLeader = "clean-leader";
        final String cleanFollower = "clean-follower";
        if ("leader".equals(targetCluster)) {
            logger.info("running against leader cluster");
            final Settings indexSettings = indexSettings(1, 0).put("index.soft_deletes.enabled", true).build();
            createIndex(adminClient(), cleanLeader, indexSettings);
        } else {
            logger.info("running against follower cluster");
            followIndex(client(), "leader_cluster", cleanLeader, cleanFollower);
            deleteIndex(client(), cleanFollower);
            // the shard follow task should have been cleaned up on behalf of the user, see ShardFollowTaskCleaner
            assertBusy(() -> {
                assertNoPendingPersistentTasks();
                assertThat(getCcrNodeTasks(), empty());
            });
        }
    }

    public void testUnPromoteAndFollowDataStream() throws Exception {
        assumeTrue("Test should only run with target_cluster=follow", "follow".equals(targetCluster));

        var numDocs = 64;
        var dataStreamName = "logs-eu-monitor1";
        var dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'hh:mm:ss", Locale.ROOT);

        // Setup
        {
            createAutoFollowPattern(adminClient(), "test_pattern", "logs-eu*", "leader_cluster", null);
        }
        // Create data stream and ensure that it is auto followed
        {
            try (var leaderClient = buildLeaderClient()) {
                for (var i = 0; i < numDocs; i++) {
                    var indexRequest = new Request("POST", "/" + dataStreamName + "/_doc");
                    indexRequest.addParameter("refresh", "true");
                    indexRequest.setJsonEntity(Strings.format("""
                        {"@timestamp": "%s","message":"abc"}
                        """, dateFormat.format(new Date())));
                    assertOK(leaderClient.performRequest(indexRequest));
                }
                verifyDataStream(leaderClient, dataStreamName, backingIndexName(dataStreamName, 1));
                verifyDocuments(leaderClient, dataStreamName, numDocs);
            }
            assertBusy(() -> {
                verifyDataStream(client(), dataStreamName, backingIndexName(dataStreamName, 1));
                ensureYellow(dataStreamName);
                verifyDocuments(client(), dataStreamName, numDocs);
            });
        }
        // promote and unfollow
        {
            assertOK(client().performRequest(new Request("POST", "/_data_stream/_promote/" + dataStreamName)));
            // Now that the data stream is a non replicated data stream, rollover.
            assertOK(client().performRequest(new Request("POST", "/" + dataStreamName + "/_rollover")));
            // Unfollow .ds-logs-eu-monitor1-000001,
            // which is now possible because this index can now be closed as it is no longer the write index.
            pauseFollow(backingIndexName(dataStreamName, 1));
            closeIndex(backingIndexName(dataStreamName, 1));
            unfollow(backingIndexName(dataStreamName, 1));
        }
    }

    private static void withMonitoring(Logger logger, CheckedRunnable<Exception> runnable) throws Exception {
        Request enableMonitoring = new Request("PUT", "/_cluster/settings");
        enableMonitoring.setOptions(RequestOptions.DEFAULT.toBuilder().setWarningsHandler(WarningsHandler.PERMISSIVE).build());
        enableMonitoring.setJsonEntity(
            "{\"persistent\":{" + "\"xpack.monitoring.collection.enabled\":true," + "\"xpack.monitoring.collection.interval\":\"1s\"" + "}}"
        );
        assertOK(adminClient().performRequest(enableMonitoring));
        logger.info("monitoring collection enabled");
        try {
            runnable.run();
        } finally {
            Request disableMonitoring = new Request("PUT", "/_cluster/settings");
            disableMonitoring.setOptions(RequestOptions.DEFAULT.toBuilder().setWarningsHandler(WarningsHandler.PERMISSIVE).build());
            disableMonitoring.setJsonEntity(
                "{\"persistent\":{"
                    + "\"xpack.monitoring.collection.enabled\":null,"
                    + "\"xpack.monitoring.collection.interval\":null"
                    + "}}"
            );
            assertOK(adminClient().performRequest(disableMonitoring));
            logger.info("monitoring collection disabled");
        }
    }

    private static void assertNoPendingPersistentTasks() throws IOException {
        Map<String, Object> clusterState = toMap(adminClient().performRequest(new Request("GET", "/_cluster/state")));
        List<?> tasks = ((List<?>) XContentMapValues.extractValue("metadata.persistent_tasks.tasks", clusterState)).stream()
            .filter(
                task -> (((task instanceof Map<?, ?> taskMap)
                    && taskMap.containsKey("id")
                    && taskMap.get("id").equals(HealthNode.TASK_NAME))) == false
            )
            .toList();
        assertThat(tasks, empty());
    }
}
