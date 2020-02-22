/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ccr;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.index.seqno.ReplicationTracker;
import org.elasticsearch.test.rest.yaml.ObjectPath;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken.basicAuthHeaderValue;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

public class FollowIndexSecurityIT extends ESCCRRestTestCase {

    @Override
    protected Settings restClientSettings() {
        String token = basicAuthHeaderValue("test_ccr", new SecureString("x-pack-test-password".toCharArray()));
        return Settings.builder()
            .put(ThreadContext.PREFIX + ".Authorization", token)
            .build();
    }

    @Override
    protected Settings restAdminSettings() {
        String token = basicAuthHeaderValue("test_admin", new SecureString("x-pack-test-password".toCharArray()));
        return Settings.builder()
            .put(ThreadContext.PREFIX + ".Authorization", token)
            .build();
    }

    public void testFollowIndex() throws Exception {
        final int numDocs = 16;
        final String allowedIndex = "allowed-index";
        final String unallowedIndex  = "unallowed-index";
        if ("leader".equals(targetCluster)) {
            logger.info("Running against leader cluster");
            createIndex(allowedIndex, Settings.EMPTY);
            createIndex(unallowedIndex, Settings.EMPTY);
            for (int i = 0; i < numDocs; i++) {
                logger.info("Indexing doc [{}]", i);
                index(allowedIndex, Integer.toString(i), "field", i);
            }
            for (int i = 0; i < numDocs; i++) {
                logger.info("Indexing doc [{}]", i);
                index(unallowedIndex, Integer.toString(i), "field", i);
            }
            refresh(allowedIndex);
            verifyDocuments(allowedIndex, numDocs, "*:*");
        } else {
            followIndex(client(), "leader_cluster", allowedIndex, allowedIndex);
            assertBusy(() -> verifyDocuments(allowedIndex, numDocs, "*:*"));
            assertThat(countCcrNodeTasks(), equalTo(1));
            assertBusy(() -> verifyCcrMonitoring(allowedIndex, allowedIndex), 30, TimeUnit.SECONDS);
            assertOK(client().performRequest(new Request("POST", "/" + allowedIndex + "/_ccr/pause_follow")));
            // Make sure that there are no other ccr relates operations running:
            assertBusy(() -> {
                Map<String, Object> clusterState = toMap(adminClient().performRequest(new Request("GET", "/_cluster/state")));
                List<?> tasks = (List<?>) XContentMapValues.extractValue("metadata.persistent_tasks.tasks", clusterState);
                assertThat(tasks.size(), equalTo(0));
                assertThat(countCcrNodeTasks(), equalTo(0));
            });

            resumeFollow(allowedIndex);
            assertThat(countCcrNodeTasks(), equalTo(1));
            assertOK(client().performRequest(new Request("POST", "/" + allowedIndex + "/_ccr/pause_follow")));
            // Make sure that there are no other ccr relates operations running:
            assertBusy(() -> {
                Map<String, Object> clusterState = toMap(adminClient().performRequest(new Request("GET", "/_cluster/state")));
                List<?> tasks = (List<?>) XContentMapValues.extractValue("metadata.persistent_tasks.tasks", clusterState);
                assertThat(tasks.size(), equalTo(0));
                assertThat(countCcrNodeTasks(), equalTo(0));
            });

            assertOK(client().performRequest(new Request("POST", "/" + allowedIndex + "/_close")));
            assertOK(client().performRequest(new Request("POST", "/" + allowedIndex + "/_ccr/unfollow")));
            Exception e = expectThrows(ResponseException.class, () -> resumeFollow(allowedIndex));
            assertThat(e.getMessage(), containsString("follow index [" + allowedIndex + "] does not have ccr metadata"));

            // User does not have manage_follow_index index privilege for 'unallowedIndex':
            e = expectThrows(ResponseException.class, () -> followIndex(client(), "leader_cluster", unallowedIndex, unallowedIndex));
            assertThat(e.getMessage(),
                containsString("action [indices:admin/xpack/ccr/put_follow] is unauthorized for user [test_ccr]"));
            // Verify that the follow index has not been created and no node tasks are running
            assertThat(indexExists(unallowedIndex), is(false));
            assertBusy(() -> assertThat(countCcrNodeTasks(), equalTo(0)));

            // User does have manage_follow_index index privilege on 'allowed' index,
            // but not read / monitor roles on 'disallowed' index:
            e = expectThrows(ResponseException.class, () -> followIndex(client(), "leader_cluster", unallowedIndex, allowedIndex));
            assertThat(e.getMessage(), containsString("insufficient privileges to follow index [unallowed-index], " +
                "privilege for action [indices:monitor/stats] is missing, " +
                "privilege for action [indices:data/read/xpack/ccr/shard_changes] is missing"));
            // Verify that the follow index has not been created and no node tasks are running
            assertThat(indexExists(unallowedIndex), is(false));
            assertBusy(() -> assertThat(countCcrNodeTasks(), equalTo(0)));

            followIndex(adminClient(), "leader_cluster", unallowedIndex, unallowedIndex);
            pauseFollow(adminClient(), unallowedIndex);

            e = expectThrows(ResponseException.class, () -> resumeFollow(unallowedIndex));
            assertThat(e.getMessage(), containsString("insufficient privileges to follow index [unallowed-index], " +
                "privilege for action [indices:monitor/stats] is missing, " +
                "privilege for action [indices:data/read/xpack/ccr/shard_changes] is missing"));
            assertBusy(() -> assertThat(countCcrNodeTasks(), equalTo(0)));

            e = expectThrows(ResponseException.class,
                () -> client().performRequest(new Request("POST", "/" + unallowedIndex + "/_ccr/unfollow")));
            assertThat(e.getMessage(), containsString("action [indices:admin/xpack/ccr/unfollow] is unauthorized for user [test_ccr]"));
            assertOK(adminClient().performRequest(new Request("POST", "/" + unallowedIndex + "/_close")));
            assertOK(adminClient().performRequest(new Request("POST", "/" + unallowedIndex + "/_ccr/unfollow")));
            assertBusy(() -> assertThat(countCcrNodeTasks(), equalTo(0)));
        }
    }

    public void testAutoFollowPatterns() throws Exception {
        assumeFalse("Test should only run when both clusters are running", "leader".equals(targetCluster));
        String allowedIndex = "logs-eu-20190101";
        String disallowedIndex = "logs-us-20190101";

        {
            Request request = new Request("PUT", "/_ccr/auto_follow/test_pattern");
            request.setJsonEntity("{\"leader_index_patterns\": [\"logs-*\"], \"remote_cluster\": \"leader_cluster\"}");
            Exception e = expectThrows(ResponseException.class, () -> assertOK(client().performRequest(request)));
            assertThat(e.getMessage(), containsString("insufficient privileges to follow index [logs-*]"));
        }

        Request request = new Request("PUT", "/_ccr/auto_follow/test_pattern");
        request.setJsonEntity("{\"leader_index_patterns\": [\"logs-eu-*\"], \"remote_cluster\": \"leader_cluster\"}");
        assertOK(client().performRequest(request));

        try (RestClient leaderClient = buildLeaderClient()) {
            for (String index : new String[]{allowedIndex, disallowedIndex}) {
                String requestBody = "{\"mappings\": {\"properties\": {\"field\": {\"type\": \"keyword\"}}}}";
                request = new Request("PUT", "/" + index);
                request.setJsonEntity(requestBody);
                assertOK(leaderClient.performRequest(request));

                for (int i = 0; i < 5; i++) {
                    String id = Integer.toString(i);
                    index(leaderClient, index, id, "field", i, "filtered_field", "true");
                }
            }
        }

        assertBusy(() -> {
            ensureYellow(allowedIndex);
            verifyDocuments(allowedIndex, 5, "*:*");
        }, 30, TimeUnit.SECONDS);
        assertThat(indexExists(disallowedIndex), is(false));
        assertBusy(() -> {
            verifyCcrMonitoring(allowedIndex, allowedIndex);
            verifyAutoFollowMonitoring();
        }, 30, TimeUnit.SECONDS);

        // Cleanup by deleting auto follow pattern and pause following:
        request = new Request("DELETE", "/_ccr/auto_follow/test_pattern");
        assertOK(client().performRequest(request));
        pauseFollow(client(), allowedIndex);
    }

    public void testForgetFollower() throws IOException {
        final String forgetLeader = "forget-leader";
        final String forgetFollower = "forget-follower";
        if ("leader".equals(targetCluster)) {
            logger.info("running against leader cluster");
            final Settings indexSettings = Settings.builder().put("index.number_of_replicas", 0).put("index.number_of_shards", 1).build();
            createIndex(forgetLeader, indexSettings);
        } else {
            logger.info("running against follower cluster");
            followIndex(client(), "leader_cluster", forgetLeader, forgetFollower);

            final Response response = client().performRequest(new Request("GET", "/" + forgetFollower + "/_stats"));
            final String followerIndexUUID = ObjectPath.createFromResponse(response).evaluate("indices." + forgetFollower + ".uuid");

            assertOK(client().performRequest(new Request("POST", "/" + forgetFollower + "/_ccr/pause_follow")));

            try (RestClient leaderClient = buildLeaderClient(restAdminSettings())) {
                final Request request = new Request("POST", "/" + forgetLeader + "/_ccr/forget_follower");
                final String requestBody = "{" +
                        "\"follower_cluster\":\"follow-cluster\"," +
                        "\"follower_index\":\"" +  forgetFollower + "\"," +
                        "\"follower_index_uuid\":\"" + followerIndexUUID + "\"," +
                        "\"leader_remote_cluster\":\"leader_cluster\"" +
                        "}";
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
                final ArrayList<Object> shardsStats =
                        ObjectPath.createFromResponse(retentionLeasesResponse).evaluate("indices." + forgetLeader + ".shards.0");
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
            final Settings indexSettings = Settings.builder()
                .put("index.number_of_replicas", 0)
                .put("index.number_of_shards", 1)
                .put("index.soft_deletes.enabled", true)
                .build();
            createIndex(cleanLeader, indexSettings);
        } else {
            logger.info("running against follower cluster");
            followIndex(client(), "leader_cluster", cleanLeader, cleanFollower);

            final Request request = new Request("DELETE", "/" + cleanFollower);
            final Response response = client().performRequest(request);
            assertOK(response);
            // the shard follow task should have been cleaned up on behalf of the user, see ShardFollowTaskCleaner
            assertBusy(() -> {
                Map<String, Object> clusterState = toMap(adminClient().performRequest(new Request("GET", "/_cluster/state")));
                List<?> tasks = (List<?>) XContentMapValues.extractValue("metadata.persistent_tasks.tasks", clusterState);
                assertThat(tasks.size(), equalTo(0));
                assertThat(countCcrNodeTasks(), equalTo(0));
            });
        }
    }

}
