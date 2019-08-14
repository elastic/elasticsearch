/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.slm;

import org.apache.http.util.EntityUtils;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.DeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xpack.core.ilm.LifecycleSettings;
import org.elasticsearch.xpack.core.slm.SnapshotLifecyclePolicy;
import org.elasticsearch.xpack.core.slm.SnapshotRetentionConfiguration;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.startsWith;

public class SnapshotLifecycleIT extends ESRestTestCase {

    @Override
    protected boolean waitForAllSnapshotsWiped() {
        return true;
    }

    public void testMissingRepo() throws Exception {
        SnapshotLifecyclePolicy policy = new SnapshotLifecyclePolicy("test-policy", "snap",
            "*/1 * * * * ?", "missing-repo", Collections.emptyMap(), SnapshotRetentionConfiguration.EMPTY);

        Request putLifecycle = new Request("PUT", "/_slm/policy/test-policy");
        XContentBuilder lifecycleBuilder = JsonXContent.contentBuilder();
        policy.toXContent(lifecycleBuilder, ToXContent.EMPTY_PARAMS);
        putLifecycle.setJsonEntity(Strings.toString(lifecycleBuilder));
        ResponseException e = expectThrows(ResponseException.class, () -> client().performRequest(putLifecycle));
        Response resp = e.getResponse();
        assertThat(resp.getStatusLine().getStatusCode(), equalTo(400));
        String jsonError = EntityUtils.toString(resp.getEntity());
        assertThat(jsonError, containsString("\"type\":\"illegal_argument_exception\""));
        assertThat(jsonError, containsString("\"reason\":\"no such repository [missing-repo]\""));
    }

    @SuppressWarnings("unchecked")
    public void testFullPolicySnapshot() throws Exception {
        final String indexName = "test";
        final String policyName = "test-policy";
        final String repoId = "my-repo";
        int docCount = randomIntBetween(10, 50);
        List<IndexRequestBuilder> indexReqs = new ArrayList<>();
        for (int i = 0; i < docCount; i++) {
            index(client(), indexName, "" + i, "foo", "bar");
        }

        // Create a snapshot repo
        initializeRepo(repoId);

        createSnapshotPolicy(policyName, "snap", "*/1 * * * * ?", repoId, indexName, true);

        // Check that the snapshot was actually taken
        assertBusy(() -> {
            Response response = client().performRequest(new Request("GET", "/_snapshot/" + repoId + "/_all"));
            Map<String, Object> snapshotResponseMap;
            try (InputStream is = response.getEntity().getContent()) {
                snapshotResponseMap = XContentHelper.convertToMap(XContentType.JSON.xContent(), is, true);
            }
            assertThat(snapshotResponseMap.size(), greaterThan(0));
            List<Map<String, Object>> snapResponse = ((List<Map<String, Object>>) snapshotResponseMap.get("responses")).stream()
                .findFirst()
                .map(m -> (List<Map<String, Object>>) m.get("snapshots"))
                .orElseThrow(() -> new AssertionError("failed to find snapshot response in " + snapshotResponseMap));
            assertThat(snapResponse.size(), greaterThan(0));
            assertThat(snapResponse.get(0).get("snapshot").toString(), startsWith("snap-"));
            assertThat(snapResponse.get(0).get("indices"), equalTo(Collections.singletonList(indexName)));
            Map<String, Object> metadata = (Map<String, Object>) snapResponse.get(0).get("metadata");
            assertNotNull(metadata);
            assertThat(metadata.get("policy"), equalTo(policyName));
            assertHistoryIsPresent(policyName, true, repoId);

            // Check that the last success date was written to the cluster state
            Request getReq = new Request("GET", "/_slm/policy/" + policyName);
            Response policyMetadata = client().performRequest(getReq);
            Map<String, Object> policyResponseMap;
            try (InputStream is = policyMetadata.getEntity().getContent()) {
                policyResponseMap = XContentHelper.convertToMap(XContentType.JSON.xContent(), is, true);
            }
            Map<String, Object> policyMetadataMap = (Map<String, Object>) policyResponseMap.get(policyName);
            Map<String, Object> lastSuccessObject = (Map<String, Object>) policyMetadataMap.get("last_success");
            assertNotNull(lastSuccessObject);
            Long lastSuccess = (Long) lastSuccessObject.get("time");
            Long modifiedDate = (Long) policyMetadataMap.get("modified_date_millis");
            assertNotNull(lastSuccess);
            assertNotNull(modifiedDate);
            assertThat(lastSuccess, greaterThan(modifiedDate));

            String lastSnapshotName = (String) lastSuccessObject.get("snapshot_name");
            assertThat(lastSnapshotName, startsWith("snap-"));

            assertHistoryIsPresent(policyName, true, repoId);

            Map<String, Object> stats = getSLMStats();
            Map<String, Object> policyStats = (Map<String, Object>) stats.get(SnapshotLifecycleStats.POLICY_STATS.getPreferredName());
            Map<String, Object> policyIdStats = (Map<String, Object>) policyStats.get(policyName);
            int snapsTaken = (int) policyIdStats.get(SnapshotLifecycleStats.SnapshotPolicyStats.SNAPSHOTS_TAKEN.getPreferredName());
            int totalTaken = (int) stats.get(SnapshotLifecycleStats.TOTAL_TAKEN.getPreferredName());
            assertThat(snapsTaken, greaterThanOrEqualTo(1));
            assertThat(totalTaken, greaterThanOrEqualTo(1));
        });

        Request delReq = new Request("DELETE", "/_slm/policy/" + policyName);
        assertOK(client().performRequest(delReq));
    }

    @SuppressWarnings("unchecked")
    public void testPolicyFailure() throws Exception {
        final String policyName = "test-policy";
        final String repoName = "test-repo";
        final String indexPattern = "index-doesnt-exist";
        initializeRepo(repoName);

        // Create a policy with ignore_unvailable: false and an index that doesn't exist
        createSnapshotPolicy(policyName, "snap", "*/1 * * * * ?", repoName, indexPattern, false);

        assertBusy(() -> {
            // Check that the failure is written to the cluster state
            Request getReq = new Request("GET", "/_slm/policy/" + policyName);
            Response policyMetadata = client().performRequest(getReq);
            try (InputStream is = policyMetadata.getEntity().getContent()) {
                Map<String, Object> responseMap = XContentHelper.convertToMap(XContentType.JSON.xContent(), is, true);
                Map<String, Object> policyMetadataMap = (Map<String, Object>) responseMap.get(policyName);
                Map<String, Object> lastFailureObject = (Map<String, Object>) policyMetadataMap.get("last_failure");
                assertNotNull(lastFailureObject);

                Long lastFailure = (Long) lastFailureObject.get("time");
                Long modifiedDate = (Long) policyMetadataMap.get("modified_date_millis");
                assertNotNull(lastFailure);
                assertNotNull(modifiedDate);
                assertThat(lastFailure, greaterThan(modifiedDate));

                String lastFailureInfo = (String) lastFailureObject.get("details");
                assertNotNull(lastFailureInfo);
                assertThat(lastFailureInfo, containsString("no such index [index-doesnt-exist]"));

                String snapshotName = (String) lastFailureObject.get("snapshot_name");
                assertNotNull(snapshotName);
                assertThat(snapshotName, startsWith("snap-"));
            }
            assertHistoryIsPresent(policyName, false, repoName);

            Map<String, Object> stats = getSLMStats();
            Map<String, Object> policyStats = (Map<String, Object>) stats.get(SnapshotLifecycleStats.POLICY_STATS.getPreferredName());
            Map<String, Object> policyIdStats = (Map<String, Object>) policyStats.get(policyName);
            int snapsFailed = (int) policyIdStats.get(SnapshotLifecycleStats.SnapshotPolicyStats.SNAPSHOTS_FAILED.getPreferredName());
            int totalFailed = (int) stats.get(SnapshotLifecycleStats.TOTAL_FAILED.getPreferredName());
            assertThat(snapsFailed, greaterThanOrEqualTo(1));
            assertThat(totalFailed, greaterThanOrEqualTo(1));
        });
    }

    @SuppressWarnings("unchecked")
    public void testPolicyManualExecution() throws Exception {
        final String indexName = "test";
        final String policyName = "test-policy";
        final String repoId = "my-repo";
        int docCount = randomIntBetween(10, 50);
        for (int i = 0; i < docCount; i++) {
            index(client(), indexName, "" + i, "foo", "bar");
        }

        // Create a snapshot repo
        initializeRepo(repoId);

        createSnapshotPolicy(policyName, "snap", "1 2 3 4 5 ?", repoId, indexName, true);

        ResponseException badResp = expectThrows(ResponseException.class,
            () -> client().performRequest(new Request("PUT", "/_slm/policy/" + policyName + "-bad/_execute")));
        assertThat(EntityUtils.toString(badResp.getResponse().getEntity()),
            containsString("no such snapshot lifecycle policy [" + policyName + "-bad]"));

        final String snapshotName = executePolicy(policyName);

        // Check that the executed snapshot is created
        assertBusy(() -> {
            try {
                Response response = client().performRequest(new Request("GET", "/_snapshot/" + repoId + "/" + snapshotName));
                Map<String, Object> snapshotResponseMap;
                try (InputStream is = response.getEntity().getContent()) {
                    snapshotResponseMap = XContentHelper.convertToMap(XContentType.JSON.xContent(), is, true);
                }
                assertThat(snapshotResponseMap.size(), greaterThan(0));
                final Map<String, Object> metadata = extractMetadata(snapshotResponseMap, snapshotName);
                assertNotNull(metadata);
                assertThat(metadata.get("policy"), equalTo(policyName));
                assertHistoryIsPresent(policyName, true, repoId);
            } catch (ResponseException e) {
                fail("expected snapshot to exist but it does not: " + EntityUtils.toString(e.getResponse().getEntity()));
            }

            Map<String, Object> stats = getSLMStats();
            Map<String, Object> policyStats = (Map<String, Object>) stats.get(SnapshotLifecycleStats.POLICY_STATS.getPreferredName());
            Map<String, Object> policyIdStats = (Map<String, Object>) policyStats.get(policyName);
            int snapsTaken = (int) policyIdStats.get(SnapshotLifecycleStats.SnapshotPolicyStats.SNAPSHOTS_TAKEN.getPreferredName());
            int totalTaken = (int) stats.get(SnapshotLifecycleStats.TOTAL_TAKEN.getPreferredName());
            assertThat(snapsTaken, equalTo(1));
            assertThat(totalTaken, equalTo(1));
        });
    }

    @SuppressWarnings("unchecked")
    public void testBasicTimeBasedRetenion() throws Exception {
        final String indexName = "test";
        final String policyName = "test-policy";
        final String repoId = "my-repo";
        int docCount = randomIntBetween(10, 50);
        List<IndexRequestBuilder> indexReqs = new ArrayList<>();
        for (int i = 0; i < docCount; i++) {
            index(client(), indexName, "" + i, "foo", "bar");
        }

        // Create a snapshot repo
        initializeRepo(repoId);

        // Create a policy with a retention period of 1 millisecond
        createSnapshotPolicy(policyName, "snap", "1 2 3 4 5 ?", repoId, indexName, true,
            new SnapshotRetentionConfiguration(TimeValue.timeValueMillis(1), null, null));

        // Manually create a snapshot
        final String snapshotName = executePolicy(policyName);
        // Check that the executed snapshot is created
        assertBusy(() -> {
            try {
                Response response = client().performRequest(new Request("GET", "/_snapshot/" + repoId + "/" + snapshotName));
                Map<String, Object> snapshotResponseMap;
                try (InputStream is = response.getEntity().getContent()) {
                    snapshotResponseMap = XContentHelper.convertToMap(XContentType.JSON.xContent(), is, true);
                }
                assertThat(snapshotResponseMap.size(), greaterThan(0));
                final Map<String, Object> metadata = extractMetadata(snapshotResponseMap, snapshotName);
                assertNotNull(metadata);
                assertThat(metadata.get("policy"), equalTo(policyName));
                assertHistoryIsPresent(policyName, true, repoId);
            } catch (ResponseException e) {
                fail("expected snapshot to exist but it does not: " + EntityUtils.toString(e.getResponse().getEntity()));
            }
        });

        // Run retention every second
        ClusterUpdateSettingsRequest req = new ClusterUpdateSettingsRequest();
        req.transientSettings(Settings.builder().put(LifecycleSettings.SLM_RETENTION_SCHEDULE, "*/1 * * * * ?"));
        try (XContentBuilder builder = jsonBuilder()) {
            req.toXContent(builder, ToXContent.EMPTY_PARAMS);
            Request r = new Request("PUT", "/_cluster/settings");
            r.setJsonEntity(Strings.toString(builder));
            Response updateSettingsResp = client().performRequest(r);
        }

        try {
            // Check that the snapshot created by the policy has been removed by retention
            assertBusy(() -> {
                // We expect a failed response because the snapshot should not exist
                try {
                    logger.info("--> checking to see if snapshot has been deleted...");
                    Response response = client().performRequest(new Request("GET", "/_snapshot/" + repoId + "/" + snapshotName));
                    assertThat(EntityUtils.toString(response.getEntity()), containsString("snapshot_missing_exception"));
                } catch (ResponseException e) {
                    assertThat(EntityUtils.toString(e.getResponse().getEntity()), containsString("snapshot_missing_exception"));
                }

                Map<String, Object> stats = getSLMStats();
                Map<String, Object> policyStats = (Map<String, Object>) stats.get(SnapshotLifecycleStats.POLICY_STATS.getPreferredName());
                Map<String, Object> policyIdStats = (Map<String, Object>) policyStats.get(policyName);
                int snapsTaken = (int) policyIdStats.get(SnapshotLifecycleStats.SnapshotPolicyStats.SNAPSHOTS_TAKEN.getPreferredName());
                int snapsDeleted = (int) policyIdStats.get(SnapshotLifecycleStats.SnapshotPolicyStats.SNAPSHOTS_DELETED.getPreferredName());
                int retentionRun = (int) stats.get(SnapshotLifecycleStats.RETENTION_RUNS.getPreferredName());
                int totalTaken = (int) stats.get(SnapshotLifecycleStats.TOTAL_TAKEN.getPreferredName());
                int totalDeleted = (int) stats.get(SnapshotLifecycleStats.TOTAL_DELETIONS.getPreferredName());
                assertThat(snapsTaken, equalTo(1));
                assertThat(totalTaken, equalTo(1));
                assertThat(retentionRun, greaterThanOrEqualTo(1));
                assertThat(snapsDeleted, equalTo(1));
                assertThat(totalDeleted, equalTo(1));
            }, 60, TimeUnit.SECONDS);

        } finally {
            // Unset retention
            ClusterUpdateSettingsRequest unsetRequest = new ClusterUpdateSettingsRequest();
            unsetRequest.transientSettings(Settings.builder().put(LifecycleSettings.SLM_RETENTION_SCHEDULE, (String) null));
            try (XContentBuilder builder = jsonBuilder()) {
                unsetRequest.toXContent(builder, ToXContent.EMPTY_PARAMS);
                Request r = new Request("PUT", "/_cluster/settings");
                r.setJsonEntity(Strings.toString(builder));
                client().performRequest(r);
            }
        }
    }

    @SuppressWarnings("unchecked")
    public void testSnapshotInProgress() throws Exception {
        final String indexName = "test";
        final String policyName = "test-policy";
        final String repoId = "my-repo";
        int docCount = 20;
        for (int i = 0; i < docCount; i++) {
            index(client(), indexName, "" + i, "foo", "bar");
        }

        // Create a snapshot repo
        initializeRepo(repoId, "1b");

        createSnapshotPolicy(policyName, "snap", "1 2 3 4 5 ?", repoId, indexName, true);

        Response executeRepsonse = client().performRequest(new Request("PUT", "/_slm/policy/" + policyName + "/_execute"));

        try (XContentParser parser = JsonXContent.jsonXContent.createParser(NamedXContentRegistry.EMPTY,
            DeprecationHandler.THROW_UNSUPPORTED_OPERATION, EntityUtils.toByteArray(executeRepsonse.getEntity()))) {
            final String snapshotName = parser.mapStrings().get("snapshot_name");

            // Check that the executed snapshot shows up in the SLM output
            assertBusy(() -> {
                try {
                    Response response = client().performRequest(new Request("GET", "/_slm/policy" + (randomBoolean() ? "" : "?human")));
                    Map<String, Object> policyResponseMap;
                    try (InputStream content = response.getEntity().getContent()) {
                        policyResponseMap = XContentHelper.convertToMap(XContentType.JSON.xContent(), content, true);
                    }
                    assertThat(policyResponseMap.size(), greaterThan(0));
                    Optional<Map<String, Object>> inProgress = Optional.ofNullable((Map<String, Object>) policyResponseMap.get(policyName))
                        .map(policy -> (Map<String, Object>) policy.get("in_progress"));

                    if (inProgress.isPresent()) {
                        Map<String, Object> inProgressMap = inProgress.get();
                        assertThat(inProgressMap.get("name"), equalTo(snapshotName));
                        assertNotNull(inProgressMap.get("uuid"));
                        assertThat(inProgressMap.get("state"), equalTo("STARTED"));
                        assertThat((long) inProgressMap.get("start_time_millis"), greaterThan(0L));
                        assertNull(inProgressMap.get("failure"));
                    } else {
                        fail("expected in_progress to contain a running snapshot, but the response was " + policyResponseMap);
                    }
                } catch (ResponseException e) {
                    fail("expected policy to exist but it does not: " + EntityUtils.toString(e.getResponse().getEntity()));
                }
            });

            // Cancel the snapshot since it is not going to complete quickly
            assertOK(client().performRequest(new Request("DELETE", "/_snapshot/" + repoId + "/" + snapshotName)));
        }
    }

    @SuppressWarnings("unchecked")
    public void testRetentionWhileSnapshotInProgress() throws Exception {
        final String indexName = "test";
        final String policyName = "test-policy";
        final String policyName2 = "test-policy2";
        final String repoId = "my-repo";
        final String repoId2 = "my-repo2";
        int docCount = 20;
        for (int i = 0; i < docCount; i++) {
            index(client(), indexName, "" + i, "foo", "bar");
        }

        // Create snapshot repos, one fast and one slow
        initializeRepo(repoId, "1b");
        initializeRepo(repoId2, "1mb");

        createSnapshotPolicy(policyName, "snap", "1 2 3 4 5 ?", repoId, indexName, true,
            new SnapshotRetentionConfiguration(TimeValue.timeValueSeconds(0), null, null));
        createSnapshotPolicy(policyName2, "snap", "1 2 3 4 5 ?", repoId2, indexName, true,
            new SnapshotRetentionConfiguration(TimeValue.timeValueSeconds(0), null, null));

        // Create a snapshot and wait for it to be complete (need something that can be deleted)
        final String completedSnapshotName = executePolicy(policyName2);
        assertBusy(() -> {
            try {
                Response getResp = client().performRequest(new Request("GET",
                    "/_snapshot/" + repoId2 + "/" + completedSnapshotName + "/_status"));
                try (InputStream content = getResp.getEntity().getContent()) {
                    Map<String, Object> snaps = XContentHelper.convertToMap(XContentType.JSON.xContent(), content, true);
                    List<Map<String, Object>> snaps2 = (List<Map<String, Object>>) snaps.get("snapshots");
                    assertThat(snaps2.get(0).get("state"), equalTo("SUCCESS"));
                }
            } catch (NullPointerException | ResponseException e) {
                fail("unable to retrieve completed snapshot: " + e);
            }
        });

        // Take another snapshot
        final String slowSnapshotName = executePolicy(policyName);

        // Check that the executed snapshot shows up in the SLM output as in_progress
        assertBusy(() -> {
            try {
                Response response = client().performRequest(new Request("GET", "/_slm/policy" + (randomBoolean() ? "" : "?human")));
                Map<String, Object> policyResponseMap;
                try (InputStream content = response.getEntity().getContent()) {
                    policyResponseMap = XContentHelper.convertToMap(XContentType.JSON.xContent(), content, true);
                }
                assertThat(policyResponseMap.size(), greaterThan(0));
                Optional<Map<String, Object>> inProgress = Optional.ofNullable((Map<String, Object>) policyResponseMap.get(policyName))
                    .map(policy -> (Map<String, Object>) policy.get("in_progress"));

                if (inProgress.isPresent()) {
                    Map<String, Object> inProgressMap = inProgress.get();
                    assertThat(inProgressMap.get("name"), equalTo(slowSnapshotName));
                    assertNotNull(inProgressMap.get("uuid"));
                    assertThat(inProgressMap.get("state"), equalTo("STARTED"));
                    assertThat((long) inProgressMap.get("start_time_millis"), greaterThan(0L));
                    assertNull(inProgressMap.get("failure"));
                } else {
                    fail("expected in_progress to contain a running snapshot, but the response was " + policyResponseMap);
                }
            } catch (ResponseException e) {
                fail("expected policy to exist but it does not: " + EntityUtils.toString(e.getResponse().getEntity()));
            }
        });

        // Run retention every second
        ClusterUpdateSettingsRequest req = new ClusterUpdateSettingsRequest();
        req.transientSettings(Settings.builder().put(LifecycleSettings.SLM_RETENTION_SCHEDULE, "*/1 * * * * ?"));
        try (XContentBuilder builder = jsonBuilder()) {
            req.toXContent(builder, ToXContent.EMPTY_PARAMS);
            Request r = new Request("PUT", "/_cluster/settings");
            r.setJsonEntity(Strings.toString(builder));
            client().performRequest(r);
        }

        // Cancel the snapshot since it is not going to complete quickly, do it in a thread because
        // cancelling the snapshot can take a long time and we might as well check retention while
        // its deleting
        Thread t = new Thread(() -> {
            try {
                assertOK(client().performRequest(new Request("DELETE", "/_snapshot/" + repoId + "/" + slowSnapshotName)));
            } catch (IOException e) {
                fail("should not have thrown " + e);
            }
        });
        t.start();

        // Check that the snapshot created by the policy has been removed by retention
        assertBusy(() -> {
            // We expect a failed response because the snapshot should not exist
            try {
                logger.info("--> checking to see if snapshot has been deleted...");
                Response response = client().performRequest(new Request("GET", "/_snapshot/" + repoId + "/" + completedSnapshotName));
                assertThat(EntityUtils.toString(response.getEntity()), containsString("snapshot_missing_exception"));
            } catch (ResponseException e) {
                assertThat(EntityUtils.toString(e.getResponse().getEntity()), containsString("snapshot_missing_exception"));
            }
        }, 60, TimeUnit.SECONDS);

        t.join(5000);
    }

    /**
     * Execute the given policy and return the generated snapshot name
     */
    private String executePolicy(String policyId) {
        try {
            Response executeRepsonse = client().performRequest(new Request("PUT", "/_slm/policy/" + policyId + "/_execute"));
            try (XContentParser parser = JsonXContent.jsonXContent.createParser(NamedXContentRegistry.EMPTY,
                DeprecationHandler.THROW_UNSUPPORTED_OPERATION, EntityUtils.toByteArray(executeRepsonse.getEntity()))) {
                return parser.mapStrings().get("snapshot_name");
            }
        } catch (Exception e) {
            fail("failed to execute policy " + policyId + " - got: " + e);
            throw new RuntimeException(e);
        }
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> extractMetadata(Map<String, Object> snapshotResponseMap, String snapshotPrefix) {
        List<Map<String, Object>> snapResponse = ((List<Map<String, Object>>) snapshotResponseMap.get("responses")).stream()
            .findFirst()
            .map(m -> (List<Map<String, Object>>) m.get("snapshots"))
            .orElseThrow(() -> new AssertionError("failed to find snapshot response in " + snapshotResponseMap));
        return snapResponse.stream()
            .filter(snapshot -> ((String) snapshot.get("snapshot")).startsWith(snapshotPrefix))
            .map(snapshot -> (Map<String, Object>) snapshot.get("metadata"))
            .findFirst()
            .orElse(null);
    }

    private Map<String, Object> getSLMStats() {
        try {
            Response response = client().performRequest(new Request("GET", "/_slm/stats"));
            try (InputStream content = response.getEntity().getContent()) {
                return XContentHelper.convertToMap(XContentType.JSON.xContent(), content, true);
            }
        } catch (Exception e) {
            fail("exception retrieving stats: " + e);
            throw new ElasticsearchException(e);
        }
    }

    // This method should be called inside an assertBusy, it has no retry logic of its own
    private void assertHistoryIsPresent(String policyName, boolean success, String repository) throws IOException {
        final Request historySearchRequest = new Request("GET", ".slm-history*/_search");
        historySearchRequest.setJsonEntity("{\n" +
            "  \"query\": {\n" +
            "    \"bool\": {\n" +
            "      \"must\": [\n" +
            "        {\n" +
            "          \"term\": {\n" +
            "            \"policy\": \"" + policyName + "\"\n" +
            "          }\n" +
            "        },\n" +
            "        {\n" +
            "          \"term\": {\n" +
            "            \"success\": " + success + "\n" +
            "          }\n" +
            "        },\n" +
            "        {\n" +
            "          \"term\": {\n" +
            "            \"repository\": \"" + repository + "\"\n" +
            "          }\n" +
            "        },\n" +
            "        {\n" +
            "          \"term\": {\n" +
            "            \"operation\": \"CREATE\"\n" +
            "          }\n" +
            "        }\n" +
            "      ]\n" +
            "    }\n" +
            "  }\n" +
            "}");
        Response historyResponse;
        try {
            historyResponse = client().performRequest(historySearchRequest);
            Map<String, Object> historyResponseMap;
            try (InputStream is = historyResponse.getEntity().getContent()) {
                historyResponseMap = XContentHelper.convertToMap(XContentType.JSON.xContent(), is, true);
            }
            assertThat((int)((Map<String, Object>) ((Map<String, Object>) historyResponseMap.get("hits")).get("total")).get("value"),
                greaterThanOrEqualTo(1));
        } catch (ResponseException e) {
            // Throw AssertionError instead of an exception if the search fails so that assertBusy works as expected
            logger.error(e);
            fail("failed to perform search:" + e.getMessage());
        }
    }

    private void createSnapshotPolicy(String policyName, String snapshotNamePattern, String schedule, String repoId,
                                      String indexPattern, boolean ignoreUnavailable) throws IOException {
        createSnapshotPolicy(policyName, snapshotNamePattern, schedule, repoId, indexPattern,
            ignoreUnavailable, SnapshotRetentionConfiguration.EMPTY);
    }

    private void createSnapshotPolicy(String policyName, String snapshotNamePattern, String schedule, String repoId,
                                      String indexPattern, boolean ignoreUnavailable,
                                      SnapshotRetentionConfiguration retention) throws IOException {
        Map<String, Object> snapConfig = new HashMap<>();
        snapConfig.put("indices", Collections.singletonList(indexPattern));
        snapConfig.put("ignore_unavailable", ignoreUnavailable);
        if (randomBoolean()) {
            Map<String, Object> metadata = new HashMap<>();
            int fieldCount = randomIntBetween(2,5);
            for (int i = 0; i < fieldCount; i++) {
                metadata.put(randomValueOtherThanMany(key -> "policy".equals(key) || metadata.containsKey(key),
                    () -> randomAlphaOfLength(5)), randomAlphaOfLength(4));
            }
        }
        SnapshotLifecyclePolicy policy = new SnapshotLifecyclePolicy(policyName, snapshotNamePattern, schedule,
            repoId, snapConfig, retention);

        Request putLifecycle = new Request("PUT", "/_slm/policy/" + policyName);
        XContentBuilder lifecycleBuilder = JsonXContent.contentBuilder();
        policy.toXContent(lifecycleBuilder, ToXContent.EMPTY_PARAMS);
        putLifecycle.setJsonEntity(Strings.toString(lifecycleBuilder));
        assertOK(client().performRequest(putLifecycle));
    }

    private void initializeRepo(String repoName) throws IOException {
        initializeRepo(repoName, "40mb");
    }

    private void initializeRepo(String repoName, String maxBytesPerSecond) throws IOException {
        Request request = new Request("PUT", "/_snapshot/" + repoName);
        request.setJsonEntity(Strings
            .toString(JsonXContent.contentBuilder()
                .startObject()
                .field("type", "fs")
                .startObject("settings")
                .field("compress", randomBoolean())
                .field("location", System.getProperty("tests.path.repo"))
                .field("max_snapshot_bytes_per_sec", maxBytesPerSecond)
                .endObject()
                .endObject()));
        assertOK(client().performRequest(request));
    }

    private static void index(RestClient client, String index, String id, Object... fields) throws IOException {
        XContentBuilder document = jsonBuilder().startObject();
        for (int i = 0; i < fields.length; i += 2) {
            document.field((String) fields[i], fields[i + 1]);
        }
        document.endObject();
        final Request request = new Request("POST", "/" + index + "/_doc/" + id);
        request.setJsonEntity(Strings.toString(document));
        assertOK(client.performRequest(request));
    }
}
