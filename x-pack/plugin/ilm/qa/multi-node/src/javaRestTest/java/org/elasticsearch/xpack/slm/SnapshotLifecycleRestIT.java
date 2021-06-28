/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
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
import org.elasticsearch.client.ilm.RolloverAction;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.Template;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.common.xcontent.DeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.test.junit.annotations.TestIssueLogging;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xpack.core.ilm.LifecycleSettings;
import org.elasticsearch.xpack.core.ilm.Step;
import org.elasticsearch.xpack.core.ilm.WaitForRolloverReadyStep;
import org.elasticsearch.xpack.core.slm.SnapshotLifecyclePolicy;
import org.elasticsearch.xpack.core.slm.SnapshotLifecycleStats;
import org.elasticsearch.xpack.core.slm.SnapshotRetentionConfiguration;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.xpack.TimeSeriesRestDriver.createComposableTemplate;
import static org.elasticsearch.xpack.TimeSeriesRestDriver.getStepKeyForIndex;
import static org.elasticsearch.xpack.core.slm.history.SnapshotHistoryItem.CREATE_OPERATION;
import static org.elasticsearch.xpack.core.slm.history.SnapshotHistoryItem.DELETE_OPERATION;
import static org.elasticsearch.xpack.core.slm.history.SnapshotHistoryStore.SLM_HISTORY_DATA_STREAM;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.startsWith;

public class SnapshotLifecycleRestIT extends ESRestTestCase {
    private static final String NEVER_EXECUTE_CRON_SCHEDULE = "* * * 31 FEB ? *";

    @Override
    protected boolean waitForAllSnapshotsWiped() {
        return true;
    }

    // as we are testing the SLM history entries we'll preserve the "slm-history-ilm-policy" policy as it'll be associated with the
    // .slm-history-* indices and we won't be able to delete it when we wipe out the cluster
    @Override
    protected boolean preserveILMPoliciesUponCompletion() {
        return true;
    }

    public void testMissingRepo() throws Exception {
        SnapshotLifecyclePolicy policy = new SnapshotLifecyclePolicy("missing-repo-policy", "snap",
            "0 0/15 * * * ?", "missing-repo", Collections.emptyMap(), SnapshotRetentionConfiguration.EMPTY);

        Request putLifecycle = new Request("PUT", "/_slm/policy/missing-repo-policy");
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
        final String policyName = "full-policy";
        final String repoId = "full-policy-repo";
        int docCount = randomIntBetween(10, 50);
        List<IndexRequestBuilder> indexReqs = new ArrayList<>();
        for (int i = 0; i < docCount; i++) {
            index(client(), indexName, "" + i, "foo", "bar");
        }

        // Create a snapshot repo
        initializeRepo(repoId);

        // allow arbitrarily frequent slm snapshots
        disableSLMMinimumIntervalValidation();

        createSnapshotPolicy(policyName, "snap", "*/1 * * * * ?", repoId, indexName, true);

        // Check that the snapshot was actually taken
        assertBusy(() -> {
            Response response = client().performRequest(new Request("GET", "/_snapshot/" + repoId + "/_all"));
            Map<String, Object> snapshotResponseMap;
            try (InputStream is = response.getEntity().getContent()) {
                snapshotResponseMap = XContentHelper.convertToMap(XContentType.JSON.xContent(), is, true);
            }
            assertThat(snapshotResponseMap.size(), greaterThan(0));
            List<Map<String, Object>> snapResponse = ((List<Map<String, Object>>) snapshotResponseMap.get("snapshots"));
            assertThat(snapResponse, not(empty()));
            assertThat(snapResponse.get(0).get("indices"), equalTo(Collections.singletonList(indexName)));
            assertThat((String) snapResponse.get(0).get("snapshot"), startsWith("snap-"));
            Map<String, Object> metadata = (Map<String, Object>) snapResponse.get(0).get("metadata");
            assertNotNull(metadata);
            assertThat(metadata.get("policy"), equalTo(policyName));
        });

        assertBusy(() -> assertHistoryIsPresent(policyName, true, repoId, CREATE_OPERATION));

        // Check that the last success date was written to the cluster state
        assertBusy(() -> {
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
        });

        // Check that the stats are written
        assertBusy(() -> {
            Map<String, Object> stats = getSLMStats();
            Map<String, Object> policyStats = policyStatsAsMap(stats);
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
        final String policyName = "failure-policy";
        final String repoName = "policy-failure-repo";
        final String indexPattern = "index-doesnt-exist";
        initializeRepo(repoName);

        // allow arbitrarily frequent slm snapshots
        disableSLMMinimumIntervalValidation();

        // Create a policy with ignore_unavailable: false and an index that doesn't exist
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
            assertHistoryIsPresent(policyName, false, repoName, CREATE_OPERATION);

            Map<String, Object> stats = getSLMStats();
            Map<String, Object> policyStats = policyStatsAsMap(stats);
            Map<String, Object> policyIdStats = (Map<String, Object>) policyStats.get(policyName);
            int snapsFailed = (int) policyIdStats.get(SnapshotLifecycleStats.SnapshotPolicyStats.SNAPSHOTS_FAILED.getPreferredName());
            int totalFailed = (int) stats.get(SnapshotLifecycleStats.TOTAL_FAILED.getPreferredName());
            assertThat(snapsFailed, greaterThanOrEqualTo(1));
            assertThat(totalFailed, greaterThanOrEqualTo(1));
        });
    }

    @SuppressWarnings("unchecked")
    @TestIssueLogging(value = "org.elasticsearch.xpack.slm:TRACE,org.elasticsearch.xpack.core.slm:TRACE,org.elasticsearch.snapshots:DEBUG",
        issueUrl = "https://github.com/elastic/elasticsearch/issues/48531")
    public void testPolicyManualExecution() throws Exception {
        final String indexName = "test";
        final String policyName = "manual-policy";
        final String repoId = "manual-execution-repo";
        int docCount = randomIntBetween(10, 50);
        for (int i = 0; i < docCount; i++) {
            index(client(), indexName, "" + i, "foo", "bar");
        }

        logSLMPolicies();

        // Create a snapshot repo
        initializeRepo(repoId);

        createSnapshotPolicy(policyName, "snap", NEVER_EXECUTE_CRON_SCHEDULE, repoId, indexName, true);

        ResponseException badResp = expectThrows(ResponseException.class,
            () -> client().performRequest(new Request("POST", "/_slm/policy/" + policyName + "-bad/_execute")));
        assertThat(EntityUtils.toString(badResp.getResponse().getEntity()),
            containsString("no such snapshot lifecycle policy [" + policyName + "-bad]"));

        final String snapshotName = executePolicy(policyName);

        logSLMPolicies();

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
                assertHistoryIsPresent(policyName, true, repoId, CREATE_OPERATION);
            } catch (ResponseException e) {
                fail("expected snapshot to exist but it does not: " + EntityUtils.toString(e.getResponse().getEntity()));
            }

            Map<String, Object> stats = getSLMStats();
            Map<String, Object> policyStats = policyStatsAsMap(stats);
            Map<String, Object> policyIdStats = (Map<String, Object>) policyStats.get(policyName);
            int snapsTaken = (int) policyIdStats.get(SnapshotLifecycleStats.SnapshotPolicyStats.SNAPSHOTS_TAKEN.getPreferredName());
            int totalTaken = (int) stats.get(SnapshotLifecycleStats.TOTAL_TAKEN.getPreferredName());
            assertThat(snapsTaken, equalTo(1));
            assertThat(totalTaken, equalTo(1));
        });
    }


    @SuppressWarnings("unchecked")
    public void testStartStopStatus() throws Exception {
        final String indexName = "test";
        final String policyName = "start-stop-policy";
        final String repoId = "start-stop-repo";
        int docCount = randomIntBetween(10, 50);
        for (int i = 0; i < docCount; i++) {
            index(client(), indexName, "" + i, "foo", "bar");
        }

        // Create a snapshot repo
        initializeRepo(repoId);

        // Stop SLM so nothing happens
        client().performRequest(new Request("POST", "/_slm/stop"));

        assertBusy(() -> {
            logger.info("--> waiting for SLM to stop");
            assertThat(EntityUtils.toString(client().performRequest(new Request("GET", "/_slm/status")).getEntity()),
                containsString("STOPPED"));
        });

        try {
            createSnapshotPolicy(policyName, "snap", "0 0/15 * * * ?", repoId, indexName, true,
                new SnapshotRetentionConfiguration(TimeValue.ZERO, null, null));
            long start = System.currentTimeMillis();
            final String snapshotName = executePolicy(policyName);

            // Check that the executed snapshot is created
            assertBusy(() -> {
                try {
                    logger.info("--> checking for snapshot creation...");
                    Response response = client().performRequest(new Request("GET", "/_snapshot/" + repoId + "/" + snapshotName));
                    Map<String, Object> snapshotResponseMap;
                    try (InputStream is = response.getEntity().getContent()) {
                        snapshotResponseMap = XContentHelper.convertToMap(XContentType.JSON.xContent(), is, true);
                    }
                    assertThat(snapshotResponseMap.size(), greaterThan(0));
                    final Map<String, Object> metadata = extractMetadata(snapshotResponseMap, snapshotName);
                    assertNotNull(metadata);
                    assertThat(metadata.get("policy"), equalTo(policyName));
                    assertHistoryIsPresent(policyName, true, repoId, CREATE_OPERATION);
                } catch (ResponseException e) {
                    fail("expected snapshot to exist but it does not: " + EntityUtils.toString(e.getResponse().getEntity()));
                }
            });

            // Sleep for up to a second, but at least 1 second since we scheduled the policy so we can
            // ensure it *would* have run if SLM were running
            Thread.sleep(Math.min(0, TimeValue.timeValueSeconds(1).millis() - Math.min(0, System.currentTimeMillis() - start)));

            client().performRequest(new Request("POST", "/_slm/_execute_retention"));

            // Retention and the manually executed policy should still have run,
            // but only the one we manually ran.
            assertBusy(() -> {
                logger.info("--> checking for stats updates...");
                Map<String, Object> stats = getSLMStats();
                Map<String, Object> policyStats = policyStatsAsMap(stats);
                Map<String, Object> policyIdStats = (Map<String, Object>) policyStats.get(policyName);
                int snapsTaken = (int) policyIdStats.get(SnapshotLifecycleStats.SnapshotPolicyStats.SNAPSHOTS_TAKEN.getPreferredName());
                int totalTaken = (int) stats.get(SnapshotLifecycleStats.TOTAL_TAKEN.getPreferredName());
                int totalFailed = (int) stats.get(SnapshotLifecycleStats.TOTAL_FAILED.getPreferredName());
                int totalDeleted = (int) stats.get(SnapshotLifecycleStats.TOTAL_DELETIONS.getPreferredName());
                assertThat(snapsTaken, equalTo(1));
                assertThat(totalTaken, equalTo(1));
                assertThat(totalDeleted, equalTo(1));
                assertThat(totalFailed, equalTo(0));
            });

            assertBusy(() -> {
                try {
                    Map<String, List<Map<?, ?>>> snaps = wipeSnapshots();
                    logger.info("--> checking for wiped snapshots: {}", snaps);
                    assertThat(snaps.size(), equalTo(0));
                } catch (ResponseException e) {
                    logger.error("got exception wiping snapshots", e);
                    fail("got exception: " + EntityUtils.toString(e.getResponse().getEntity()));
                }
            });
        } finally {
            client().performRequest(new Request("POST", "/_slm/start"));

            assertBusy(() -> {
                logger.info("--> waiting for SLM to start");
                assertThat(EntityUtils.toString(client().performRequest(new Request("GET", "/_slm/status")).getEntity()),
                    containsString("RUNNING"));
            });
        }
    }

    @SuppressWarnings("unchecked")
    @TestIssueLogging(value = "org.elasticsearch.xpack.slm:TRACE,org.elasticsearch.xpack.core.slm:TRACE,org.elasticsearch.snapshots:TRACE",
        issueUrl = "https://github.com/elastic/elasticsearch/issues/48017")
    public void testBasicTimeBasedRetention() throws Exception {
        final String indexName = "test";
        final String policyName = "basic-time-policy";
        final String repoId = "time-based-retention-repo";
        int docCount = randomIntBetween(10, 50);
        List<IndexRequestBuilder> indexReqs = new ArrayList<>();
        for (int i = 0; i < docCount; i++) {
            index(client(), indexName, "" + i, "foo", "bar");
        }
        logSLMPolicies();

        // Create a snapshot repo
        initializeRepo(repoId);

        // Create a policy with a retention period of 1 millisecond
        createSnapshotPolicy(policyName, "snap", NEVER_EXECUTE_CRON_SCHEDULE, repoId, indexName, true,
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
                assertHistoryIsPresent(policyName, true, repoId, CREATE_OPERATION);
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
            assertAcked(updateSettingsResp);
        }

        logSLMPolicies();

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
                assertHistoryIsPresent(policyName, true, repoId, DELETE_OPERATION);

                Map<String, Object> stats = getSLMStats();
                Map<String, Object> policyStats = policyStatsAsMap(stats);
                Map<String, Object> policyIdStats = (Map<String, Object>) policyStats.get(policyName);
                int snapsTaken = (int) policyIdStats.get(SnapshotLifecycleStats.SnapshotPolicyStats.SNAPSHOTS_TAKEN.getPreferredName());
                int snapsDeleted = (int) policyIdStats.get(SnapshotLifecycleStats.SnapshotPolicyStats.SNAPSHOTS_DELETED.getPreferredName());
                int retentionRun = (int) stats.get(SnapshotLifecycleStats.RETENTION_RUNS.getPreferredName());
                int totalTaken = (int) stats.get(SnapshotLifecycleStats.TOTAL_TAKEN.getPreferredName());
                int totalDeleted = (int) stats.get(SnapshotLifecycleStats.TOTAL_DELETIONS.getPreferredName());
                assertThat(snapsTaken, greaterThanOrEqualTo(1));
                assertThat(totalTaken, greaterThanOrEqualTo(1));
                assertThat(retentionRun, greaterThanOrEqualTo(1));
                assertThat(snapsDeleted, greaterThanOrEqualTo(1));
                assertThat(totalDeleted, greaterThanOrEqualTo(1));
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

    public void testDataStreams() throws Exception {
        String dataStreamName = "ds-test";
        String repoId = "ds-repo";
        String policyName = "ds-policy";

        String mapping = "{\n" +
            "      \"properties\": {\n" +
            "        \"@timestamp\": {\n" +
            "          \"type\": \"date\"\n" +
            "        }\n" +
            "      }\n" +
            "    }";
        Template template = new Template(null, new CompressedXContent(mapping), null);
        createComposableTemplate(client(), "ds-template", dataStreamName, template);

        client().performRequest(new Request("PUT", "_data_stream/" + dataStreamName));

        // Create a snapshot repo
        initializeRepo(repoId);

        createSnapshotPolicy(policyName, "snap", NEVER_EXECUTE_CRON_SCHEDULE, repoId, dataStreamName, true);

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
                final Map<String, Object> snapshot = extractSnapshot(snapshotResponseMap, snapshotName);
                assertEquals(Collections.singletonList(dataStreamName), snapshot.get("data_streams"));
                assertEquals(Collections.singletonList(DataStream.getDefaultBackingIndexName(dataStreamName, 1)), snapshot.get("indices"));
            } catch (ResponseException e) {
                fail("expected snapshot to exist but it does not: " + EntityUtils.toString(e.getResponse().getEntity()));
            }
        });
    }

    @SuppressWarnings("unchecked")
    public void testSLMXpackInfo() {
        Map<String, Object> features = (Map<String, Object>) getLocation("/_xpack").get("features");
        assertNotNull(features);
        Map<String, Object> slm = (Map<String, Object>) features.get("slm");
        assertNotNull(slm);
        assertTrue((boolean) slm.get("available"));
        assertTrue((boolean) slm.get("enabled"));
    }

    @SuppressWarnings("unchecked")
    public void testSLMXpackUsage() throws Exception {
        Map<String, Object> slm = (Map<String, Object>) getLocation("/_xpack/usage").get("slm");
        assertNotNull(slm);
        assertTrue((boolean) slm.get("available"));
        assertTrue((boolean) slm.get("enabled"));
        assertThat(slm.get("policy_count"), anyOf(equalTo(null), equalTo(0)));

        // Create a snapshot repo
        initializeRepo("repo");
        // Create a policy with a retention period of 1 millisecond
        createSnapshotPolicy("policy", "snap", NEVER_EXECUTE_CRON_SCHEDULE, "repo", "*", true,
            new SnapshotRetentionConfiguration(TimeValue.timeValueMillis(1), null, null));
        final String snapshotName = executePolicy("policy");

        // Check that the executed snapshot is created
        assertBusy(() -> {
            try {
                logger.info("--> checking for snapshot creation...");
                Response response = client().performRequest(new Request("GET", "/_snapshot/repo/" + snapshotName));
                Map<String, Object> snapshotResponseMap;
                try (InputStream is = response.getEntity().getContent()) {
                    snapshotResponseMap = XContentHelper.convertToMap(XContentType.JSON.xContent(), is, true);
                }
                assertThat(snapshotResponseMap.size(), greaterThan(0));
            } catch (ResponseException e) {
                fail("expected snapshot to exist but it does not: " + EntityUtils.toString(e.getResponse().getEntity()));
            }
        });

        // Wait for stats to be updated
        assertBusy(() -> {
            logger.info("--> checking for stats to be updated...");
            Map<String, Object> stats = getSLMStats();
            Map<String, Object> policyStats = policyStatsAsMap(stats);
            Map<String, Object> policyIdStats = (Map<String, Object>) policyStats.get("policy");
            assertNotNull(policyIdStats);
        });

        slm = (Map<String, Object>) getLocation("/_xpack/usage").get("slm");
        assertNotNull(slm);
        assertTrue((boolean) slm.get("available"));
        assertTrue((boolean) slm.get("enabled"));
        assertThat("got: " + slm, slm.get("policy_count"), equalTo(1));
        assertNotNull(slm.get("policy_stats"));
    }

    public Map<String, Object> getLocation(String path) {
        try {
            Response executeRepsonse = client().performRequest(new Request("GET", path));
            try (XContentParser parser = JsonXContent.jsonXContent.createParser(NamedXContentRegistry.EMPTY,
                DeprecationHandler.THROW_UNSUPPORTED_OPERATION, EntityUtils.toByteArray(executeRepsonse.getEntity()))) {
                return parser.map();
            }
        } catch (Exception e) {
            fail("failed to execute GET request to " + path + " - got: " + e);
            throw new RuntimeException(e);
        }
    }

    /**
     * Execute the given policy and return the generated snapshot name
     */
    private String executePolicy(String policyId) {
        try {
            Response executeRepsonse = client().performRequest(new Request("POST", "/_slm/policy/" + policyId + "/_execute"));
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
        return (Map<String, Object>) extractSnapshot(snapshotResponseMap, snapshotPrefix).get("metadata");
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> extractSnapshot(Map<String, Object> snapshotResponseMap, String snapshotPrefix) {
        List<Map<String, Object>> snapResponse = ((List<Map<String, Object>>) snapshotResponseMap.get("snapshots"));
        return snapResponse.stream()
            .filter(snapshot -> ((String) snapshot.get("snapshot")).startsWith(snapshotPrefix))
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
    @SuppressWarnings("unchecked")
    private void assertHistoryIsPresent(String policyName, boolean success, String repository, String operation) throws IOException {
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
            "            \"operation\": \"" + operation + "\"\n" +
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

        // Finally, check that the history index is in a good state
        assertHistoryIndexWaitingForRollover();
    }

    private void assertHistoryIndexWaitingForRollover() throws IOException {
        Step.StepKey stepKey = getStepKeyForIndex(client(), DataStream.getDefaultBackingIndexName(SLM_HISTORY_DATA_STREAM, 1));
        assertEquals("hot", stepKey.getPhase());
        assertEquals(RolloverAction.NAME, stepKey.getAction());
        assertEquals(WaitForRolloverReadyStep.NAME, stepKey.getName());
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
        final Response response = client().performRequest(putLifecycle);
        assertAcked(response);
    }

    private void disableSLMMinimumIntervalValidation() throws IOException {
        ClusterUpdateSettingsRequest req = new ClusterUpdateSettingsRequest();
        req.transientSettings(Settings.builder().put(LifecycleSettings.SLM_MINIMUM_INTERVAL, "0s"));
        try (XContentBuilder builder = jsonBuilder()) {
            req.toXContent(builder, ToXContent.EMPTY_PARAMS);
            Request r = new Request("PUT", "/_cluster/settings");
            r.setJsonEntity(Strings.toString(builder));
            Response updateSettingsResp = client().performRequest(r);
            assertAcked(updateSettingsResp);
        }
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

    @SuppressWarnings("unchecked")
    private static Map<String, Object> policyStatsAsMap(Map<String, Object> stats) {
        return ((List<Map<String, Object>>) stats.get(SnapshotLifecycleStats.POLICY_STATS.getPreferredName()))
            .stream()
            .collect(Collectors.toMap(
                m -> (String) m.get(SnapshotLifecycleStats.SnapshotPolicyStats.POLICY_ID.getPreferredName()),
                Function.identity()));
    }

    private void assertAcked(Response response) throws IOException {
        assertOK(response);
        Map<String, Object> putLifecycleResponseMap;
        try (InputStream is = response.getEntity().getContent()) {
            putLifecycleResponseMap = XContentHelper.convertToMap(XContentType.JSON.xContent(), is, true);
        }
        assertThat(putLifecycleResponseMap.get("acknowledged"), equalTo(true));
    }

    private void logSLMPolicies() throws IOException {
        Request request = new Request("GET" , "/_slm/policy?human");
        Response response = client().performRequest(request);
        assertOK(response);
        logger.info("SLM policies: {}", EntityUtils.toString(response.getEntity()));
    }
}
