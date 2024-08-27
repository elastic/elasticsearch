/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.slm;

import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.cluster.metadata.Template;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.junit.annotations.TestIssueLogging;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.core.ilm.LifecycleSettings;
import org.elasticsearch.xpack.core.slm.SnapshotLifecyclePolicy;
import org.elasticsearch.xpack.core.slm.SnapshotLifecycleStats;
import org.elasticsearch.xpack.core.slm.SnapshotRetentionConfiguration;

import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.xpack.slm.history.SnapshotHistoryItem.CREATE_OPERATION;
import static org.elasticsearch.xpack.slm.history.SnapshotHistoryItem.DELETE_OPERATION;
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

    // as we are testing the SLM history entries we'll preserve the "slm-history-ilm-policy" policy as it'll be associated with the
    // .slm-history-* indices and we won't be able to delete it when we wipe out the cluster
    @Override
    protected boolean preserveILMPoliciesUponCompletion() {
        return true;
    }

    public void testMissingRepo() throws Exception {
        SnapshotLifecyclePolicy policy = new SnapshotLifecyclePolicy(
            "missing-repo-policy",
            "snap",
            "0 0/15 * * * ?",
            "missing-repo",
            Collections.emptyMap(),
            SnapshotRetentionConfiguration.EMPTY
        );

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
        for (int i = 0; i < docCount; i++) {
            index(client(), indexName, "" + i, "foo", "bar");
        }

        // Create a snapshot repo
        initializeRepo(repoId);

        // allow arbitrarily frequent slm snapshots
        disableSLMMinimumIntervalValidation();

        var schedule = randomBoolean() ? "*/1 * * * * ?" : "1s";
        createSnapshotPolicy(policyName, "snap", schedule, repoId, indexName, true);

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
        var schedule = randomBoolean() ? "*/1 * * * * ?" : "1s";
        createSnapshotPolicy(policyName, "snap", schedule, repoName, indexPattern, false);

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
    @TestIssueLogging(
        value = "org.elasticsearch.xpack.slm:TRACE,org.elasticsearch.xpack.core.slm:TRACE,org.elasticsearch.snapshots:DEBUG",
        issueUrl = "https://github.com/elastic/elasticsearch/issues/48531"
    )
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

        ResponseException badResp = expectThrows(
            ResponseException.class,
            () -> client().performRequest(new Request("POST", "/_slm/policy/" + policyName + "-bad/_execute"))
        );
        assertThat(
            EntityUtils.toString(badResp.getResponse().getEntity()),
            containsString("no such snapshot lifecycle policy [" + policyName + "-bad]")
        );

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
            assertThat(
                EntityUtils.toString(client().performRequest(new Request("GET", "/_slm/status")).getEntity()),
                containsString("STOPPED")
            );
        });

        try {
            var schedule = randomBoolean() ? "0 0/15 * * * ?" : "15m";
            createSnapshotPolicy(
                policyName,
                "snap",
                schedule,
                repoId,
                indexName,
                true,
                new SnapshotRetentionConfiguration(TimeValue.ZERO, null, null)
            );
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
                    wipeSnapshots();
                } catch (ResponseException e) {
                    logger.error("got exception wiping snapshots", e);
                    fail("got exception: " + EntityUtils.toString(e.getResponse().getEntity()));
                }
            });
        } finally {
            client().performRequest(new Request("POST", "/_slm/start"));

            assertBusy(() -> {
                logger.info("--> waiting for SLM to start");
                assertThat(
                    EntityUtils.toString(client().performRequest(new Request("GET", "/_slm/status")).getEntity()),
                    containsString("RUNNING")
                );
            });
        }
    }

    @SuppressWarnings("unchecked")
    @TestIssueLogging(
        value = "org.elasticsearch.xpack.slm:TRACE,org.elasticsearch.xpack.core.slm:TRACE,org.elasticsearch.snapshots:TRACE",
        issueUrl = "https://github.com/elastic/elasticsearch/issues/48017"
    )
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
        createSnapshotPolicy(
            policyName,
            "snap",
            NEVER_EXECUTE_CRON_SCHEDULE,
            repoId,
            indexName,
            true,
            new SnapshotRetentionConfiguration(TimeValue.timeValueMillis(1), null, null)
        );

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
        }, 60, TimeUnit.SECONDS);

        // Run retention every second
        ClusterUpdateSettingsRequest req = new ClusterUpdateSettingsRequest();
        req.persistentSettings(Settings.builder().put(LifecycleSettings.SLM_RETENTION_SCHEDULE, "*/1 * * * * ?"));
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
            unsetRequest.persistentSettings(Settings.builder().put(LifecycleSettings.SLM_RETENTION_SCHEDULE, (String) null));
            try (XContentBuilder builder = jsonBuilder()) {
                unsetRequest.toXContent(builder, ToXContent.EMPTY_PARAMS);
                Request r = new Request("PUT", "/_cluster/settings");
                r.setJsonEntity(Strings.toString(builder));
                client().performRequest(r);
            }
        }
    }

    @SuppressWarnings("unchecked")
    public void testDataStreams() throws Exception {
        String dataStreamName = "ds-test";
        String repoId = "ds-repo";
        String policyName = "ds-policy";

        String mapping = """
            {
                  "properties": {
                    "@timestamp": {
                      "type": "date"
                    }
                  }
                }""";
        Template template = new Template(null, new CompressedXContent(mapping), null);
        createComposableTemplate(client(), "ds-template", dataStreamName, template);

        client().performRequest(new Request("PUT", "_data_stream/" + dataStreamName));
        /*
         * We make the following request just to get the backing index name (we can't assume we know what it is based on the date because
         * this test could run across midnight on two days.
         */
        Response dataStreamResponse = client().performRequest(new Request("GET", "_data_stream/" + dataStreamName));
        final String dataStreamIndexName;
        try (InputStream is = dataStreamResponse.getEntity().getContent()) {
            Map<String, Object> dataStreamResponseMap = XContentHelper.convertToMap(XContentType.JSON.xContent(), is, true);
            List<Map<String, Object>> dataStreams = (List<Map<String, Object>>) dataStreamResponseMap.get("data_streams");
            assertThat(dataStreams.size(), equalTo(1));
            List<Map<String, String>> indices = (List<Map<String, String>>) dataStreams.get(0).get("indices");
            assertThat(indices.size(), equalTo(1));
            dataStreamIndexName = indices.get(0).get("index_name");
            assertNotNull(dataStreamIndexName);
        }

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
                assertEquals(Collections.singletonList(dataStreamIndexName), snapshot.get("indices"));
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
        createSnapshotPolicy(
            "policy",
            "snap",
            NEVER_EXECUTE_CRON_SCHEDULE,
            "repo",
            "*",
            true,
            new SnapshotRetentionConfiguration(TimeValue.timeValueMillis(1), null, null)
        );
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

    public void testSnapshotRetentionWithMissingRepo() throws Exception {
        // Create two snapshot repositories
        String repo = "test-repo";
        initializeRepo(repo);
        String missingRepo = "missing-repo";
        initializeRepo(missingRepo);

        // Create a policy per repository
        final String indexName = "test";
        final String policyName = "policy-1";
        createSnapshotPolicy(
            policyName,
            "snap",
            NEVER_EXECUTE_CRON_SCHEDULE,
            repo,
            indexName,
            true,
            new SnapshotRetentionConfiguration(TimeValue.timeValueMillis(1), null, null)
        );
        final String policyWithMissingRepo = "policy-2";
        createSnapshotPolicy(
            policyWithMissingRepo,
            "snap",
            NEVER_EXECUTE_CRON_SCHEDULE,
            missingRepo,
            indexName,
            true,
            new SnapshotRetentionConfiguration(TimeValue.timeValueMillis(1), null, null)
        );

        // Delete the repo of one of the policies
        deleteRepository(missingRepo);

        // Manually create a snapshot based on the "correct" policy
        final String snapshotName = executePolicy(policyName);

        // Check that the executed snapshot is created
        assertBusy(() -> {
            try {
                Response response = client().performRequest(new Request("GET", "/_snapshot/" + repo + "/" + snapshotName));
                Map<String, Object> snapshotResponseMap;
                try (InputStream is = response.getEntity().getContent()) {
                    snapshotResponseMap = XContentHelper.convertToMap(XContentType.JSON.xContent(), is, true);
                }
                assertThat(snapshotResponseMap.size(), greaterThan(0));
                final Map<String, Object> metadata = extractMetadata(snapshotResponseMap, snapshotName);
                assertNotNull(metadata);
                assertThat(metadata.get("policy"), equalTo(policyName));
                assertHistoryIsPresent(policyName, true, repo, CREATE_OPERATION);
            } catch (ResponseException e) {
                fail("expected snapshot to exist but it does not: " + EntityUtils.toString(e.getResponse().getEntity()));
            }
        }, 60, TimeUnit.SECONDS);

        execute_retention(client());

        // Check that the snapshot created by the policy has been removed by retention
        assertBusy(() -> {
            try {
                Response response = client().performRequest(new Request("GET", "/_snapshot/" + repo + "/" + snapshotName));
                assertThat(EntityUtils.toString(response.getEntity()), containsString("snapshot_missing_exception"));
            } catch (ResponseException e) {
                assertThat(EntityUtils.toString(e.getResponse().getEntity()), containsString("snapshot_missing_exception"));
            }
            assertHistoryIsPresent(policyName, true, repo, DELETE_OPERATION);
        }, 60, TimeUnit.SECONDS);
    }

    @SuppressWarnings("unchecked")
    public void testGetIntervalSchedule() throws Exception {
        final String indexName = "index-1";
        final String policyName = "policy-1";
        final String repoId = "repo-1";

        initializeRepo(repoId);

        var schedule = "30m";
        var now = Instant.now();
        createSnapshotPolicy(policyName, "snap", schedule, repoId, indexName, true);

        assertBusy(() -> {
            Request getReq = new Request("GET", "/_slm/policy/" + policyName);
            Response policyMetadata = client().performRequest(getReq);
            Map<String, Object> policyResponseMap;
            try (InputStream is = policyMetadata.getEntity().getContent()) {
                policyResponseMap = XContentHelper.convertToMap(XContentType.JSON.xContent(), is, true);
            }

            Map<String, Object> policyMetadataMap = (Map<String, Object>) policyResponseMap.get(policyName);
            Long nextExecutionMillis = (Long) policyMetadataMap.get("next_execution_millis");
            assertNotNull(nextExecutionMillis);

            Instant nextExecution = Instant.ofEpochMilli(nextExecutionMillis);
            assertTrue(nextExecution.isAfter(now.plus(Duration.ofMinutes(29))));
            assertTrue(nextExecution.isBefore(now.plus(Duration.ofMinutes(31))));
        });
    }

    public Map<String, Object> getLocation(String path) {
        try {
            Response executeRepsonse = client().performRequest(new Request("GET", path));
            try (
                XContentParser parser = JsonXContent.jsonXContent.createParser(
                    XContentParserConfiguration.EMPTY,
                    EntityUtils.toByteArray(executeRepsonse.getEntity())
                )
            ) {
                return parser.map();
            }
        } catch (Exception e) {
            fail("failed to execute GET request to " + path + " - got: " + e);
            throw new RuntimeException(e);
        }
    }

    private static void createComposableTemplate(RestClient client, String templateName, String indexPattern, Template template)
        throws IOException {
        XContentBuilder builder = jsonBuilder();
        template.toXContent(builder, ToXContent.EMPTY_PARAMS);
        StringEntity templateJSON = new StringEntity(String.format(Locale.ROOT, """
            {
              "index_patterns": "%s",
              "data_stream": {},
              "template": %s
            }""", indexPattern, Strings.toString(builder)), ContentType.APPLICATION_JSON);
        Request createIndexTemplateRequest = new Request("PUT", "_index_template/" + templateName);
        createIndexTemplateRequest.setEntity(templateJSON);
        client.performRequest(createIndexTemplateRequest);
    }

    /**
     * Execute the given policy and return the generated snapshot name
     */
    private String executePolicy(String policyId) {
        try {
            Response executeRepsonse = client().performRequest(new Request("POST", "/_slm/policy/" + policyId + "/_execute"));
            try (
                XContentParser parser = JsonXContent.jsonXContent.createParser(
                    XContentParserConfiguration.EMPTY,
                    EntityUtils.toByteArray(executeRepsonse.getEntity())
                )
            ) {
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
        historySearchRequest.setJsonEntity(Strings.format("""
            {
              "query": {
                "bool": {
                  "must": [
                    {
                      "term": {
                        "policy": "%s"
                      }
                    },
                    {
                      "term": {
                        "success": %s
                      }
                    },
                    {
                      "term": {
                        "repository": "%s"
                      }
                    },
                    {
                      "term": {
                        "operation": "%s"
                      }
                    }
                  ]
                }
              }
            }""", policyName, success, repository, operation));
        Response historyResponse;
        try {
            historyResponse = client().performRequest(historySearchRequest);
            Map<String, Object> historyResponseMap;
            try (InputStream is = historyResponse.getEntity().getContent()) {
                historyResponseMap = XContentHelper.convertToMap(XContentType.JSON.xContent(), is, true);
            }
            assertThat(
                (int) ((Map<String, Object>) ((Map<String, Object>) historyResponseMap.get("hits")).get("total")).get("value"),
                greaterThanOrEqualTo(1)
            );
        } catch (ResponseException e) {
            // Throw AssertionError instead of an exception if the search fails so that assertBusy works as expected
            logger.error(e);
            fail("failed to perform search:" + e.getMessage());
        }

    }

    private void createSnapshotPolicy(
        String policyName,
        String snapshotNamePattern,
        String schedule,
        String repoId,
        String indexPattern,
        boolean ignoreUnavailable
    ) throws IOException {
        createSnapshotPolicy(
            policyName,
            snapshotNamePattern,
            schedule,
            repoId,
            indexPattern,
            ignoreUnavailable,
            SnapshotRetentionConfiguration.EMPTY
        );
    }

    private void createSnapshotPolicy(
        String policyName,
        String snapshotNamePattern,
        String schedule,
        String repoId,
        String indexPattern,
        boolean ignoreUnavailable,
        SnapshotRetentionConfiguration retention
    ) throws IOException {
        Map<String, Object> snapConfig = new HashMap<>();
        snapConfig.put("indices", Collections.singletonList(indexPattern));
        snapConfig.put("ignore_unavailable", ignoreUnavailable);
        if (randomBoolean()) {
            Map<String, Object> metadata = new HashMap<>();
            int fieldCount = randomIntBetween(2, 5);
            for (int i = 0; i < fieldCount; i++) {
                metadata.put(
                    randomValueOtherThanMany(key -> "policy".equals(key) || metadata.containsKey(key), () -> randomAlphaOfLength(5)),
                    randomAlphaOfLength(4)
                );
            }
        }
        SnapshotLifecyclePolicy policy = new SnapshotLifecyclePolicy(
            policyName,
            snapshotNamePattern,
            schedule,
            repoId,
            snapConfig,
            retention
        );

        Request putLifecycle = new Request("PUT", "/_slm/policy/" + policyName);
        XContentBuilder lifecycleBuilder = JsonXContent.contentBuilder();
        policy.toXContent(lifecycleBuilder, ToXContent.EMPTY_PARAMS);
        putLifecycle.setJsonEntity(Strings.toString(lifecycleBuilder));
        final Response response = client().performRequest(putLifecycle);
        assertAcked(response);
    }

    private void disableSLMMinimumIntervalValidation() throws IOException {
        ClusterUpdateSettingsRequest req = new ClusterUpdateSettingsRequest();
        req.persistentSettings(Settings.builder().put(LifecycleSettings.SLM_MINIMUM_INTERVAL, "0s"));
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
        request.setJsonEntity(
            Strings.toString(
                JsonXContent.contentBuilder()
                    .startObject()
                    .field("type", "fs")
                    .startObject("settings")
                    .field("compress", randomBoolean())
                    .field("location", System.getProperty("tests.path.repo"))
                    .field("max_snapshot_bytes_per_sec", maxBytesPerSecond)
                    .endObject()
                    .endObject()
            )
        );
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

    private static void execute_retention(RestClient client) throws IOException {
        final Request request = new Request("POST", "/_slm/_execute_retention");
        assertOK(client.performRequest(request));
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> policyStatsAsMap(Map<String, Object> stats) {
        return ((List<Map<String, Object>>) stats.get(SnapshotLifecycleStats.POLICY_STATS.getPreferredName())).stream()
            .collect(
                Collectors.toMap(
                    m -> (String) m.get(SnapshotLifecycleStats.SnapshotPolicyStats.POLICY_ID.getPreferredName()),
                    Function.identity()
                )
            );
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
        Request request = new Request("GET", "/_slm/policy?human");
        Response response = client().performRequest(request);
        assertOK(response);
        logger.info("SLM policies: {}", EntityUtils.toString(response.getEntity()));
    }
}
