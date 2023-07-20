/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.upgrades;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.Version;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.test.rest.XPackRestTestConstants;
import org.junit.BeforeClass;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

@SuppressWarnings("removal")
public class MlJobSnapshotUpgradeIT extends AbstractUpgradeTestCase {

    private static final String JOB_ID = "ml-snapshots-upgrade-job";

    @BeforeClass
    public static void maybeSkip() {
        assumeFalse("Skip ML tests on unsupported glibc versions", SKIP_ML_TESTS);
    }

    @Override
    protected Collection<String> templatesToWaitFor() {
        // We shouldn't wait for ML templates during the upgrade - production won't
        if (CLUSTER_TYPE != ClusterType.OLD) {
            return super.templatesToWaitFor();
        }
        return Stream.concat(XPackRestTestConstants.ML_POST_V7120_TEMPLATES.stream(), super.templatesToWaitFor().stream())
            .collect(Collectors.toSet());
    }

    protected static void waitForPendingUpgraderTasks() throws Exception {
        waitForPendingTasks(adminClient(), taskName -> taskName.startsWith("xpack/ml/job/snapshot/upgrade") == false);
    }

    /**
     * The purpose of this test is to ensure that when a job is open through a rolling upgrade we upgrade the results
     * index mappings when it is assigned to an upgraded node even if no other ML endpoint is called after the upgrade
     */
    public void testSnapshotUpgrader() throws Exception {
        Request adjustLoggingLevels = new Request("PUT", "/_cluster/settings");
        adjustLoggingLevels.setJsonEntity("""
            {"persistent": {"logger.org.elasticsearch.xpack.ml": "trace"}}""");
        client().performRequest(adjustLoggingLevels);
        switch (CLUSTER_TYPE) {
            case OLD -> createJobAndSnapshots();
            case MIXED -> {
                assumeTrue("We should only test if old cluster is before new cluster", UPGRADE_FROM_VERSION.before(Version.CURRENT));
                ensureHealth((request -> {
                    request.addParameter("timeout", "70s");
                    request.addParameter("wait_for_nodes", "3");
                    request.addParameter("wait_for_status", "yellow");
                }));
                testSnapshotUpgradeFailsOnMixedCluster();
            }
            case UPGRADED -> {
                assumeTrue("We should only test if old cluster is before new cluster", UPGRADE_FROM_VERSION.before(Version.CURRENT));
                ensureHealth((request -> {
                    request.addParameter("timeout", "70s");
                    request.addParameter("wait_for_nodes", "3");
                    request.addParameter("wait_for_status", "yellow");
                }));
                testSnapshotUpgrade();
                waitForPendingUpgraderTasks();
            }
            default -> throw new UnsupportedOperationException("Unknown cluster type [" + CLUSTER_TYPE + "]");
        }
    }

    @SuppressWarnings("unchecked")
    private void testSnapshotUpgradeFailsOnMixedCluster() throws Exception {
        Map<String, Object> jobs = entityAsMap(getJob(JOB_ID));

        String currentSnapshot = ((List<String>) XContentMapValues.extractValue("jobs.model_snapshot_id", jobs)).get(0);
        Response getResponse = getModelSnapshots(JOB_ID);
        List<Map<String, Object>> snapshots = (List<Map<String, Object>>) entityAsMap(getResponse).get("model_snapshots");
        assertThat(snapshots, hasSize(2));

        Map<String, Object> snapshot = snapshots.stream()
            .filter(s -> s.get("snapshot_id").equals(currentSnapshot) == false)
            .findFirst()
            .orElseThrow(() -> new ElasticsearchException("Not found snapshot other than " + currentSnapshot));

        Exception ex = expectThrows(Exception.class, () -> upgradeJobSnapshot(JOB_ID, (String) snapshot.get("snapshot_id"), true));
        assertThat(ex.getMessage(), containsString("Cannot upgrade job"));
    }

    @SuppressWarnings("unchecked")
    private void testSnapshotUpgrade() throws Exception {
        Map<String, Object> jobs = entityAsMap(getJob(JOB_ID));
        String currentSnapshotId = ((List<String>) XContentMapValues.extractValue("jobs.model_snapshot_id", jobs)).get(0);

        Response getSnapshotsResponse = getModelSnapshots(JOB_ID);
        List<Map<String, Object>> snapshots = (List<Map<String, Object>>) entityAsMap(getSnapshotsResponse).get("model_snapshots");
        assertThat(snapshots, hasSize(2));
        assertThat(Integer.parseInt(snapshots.get(0).get("min_version").toString(), 0, 1, 10), equalTo((int) UPGRADE_FROM_VERSION.major));
        assertThat(Integer.parseInt(snapshots.get(1).get("min_version").toString(), 0, 1, 10), equalTo((int) UPGRADE_FROM_VERSION.major));

        Map<String, Object> snapshotToUpgrade = snapshots.stream()
            .filter(s -> s.get("snapshot_id").equals(currentSnapshotId) == false)
            .findFirst()
            .orElseThrow(() -> new ElasticsearchException("Not found snapshot other than " + currentSnapshotId));

        // Don't wait for completion in the initial upgrade call, but instead poll for status
        // using the stats endpoint - this mimics what the Kibana upgrade assistant does
        String snapshotToUpgradeId = (String) snapshotToUpgrade.get("snapshot_id");
        Map<String, Object> upgradeResponse = entityAsMap(upgradeJobSnapshot(JOB_ID, snapshotToUpgradeId, false));
        assertFalse((boolean) upgradeResponse.get("completed"));

        // Wait for completion by waiting for the persistent task to disappear
        assertBusy(() -> {
            try {
                Response response = client().performRequest(
                    new Request("GET", "_ml/anomaly_detectors/" + JOB_ID + "/model_snapshots/" + snapshotToUpgradeId + "/_upgrade/_stats")
                );
                // Doing this instead of using expectThrows() on the line above means we get better diagnostics if the test fails
                fail("Upgrade still in progress: " + entityAsMap(response));
            } catch (ResponseException e) {
                assertThat(e.getResponse().toString(), e.getResponse().getStatusLine().getStatusCode(), is(404));
            }
        }, 30, TimeUnit.SECONDS);

        List<Map<String, Object>> upgradedSnapshot = (List<Map<String, Object>>) entityAsMap(getModelSnapshots(JOB_ID, snapshotToUpgradeId))
            .get("model_snapshots");
        assertThat(upgradedSnapshot, hasSize(1));
        assertThat(upgradedSnapshot.get(0).get("latest_record_time_stamp"), equalTo(snapshotToUpgrade.get("latest_record_time_stamp")));

        // Does the snapshot still work?
        var stats = entityAsMap(getJobStats(JOB_ID));
        List<Map<String, Object>> jobStats = (List<Map<String, Object>>) XContentMapValues.extractValue("jobs", stats);
        assertThat(
            (long) XContentMapValues.extractValue("data_counts.latest_record_timestamp", jobStats.get(0)),
            greaterThan((long) snapshotToUpgrade.get("latest_record_time_stamp"))
        );

        var revertResponse = entityAsMap(revertModelSnapshot(JOB_ID, snapshotToUpgradeId, true));
        assertThat((String) XContentMapValues.extractValue("model.snapshot_id", revertResponse), equalTo(snapshotToUpgradeId));
        assertThat(entityAsMap(openJob(JOB_ID)).get("opened"), is(true));

        stats = entityAsMap(getJobStats(JOB_ID));
        jobStats = (List<Map<String, Object>>) XContentMapValues.extractValue("jobs", stats);
        assertThat(
            (long) XContentMapValues.extractValue("data_counts.latest_record_timestamp", jobStats.get(0)),
            equalTo((long) upgradedSnapshot.get(0).get("latest_record_time_stamp"))
        );
        closeJob(JOB_ID);
    }

    @SuppressWarnings("unchecked")
    private void createJobAndSnapshots() throws Exception {
        TimeValue bucketSpan = TimeValue.timeValueHours(1);
        long startTime = 1491004800000L;

        buildAndPutJob(JOB_ID, bucketSpan);
        openJob(JOB_ID);
        var dataCounts = entityAsMap(
            postData(
                JOB_ID,
                String.join(
                    "",
                    generateData(
                        startTime,
                        bucketSpan,
                        10,
                        Collections.singletonList("foo"),
                        (bucketIndex, series) -> bucketIndex == 5 ? 100.0 : 10.0
                    )
                )
            )
        );

        assertThat((Integer) dataCounts.get("invalid_date_count"), equalTo(0));
        assertThat((Integer) dataCounts.get("bucket_count"), greaterThan(0));
        final int lastCount = (Integer) dataCounts.get("bucket_count");
        flushJob(JOB_ID);
        closeJob(JOB_ID);

        // We need to wait a second to ensure the second time around model snapshot will have a different ID (it depends on epoch seconds)
        waitUntil(() -> false, 2, TimeUnit.SECONDS);

        openJob(JOB_ID);
        dataCounts = entityAsMap(
            postData(
                JOB_ID,
                String.join(
                    "",
                    generateData(
                        startTime + 10 * bucketSpan.getMillis(),
                        bucketSpan,
                        10,
                        Collections.singletonList("foo"),
                        (bucketIndex, series) -> 10.0
                    )
                )
            )
        );
        assertThat((Integer) dataCounts.get("invalid_date_count"), equalTo(0));
        assertThat((Integer) dataCounts.get("bucket_count"), greaterThan(lastCount));
        flushJob(JOB_ID);
        closeJob(JOB_ID);

        var modelSnapshots = entityAsMap(getModelSnapshots(JOB_ID));
        var snapshots = (List<Map<String, Object>>) modelSnapshots.get("model_snapshots");
        assertThat(snapshots, hasSize(2));
        assertThat(Integer.parseInt(snapshots.get(0).get("min_version").toString(), 0, 1, 10), equalTo((int) UPGRADE_FROM_VERSION.major));
        assertThat(Integer.parseInt(snapshots.get(1).get("min_version").toString(), 0, 1, 10), equalTo((int) UPGRADE_FROM_VERSION.major));
    }

    private Response buildAndPutJob(String jobId, TimeValue bucketSpan) throws Exception {
        boolean isCategorization = randomBoolean();
        String jobConfig;

        if (isCategorization) {
            jobConfig = """
                {
                    "analysis_config" : {
                        "bucket_span":""" + "\"" + bucketSpan + "\"," + """
                        "detectors":[{"function":"mean", "field_name":"value", "partition_field_name":"series"},
                        {"function":"count", "by_field_name":"mlcategory"}],
                        "categorization_field_name":"text"
                    },
                    "data_description" : {
                    }
                }""";
        } else {
            jobConfig = """
                {
                    "analysis_config" : {
                        "bucket_span":""" + "\"" + bucketSpan + "\"," + """
                        "detectors":[{"function":"mean", "field_name":"value", "partition_field_name":"series"}]
                    },
                    "data_description" : {
                    }
                }""";
        }
        Request request = new Request("PUT", "/_ml/anomaly_detectors/" + jobId);
        request.setJsonEntity(jobConfig);
        return client().performRequest(request);
    }

    private static List<String> generateData(
        long timestamp,
        TimeValue bucketSpan,
        int bucketCount,
        List<String> series,
        BiFunction<Integer, String, Double> timeAndSeriesToValueFunction
    ) throws IOException {
        List<String> data = new ArrayList<>();
        long now = timestamp;
        for (int i = 0; i < bucketCount; i++) {
            for (String field : series) {
                Map<String, Object> record = new HashMap<>();
                record.put("time", now);
                record.put("value", timeAndSeriesToValueFunction.apply(i, field));
                record.put("text", randomFrom("foo has landed 3", "bar has landed 5", "bar has finished 2", "foo has finished 10"));
                record.put("series", field);
                data.add(createJsonRecord(record));

                record = new HashMap<>();
                record.put("time", now + bucketSpan.getMillis() / 2);
                record.put("value", timeAndSeriesToValueFunction.apply(i, field));
                record.put("series", field);
                data.add(createJsonRecord(record));
            }
            now += bucketSpan.getMillis();
        }
        return data;
    }

    protected Response getJob(String jobId) throws IOException {
        return client().performRequest(new Request("GET", "/_ml/anomaly_detectors/" + jobId));
    }

    protected Response getJobStats(String jobId) throws IOException {
        return client().performRequest(new Request("GET", "/_ml/anomaly_detectors/" + jobId + "/_stats"));
    }

    protected Response openJob(String jobId) throws IOException {
        return client().performRequest(new Request("POST", "/_ml/anomaly_detectors/" + jobId + "/_open"));
    }

    protected Response postData(String jobId, String data) throws IOException {
        // Post data is deprecated, so a deprecation warning is possible (depending on the old version)
        RequestOptions postDataOptions = RequestOptions.DEFAULT.toBuilder().setWarningsHandler(warnings -> {
            if (warnings.isEmpty()) {
                // No warning is OK - it means we hit an old node where post data is not deprecated
                return false;
            } else if (warnings.size() > 1) {
                return true;
            }
            return warnings.get(0)
                .equals(
                    "Posting data directly to anomaly detection jobs is deprecated, "
                        + "in a future major version it will be compulsory to use a datafeed"
                ) == false;
        }).build();

        Request postDataRequest = new Request("POST", "/_ml/anomaly_detectors/" + jobId + "/_data");
        // Post data is deprecated, so expect a deprecation warning
        postDataRequest.setOptions(postDataOptions);
        postDataRequest.setJsonEntity(data);
        return client().performRequest(postDataRequest);
    }

    protected void flushJob(String jobId) throws IOException {
        client().performRequest(new Request("POST", "/_ml/anomaly_detectors/" + jobId + "/_flush"));
    }

    private void closeJob(String jobId) throws IOException {
        Response closeResponse = client().performRequest(new Request("POST", "/_ml/anomaly_detectors/" + jobId + "/_close"));
        assertThat(entityAsMap(closeResponse), hasEntry("closed", true));
    }

    protected Response getModelSnapshots(String jobId) throws IOException {
        return getModelSnapshots(jobId, null);
    }

    protected Response getModelSnapshots(String jobId, String snapshotId) throws IOException {
        String url = "_ml/anomaly_detectors/" + jobId + "/model_snapshots/";
        if (snapshotId != null) {
            url = url + snapshotId;
        }
        return client().performRequest(new Request("GET", url));
    }

    private Response revertModelSnapshot(String jobId, String snapshotId, boolean deleteIntervening) throws IOException {
        String url = "_ml/anomaly_detectors/" + jobId + "/model_snapshots/" + snapshotId + "/_revert";

        if (deleteIntervening) {
            url = url + "?delete_intervening_results=true";
        }
        Request request = new Request("POST", url);
        return client().performRequest(request);
    }

    private Response upgradeJobSnapshot(String jobId, String snapshotId, boolean waitForCompletion) throws IOException {
        String url = "_ml/anomaly_detectors/" + jobId + "/model_snapshots/" + snapshotId + "/_upgrade";
        if (waitForCompletion) {
            url = url + "?wait_for_completion=true";
        }
        Request request = new Request("POST", url);
        return client().performRequest(request);
    }

    protected static String createJsonRecord(Map<String, Object> keyValueMap) throws IOException {
        return Strings.toString(JsonXContent.contentBuilder().map(keyValueMap)) + "\n";
    }

}
