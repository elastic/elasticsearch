/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.upgrades;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.Version;
import org.elasticsearch.client.MachineLearningClient;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.ml.CloseJobRequest;
import org.elasticsearch.client.ml.CloseJobResponse;
import org.elasticsearch.client.ml.FlushJobRequest;
import org.elasticsearch.client.ml.FlushJobResponse;
import org.elasticsearch.client.ml.GetJobRequest;
import org.elasticsearch.client.ml.GetJobResponse;
import org.elasticsearch.client.ml.GetJobStatsRequest;
import org.elasticsearch.client.ml.GetModelSnapshotsRequest;
import org.elasticsearch.client.ml.GetModelSnapshotsResponse;
import org.elasticsearch.client.ml.OpenJobRequest;
import org.elasticsearch.client.ml.OpenJobResponse;
import org.elasticsearch.client.ml.PostDataRequest;
import org.elasticsearch.client.ml.PostDataResponse;
import org.elasticsearch.client.ml.PutJobRequest;
import org.elasticsearch.client.ml.PutJobResponse;
import org.elasticsearch.client.ml.RevertModelSnapshotRequest;
import org.elasticsearch.client.ml.UpgradeJobModelSnapshotRequest;
import org.elasticsearch.client.ml.job.config.AnalysisConfig;
import org.elasticsearch.client.ml.job.config.DataDescription;
import org.elasticsearch.client.ml.job.config.Detector;
import org.elasticsearch.client.ml.job.config.Job;
import org.elasticsearch.client.ml.job.process.DataCounts;
import org.elasticsearch.client.ml.job.process.ModelSnapshot;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.test.rest.XPackRestTestConstants;
import org.junit.BeforeClass;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

@SuppressWarnings("removal")
public class MlJobSnapshotUpgradeIT extends AbstractUpgradeTestCase {

    private static final String JOB_ID = "ml-snapshots-upgrade-job";
    // min version in upgraded 7.series is 7.11.0
    private static final Version CPP_COMPATIBILTIY_VERSION = Version.V_7_11_0;

    private static class HLRC extends RestHighLevelClient {
        HLRC(RestClient restClient) {
            super(restClient, RestClient::close, new ArrayList<>());
        }
    }

    private MachineLearningClient hlrc;

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
        List<String> templatesToWaitFor = UPGRADE_FROM_VERSION.onOrAfter(Version.V_7_12_0)
            ? XPackRestTestConstants.ML_POST_V7120_TEMPLATES
            : XPackRestTestConstants.ML_POST_V660_TEMPLATES;
        return Stream.concat(templatesToWaitFor.stream(), super.templatesToWaitFor().stream()).collect(Collectors.toSet());
    }

    protected static void waitForPendingUpgraderTasks() throws Exception {
        waitForPendingTasks(adminClient(), taskName -> taskName.startsWith("xpack/ml/job/snapshot/upgrade") == false);
    }

    /**
     * The purpose of this test is to ensure that when a job is open through a rolling upgrade we upgrade the results
     * index mappings when it is assigned to an upgraded node even if no other ML endpoint is called after the upgrade
     */
    public void testSnapshotUpgrader() throws Exception {
        hlrc = new HLRC(client()).machineLearning();
        Request adjustLoggingLevels = new Request("PUT", "/_cluster/settings");
        adjustLoggingLevels.setJsonEntity("{\"persistent\": {" + "\"logger.org.elasticsearch.xpack.ml\": \"trace\"" + "}}");
        client().performRequest(adjustLoggingLevels);
        switch (CLUSTER_TYPE) {
            case OLD:
                createJobAndSnapshots();
                break;
            case MIXED:
                // Add mixed cluster test after backported
                break;
            case UPGRADED:
                assumeTrue("We should only test if old cluster is before new cluster", UPGRADE_FROM_VERSION.before(Version.CURRENT));
                ensureHealth((request -> {
                    request.addParameter("timeout", "70s");
                    request.addParameter("wait_for_nodes", "3");
                    request.addParameter("wait_for_status", "yellow");
                }));
                testSnapshotUpgrade();
                waitForPendingUpgraderTasks();
                break;
            default:
                throw new UnsupportedOperationException("Unknown cluster type [" + CLUSTER_TYPE + "]");
        }
    }

    private void testSnapshotUpgrade() throws Exception {
        Job job = getJob(JOB_ID).jobs().get(0);
        String currentSnapshot = job.getModelSnapshotId();

        GetModelSnapshotsResponse modelSnapshots = getModelSnapshots(job.getId());
        assertThat(modelSnapshots.snapshots(), hasSize(2));

        ModelSnapshot snapshot = modelSnapshots.snapshots()
            .stream()
            .filter(s -> s.getSnapshotId().equals(currentSnapshot) == false)
            .findFirst()
            .orElseThrow(() -> new ElasticsearchException("Not found snapshot other than " + currentSnapshot));

        // Don't wait for completion in the initial upgrade call, but instead poll for status
        // using the stats endpoint - this mimics what the Kibana upgrade assistant does
        String snapshotToUpgrade = snapshot.getSnapshotId();
        assertThat(
            hlrc.upgradeJobSnapshot(new UpgradeJobModelSnapshotRequest(JOB_ID, snapshotToUpgrade, null, false), RequestOptions.DEFAULT)
                .isCompleted(),
            is(false)
        );

        // Wait for completion by waiting for the persistent task to disappear
        assertBusy(() -> {
            try {
                Response response = client().performRequest(
                    new Request("GET", "_ml/anomaly_detectors/" + JOB_ID + "/model_snapshots/" + snapshotToUpgrade + "/_upgrade/_stats")
                );
                // Doing this instead of using expectThrows() on the line above means we get better diagnostics if the test fails
                fail("Upgrade still in progress: " + entityAsMap(response));
            } catch (ResponseException e) {
                assertThat(e.getResponse().toString(), e.getResponse().getStatusLine().getStatusCode(), is(404));
            }
        }, 30, TimeUnit.SECONDS);

        List<ModelSnapshot> snapshots = getModelSnapshots(job.getId(), snapshotToUpgrade).snapshots();
        assertThat(snapshots, hasSize(1));
        snapshot = snapshots.get(0);
        assertThat(snapshot.getLatestRecordTimeStamp(), equalTo(snapshots.get(0).getLatestRecordTimeStamp()));
        assertThat(snapshot.getMinVersion(), equalTo(CPP_COMPATIBILTIY_VERSION));

        // Does the snapshot still work?
        assertThat(
            hlrc.getJobStats(new GetJobStatsRequest(JOB_ID), RequestOptions.DEFAULT)
                .jobStats()
                .get(0)
                .getDataCounts()
                .getLatestRecordTimeStamp(),
            greaterThan(snapshot.getLatestRecordTimeStamp())
        );
        RevertModelSnapshotRequest revertModelSnapshotRequest = new RevertModelSnapshotRequest(JOB_ID, snapshotToUpgrade);
        revertModelSnapshotRequest.setDeleteInterveningResults(true);
        assertThat(
            hlrc.revertModelSnapshot(revertModelSnapshotRequest, RequestOptions.DEFAULT).getModel().getSnapshotId(),
            equalTo(snapshotToUpgrade)
        );
        assertThat(openJob(JOB_ID).isOpened(), is(true));
        assertThat(
            hlrc.getJobStats(new GetJobStatsRequest(JOB_ID), RequestOptions.DEFAULT)
                .jobStats()
                .get(0)
                .getDataCounts()
                .getLatestRecordTimeStamp(),
            equalTo(snapshot.getLatestRecordTimeStamp())
        );
        closeJob(JOB_ID);
    }

    private void createJobAndSnapshots() throws Exception {
        TimeValue bucketSpan = TimeValue.timeValueHours(1);
        long startTime = 1491004800000L;

        PutJobResponse jobResponse = buildAndPutJob(JOB_ID, bucketSpan);
        Job job = jobResponse.getResponse();
        openJob(job.getId());
        DataCounts dataCounts = postData(
            job.getId(),
            generateData(startTime, bucketSpan, 10, Arrays.asList("foo"), (bucketIndex, series) -> bucketIndex == 5 ? 100.0 : 10.0).stream()
                .collect(Collectors.joining())
        ).getDataCounts();
        assertThat(dataCounts.getInvalidDateCount(), equalTo(0L));
        assertThat(dataCounts.getBucketCount(), greaterThan(0L));
        final long lastCount = dataCounts.getBucketCount();
        flushJob(job.getId());
        closeJob(job.getId());

        // We need to wait a second to ensure the second time around model snapshot will have a different ID (it depends on epoch seconds)
        waitUntil(() -> false, 2, TimeUnit.SECONDS);

        openJob(job.getId());
        dataCounts = postData(
            job.getId(),
            generateData(startTime + 10 * bucketSpan.getMillis(), bucketSpan, 10, Arrays.asList("foo"), (bucketIndex, series) -> 10.0)
                .stream()
                .collect(Collectors.joining())
        ).getDataCounts();
        assertThat(dataCounts.getInvalidDateCount(), equalTo(0L));
        assertThat(dataCounts.getBucketCount(), greaterThan(lastCount));
        flushJob(job.getId());
        closeJob(job.getId());

        GetModelSnapshotsResponse modelSnapshots = getModelSnapshots(job.getId());
        assertThat(modelSnapshots.snapshots(), hasSize(2));
    }

    private PutJobResponse buildAndPutJob(String jobId, TimeValue bucketSpan) throws Exception {
        Detector.Builder detector = new Detector.Builder("mean", "value");
        detector.setPartitionFieldName("series");
        List<Detector> detectors = new ArrayList<>();
        detectors.add(detector.build());
        boolean isCategorization = randomBoolean();
        if (isCategorization) {
            detectors.add(new Detector.Builder("count", null).setByFieldName("mlcategory").build());
        }
        AnalysisConfig.Builder analysisConfig = new AnalysisConfig.Builder(detectors);
        analysisConfig.setBucketSpan(bucketSpan);
        if (isCategorization) {
            analysisConfig.setCategorizationFieldName("text");
        }
        Job.Builder job = new Job.Builder(jobId);
        job.setAnalysisConfig(analysisConfig);
        DataDescription.Builder dataDescription = new DataDescription.Builder();
        job.setDataDescription(dataDescription);
        return putJob(job.build());
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

    protected GetJobResponse getJob(String jobId) throws IOException {
        return hlrc.getJob(new GetJobRequest(jobId), RequestOptions.DEFAULT);
    }

    protected PutJobResponse putJob(Job job) throws IOException {
        return hlrc.putJob(new PutJobRequest(job), RequestOptions.DEFAULT);
    }

    protected OpenJobResponse openJob(String jobId) throws IOException {
        return hlrc.openJob(new OpenJobRequest(jobId), RequestOptions.DEFAULT);
    }

    protected PostDataResponse postData(String jobId, String data) throws IOException {
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
        return hlrc.postData(new PostDataRequest(jobId, XContentType.JSON, new BytesArray(data)), postDataOptions);
    }

    protected FlushJobResponse flushJob(String jobId) throws IOException {
        return hlrc.flushJob(new FlushJobRequest(jobId), RequestOptions.DEFAULT);
    }

    protected CloseJobResponse closeJob(String jobId) throws IOException {
        return hlrc.closeJob(new CloseJobRequest(jobId), RequestOptions.DEFAULT);
    }

    protected GetModelSnapshotsResponse getModelSnapshots(String jobId) throws IOException {
        return getModelSnapshots(jobId, null);
    }

    protected GetModelSnapshotsResponse getModelSnapshots(String jobId, String snapshotId) throws IOException {
        GetModelSnapshotsRequest getModelSnapshotsRequest = new GetModelSnapshotsRequest(jobId);
        getModelSnapshotsRequest.setSnapshotId(snapshotId);
        return hlrc.getModelSnapshots(getModelSnapshotsRequest, RequestOptions.DEFAULT);
    }

    protected static String createJsonRecord(Map<String, Object> keyValueMap) throws IOException {
        return Strings.toString(JsonXContent.contentBuilder().map(keyValueMap)) + "\n";
    }

}
