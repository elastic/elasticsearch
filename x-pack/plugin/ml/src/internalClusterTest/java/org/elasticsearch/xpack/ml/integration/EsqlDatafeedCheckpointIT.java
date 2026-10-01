/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.integration;

import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.internal.OriginSettingClient;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.routing.OperationRouting;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.cluster.service.ClusterApplierService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.cluster.service.MasterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.indices.TestIndexNameExpressionResolver;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.ml.action.PutJobAction;
import org.elasticsearch.xpack.core.ml.action.ResetJobAction;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedConfig;
import org.elasticsearch.xpack.core.ml.datafeed.EsqlDatafeedSourceCheckpoint;
import org.elasticsearch.xpack.core.ml.job.config.AnalysisConfig;
import org.elasticsearch.xpack.core.ml.job.config.DataDescription;
import org.elasticsearch.xpack.core.ml.job.config.Detector;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.core.ml.job.persistence.AnomalyDetectorsIndex;
import org.elasticsearch.xpack.core.ml.job.results.Bucket;
import org.elasticsearch.xpack.core.ml.job.results.Result;
import org.elasticsearch.xpack.ml.MlAssignmentNotifier;
import org.elasticsearch.xpack.ml.MlDailyMaintenanceService;
import org.elasticsearch.xpack.ml.MlSingleNodeTestCase;
import org.elasticsearch.xpack.ml.datafeed.DatafeedContextProvider;
import org.elasticsearch.xpack.ml.inference.ingest.InferenceProcessor;
import org.elasticsearch.xpack.ml.job.persistence.JobResultsPersister;
import org.elasticsearch.xpack.ml.job.persistence.JobResultsProvider;
import org.elasticsearch.xpack.ml.job.retention.ExpiredResultsRemover;
import org.elasticsearch.xpack.ml.notifications.AnomalyDetectionAuditor;
import org.elasticsearch.xpack.ml.utils.persistence.ResultsPersisterService;
import org.junit.Before;

import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.Locale;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Mockito.mock;

public class EsqlDatafeedCheckpointIT extends MlSingleNodeTestCase {

    private OriginSettingClient originClient;
    private JobResultsPersister resultsPersister;
    private JobResultsProvider resultsProvider;
    private ThreadPool threadPool;

    @Before
    public void setUpComponents() throws Exception {
        waitForMlTemplates();
        Settings.Builder settingsBuilder = Settings.builder()
            .put(UnassignedInfo.INDEX_DELAYED_NODE_LEFT_TIMEOUT_SETTING.getKey(), TimeValue.timeValueSeconds(1));
        threadPool = mockThreadPool();
        originClient = new OriginSettingClient(client(), ClientHelper.ML_ORIGIN);
        ClusterSettings clusterSettings = new ClusterSettings(
            settingsBuilder.build(),
            new HashSet<>(
                Arrays.asList(
                    InferenceProcessor.MAX_INFERENCE_PROCESSORS,
                    MasterService.MASTER_SERVICE_SLOW_TASK_LOGGING_THRESHOLD_SETTING,
                    OperationRouting.USE_ADAPTIVE_REPLICA_SELECTION_SETTING,
                    ResultsPersisterService.PERSIST_RESULTS_MAX_RETRIES,
                    ClusterService.USER_DEFINED_METADATA,
                    ClusterApplierService.CLUSTER_SERVICE_SLOW_TASK_LOGGING_THRESHOLD_SETTING,
                    ClusterApplierService.CLUSTER_SERVICE_SLOW_TASK_THREAD_DUMP_TIMEOUT_SETTING,
                    ClusterApplierService.CLUSTER_APPLIER_THREAD_WATCHDOG_INTERVAL,
                    ClusterApplierService.CLUSTER_APPLIER_THREAD_WATCHDOG_QUIET_TIME
                )
            )
        );
        ClusterService clusterService = new ClusterService(settingsBuilder.build(), clusterSettings, threadPool, null);
        ResultsPersisterService resultsPersisterService = new ResultsPersisterService(
            threadPool,
            originClient,
            clusterService,
            settingsBuilder.build()
        );
        resultsPersister = new JobResultsPersister(originClient, resultsPersisterService);
        resultsProvider = new JobResultsProvider(client(), Settings.EMPTY, TestIndexNameExpressionResolver.newInstance());
    }

    public void testPersistedCheckpointShouldSurviveRetentionSweep() throws Exception {
        String jobId = createJob();
        String datafeedId = datafeedIdFor(jobId);
        EsqlDatafeedSourceCheckpoint checkpoint = persistCheckpoint(jobId, datafeedId, 3_600_000L, WriteRequest.RefreshPolicy.IMMEDIATE);
        long now = System.currentTimeMillis();
        indexBucket(jobId, now - TimeValue.timeValueDays(2).millis());
        indexBucket(jobId, now);

        runExpiredResultsRemover(jobId, 1);

        assertCheckpointLoaded(jobId, checkpoint.getSourceEndMs(), checkpoint.getFingerprint());
        assertDocExists(AnomalyDetectorsIndex.jobResultsAliasedName(jobId), EsqlDatafeedSourceCheckpoint.documentId(jobId));
    }

    public void testPersistedCheckpointShouldBeReadableWithoutRefresh() throws Exception {
        String jobId = createJob();
        String datafeedId = datafeedIdFor(jobId);
        EsqlDatafeedSourceCheckpoint checkpoint = persistCheckpoint(jobId, datafeedId, 3_600_000L, WriteRequest.RefreshPolicy.NONE);
        assertCheckpointLoaded(jobId, checkpoint.getSourceEndMs(), checkpoint.getFingerprint());
    }

    public void testResetJobShouldClearCheckpoint() throws Exception {
        String jobId = createJob();
        String datafeedId = datafeedIdFor(jobId);
        persistCheckpoint(jobId, datafeedId, 3_600_000L, WriteRequest.RefreshPolicy.IMMEDIATE);
        client().execute(ResetJobAction.INSTANCE, new ResetJobAction.Request(jobId)).actionGet();

        assertBusy(() -> {
            refreshResultsIndex(jobId);
            GetResponse getResponse = client().get(
                new GetRequest(AnomalyDetectorsIndex.jobResultsAliasedName(jobId), EsqlDatafeedSourceCheckpoint.documentId(jobId))
            ).actionGet();
            assertThat(getResponse.isExists(), equalTo(false));
        }, 30, TimeUnit.SECONDS);

        AtomicReference<EsqlDatafeedSourceCheckpoint> loaded = new AtomicReference<>();
        PlainActionFuture<Void> future = new PlainActionFuture<>();
        resultsProvider.esqlDatafeedSourceCheckpoint(jobId, checkpoint -> {
            loaded.set(checkpoint);
            future.onResponse(null);
        }, future::onFailure);
        future.actionGet();
        assertThat(loaded.get(), nullValue());
    }

    public void testFingerprintMismatchShouldNotReusePersistedCheckpoint() {
        String jobId = createJob();
        String datafeedId = datafeedIdFor(jobId);
        EsqlDatafeedSourceCheckpoint checkpoint = new EsqlDatafeedSourceCheckpoint(jobId, datafeedId, 3_600_000L, "mismatch");
        Job job = buildEsqlJob(jobId);
        DatafeedConfig datafeed = buildEsqlDatafeed(datafeedId, jobId);
        assertThat(DatafeedContextProvider.validateLoadedCheckpoint(datafeed, job, checkpoint), nullValue());
    }

    public void testCheckpointShouldBeLoadedAfterResultsIndexRollover() throws Exception {
        String jobId = createJob();
        String datafeedId = datafeedIdFor(jobId);
        EsqlDatafeedSourceCheckpoint checkpoint = persistCheckpoint(jobId, datafeedId, 3_600_000L, WriteRequest.RefreshPolicy.IMMEDIATE);

        rolloverResultsIndex(jobId);

        assertCheckpointLoaded(jobId, checkpoint.getSourceEndMs(), checkpoint.getFingerprint());
    }

    public void testNewestCheckpointShouldBeLoadedAfterRepeatedRollovers() throws Exception {
        String jobId = createJob();
        String datafeedId = datafeedIdFor(jobId);
        // The two values differ in digit count so that a lexicographic comparison would pick the older one.
        persistCheckpoint(jobId, datafeedId, 9_000_000L, WriteRequest.RefreshPolicy.IMMEDIATE);
        rolloverResultsIndex(jobId);
        EsqlDatafeedSourceCheckpoint newest = persistCheckpoint(jobId, datafeedId, 10_800_000L, WriteRequest.RefreshPolicy.IMMEDIATE);
        rolloverResultsIndex(jobId);

        assertCheckpointLoaded(jobId, newest.getSourceEndMs(), newest.getFingerprint());
    }

    public void testCheckpointWrittenAfterRolloverShouldBeReadableWithoutRefresh() throws Exception {
        String jobId = createJob();
        String datafeedId = datafeedIdFor(jobId);
        persistCheckpoint(jobId, datafeedId, 9_000_000L, WriteRequest.RefreshPolicy.IMMEDIATE);
        rolloverResultsIndex(jobId);
        EsqlDatafeedSourceCheckpoint newest = persistCheckpoint(jobId, datafeedId, 10_800_000L, WriteRequest.RefreshPolicy.NONE);

        assertCheckpointLoaded(jobId, newest.getSourceEndMs(), newest.getFingerprint());
    }

    /**
     * Roll the results indices over the way the nightly maintenance task does: the write alias moves to the new
     * index while the read alias spans the old and the new index.
     */
    private void rolloverResultsIndex(String jobId) {
        MlDailyMaintenanceService maintenanceService = new MlDailyMaintenanceService(
            Settings.EMPTY,
            ClusterName.DEFAULT,
            threadPool,
            client(),
            getInstanceFromNode(ClusterService.class),
            mock(AnomalyDetectionAuditor.class),
            mock(MlAssignmentNotifier.class),
            TestIndexNameExpressionResolver.newInstance(),
            true,
            false,
            false,
            false
        );
        // zero max size makes the rollover unconditional
        maintenanceService.setRolloverMaxSize(ByteSizeValue.ZERO);
        PlainActionFuture<AcknowledgedResponse> future = new PlainActionFuture<>();
        maintenanceService.triggerRollResultsIndicesIfNecessaryTask(future);
        assertTrue(future.actionGet().isAcknowledged());
        assertThat(
            client().admin()
                .indices()
                .prepareGetIndex(TEST_REQUEST_TIMEOUT)
                .setIndices(AnomalyDetectorsIndex.jobResultsAliasedName(jobId))
                .get()
                .getIndices().length,
            greaterThan(1)
        );
    }

    private String createJob() {
        String jobId = "esql-cp-" + randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        Job.Builder jobBuilder = new Job.Builder(jobId);
        jobBuilder.setAnalysisConfig(buildEsqlAnalysisConfig());
        jobBuilder.setDataDescription(new DataDescription.Builder().setTimeField("time").setTimeFormat(DataDescription.EPOCH_MS));
        client().execute(PutJobAction.INSTANCE, new PutJobAction.Request(jobBuilder)).actionGet();
        return jobId;
    }

    private static String datafeedIdFor(String jobId) {
        return jobId + "-df";
    }

    private EsqlDatafeedSourceCheckpoint persistCheckpoint(
        String jobId,
        String datafeedId,
        long sourceEndMs,
        WriteRequest.RefreshPolicy refreshPolicy
    ) {
        Job job = buildEsqlJob(jobId);
        DatafeedConfig datafeed = buildEsqlDatafeed(datafeedId, jobId);
        String fingerprint = EsqlDatafeedSourceCheckpoint.computeFingerprint(datafeed, job.getDataDescription().getTimeField());
        EsqlDatafeedSourceCheckpoint checkpoint = new EsqlDatafeedSourceCheckpoint(jobId, datafeedId, sourceEndMs, fingerprint);
        resultsPersister.persistEsqlDatafeedSourceCheckpoint(checkpoint, refreshPolicy).actionGet();
        if (refreshPolicy != WriteRequest.RefreshPolicy.NONE) {
            refreshResultsIndex(jobId);
        }
        return checkpoint;
    }

    private void assertCheckpointLoaded(String jobId, long sourceEndMs, String fingerprint) throws Exception {
        AtomicReference<EsqlDatafeedSourceCheckpoint> loaded = new AtomicReference<>();
        PlainActionFuture<Void> future = new PlainActionFuture<>();
        resultsProvider.esqlDatafeedSourceCheckpoint(jobId, checkpoint -> {
            loaded.set(checkpoint);
            future.onResponse(null);
        }, future::onFailure);
        future.actionGet();
        assertThat(loaded.get(), notNullValue());
        assertThat(loaded.get().getSourceEndMs(), equalTo(sourceEndMs));
        assertThat(loaded.get().getFingerprint(), equalTo(fingerprint));
    }

    private void indexBucket(String jobId, long timestampMs) throws Exception {
        Bucket bucket = new Bucket(jobId, new Date(timestampMs), 60_000);
        bucket.setEventCount(1);
        resultsPersister.bulkPersisterBuilder(jobId).persistBucket(bucket).executeRequest();
        refreshResultsIndex(jobId);
    }

    private void runExpiredResultsRemover(String jobId, long retentionDays) {
        Job.Builder jobBuilder = new Job.Builder(jobId);
        jobBuilder.setAnalysisConfig(buildEsqlAnalysisConfig());
        jobBuilder.setDataDescription(new DataDescription.Builder().setTimeField("time").setTimeFormat(DataDescription.EPOCH_MS));
        jobBuilder.setResultsRetentionDays(retentionDays);
        Job job = jobBuilder.build(new Date());
        PlainActionFuture<Boolean> future = new PlainActionFuture<>();
        new ExpiredResultsRemover(
            originClient,
            Collections.singletonList(job).iterator(),
            new TaskId("test-node", 0L),
            mock(AnomalyDetectionAuditor.class),
            threadPool
        ).remove(1000f, future, () -> false);
        future.actionGet();
    }

    private void refreshResultsIndex(String jobId) {
        client().admin().indices().refresh(new RefreshRequest(AnomalyDetectorsIndex.jobResultsAliasedName(jobId))).actionGet();
    }

    private void assertDocExists(String index, String id) {
        GetResponse response = client().get(new GetRequest(index, id)).actionGet();
        assertThat(response.isExists(), equalTo(true));
        assertThat(
            response.getSource().get(Result.RESULT_TYPE.getPreferredName()),
            equalTo(EsqlDatafeedSourceCheckpoint.TYPE.getPreferredName())
        );
    }

    private static Job buildEsqlJob(String jobId) {
        Job.Builder builder = new Job.Builder(jobId);
        builder.setAnalysisConfig(buildEsqlAnalysisConfig());
        builder.setDataDescription(new DataDescription.Builder().setTimeField("time").setTimeFormat(DataDescription.EPOCH_MS));
        return builder.build(new Date());
    }

    private static AnalysisConfig.Builder buildEsqlAnalysisConfig() {
        Detector.Builder detector = new Detector.Builder("count", null);
        AnalysisConfig.Builder analysisConfig = new AnalysisConfig.Builder(Collections.singletonList(detector.build()));
        analysisConfig.setBucketSpan(TimeValue.timeValueHours(1));
        return analysisConfig;
    }

    private static DatafeedConfig buildEsqlDatafeed(String datafeedId, String jobId) {
        return new DatafeedConfig.Builder(datafeedId, jobId).setEsqlQuery("FROM esql-checkpoint-index | KEEP time, value")
            .setSourceTimeField("@timestamp")
            .setGroupingInterval(TimeValue.timeValueHours(1))
            .build();
    }
}
