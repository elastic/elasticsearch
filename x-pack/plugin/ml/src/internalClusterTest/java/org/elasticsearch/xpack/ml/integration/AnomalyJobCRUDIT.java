/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.integration;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.internal.OriginSettingClient;
import org.elasticsearch.cluster.routing.OperationRouting;
import org.elasticsearch.cluster.service.ClusterApplierService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.cluster.service.MasterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.ml.action.GetModelSnapshotsAction;
import org.elasticsearch.xpack.core.ml.action.OpenJobAction;
import org.elasticsearch.xpack.core.ml.action.PutJobAction;
import org.elasticsearch.xpack.core.ml.action.RevertModelSnapshotAction;
import org.elasticsearch.xpack.core.ml.action.UpdateJobAction;
import org.elasticsearch.xpack.core.ml.job.config.AnalysisConfig;
import org.elasticsearch.xpack.core.ml.job.config.AnalysisLimits;
import org.elasticsearch.xpack.core.ml.job.config.DataDescription;
import org.elasticsearch.xpack.core.ml.job.config.DetectionRule;
import org.elasticsearch.xpack.core.ml.job.config.Detector;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.core.ml.job.config.JobUpdate;
import org.elasticsearch.xpack.core.ml.job.process.autodetect.state.ModelSizeStats;
import org.elasticsearch.xpack.core.ml.job.process.autodetect.state.ModelSnapshot;
import org.elasticsearch.xpack.core.ml.job.process.autodetect.state.Quantiles;
import org.elasticsearch.xpack.ml.MlSingleNodeTestCase;
import org.elasticsearch.xpack.ml.inference.ingest.InferenceProcessor;
import org.elasticsearch.xpack.ml.job.persistence.JobResultsPersister;
import org.elasticsearch.xpack.ml.utils.persistence.ResultsPersisterService;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.List;

import static org.elasticsearch.xpack.core.ClientHelper.ML_ORIGIN;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

public class AnomalyJobCRUDIT extends MlSingleNodeTestCase {

    private JobResultsPersister jobResultsPersister;

    @Before
    public void createComponents() throws Exception {
        ThreadPool tp = mockThreadPool();
        ClusterSettings clusterSettings = new ClusterSettings(
            Settings.EMPTY,
            new HashSet<>(
                Arrays.asList(
                    InferenceProcessor.MAX_INFERENCE_PROCESSORS,
                    MasterService.MASTER_SERVICE_SLOW_TASK_LOGGING_THRESHOLD_SETTING,
                    OperationRouting.USE_ADAPTIVE_REPLICA_SELECTION_SETTING,
                    ResultsPersisterService.PERSIST_RESULTS_MAX_RETRIES,
                    ClusterService.USER_DEFINED_METADATA,
                    ClusterApplierService.CLUSTER_SERVICE_SLOW_TASK_LOGGING_THRESHOLD_SETTING,
                    ClusterApplierService.CLUSTER_SERVICE_SLOW_TASK_THREAD_DUMP_TIMEOUT_SETTING
                )
            )
        );
        ClusterService clusterService = new ClusterService(Settings.EMPTY, clusterSettings, tp, null);

        OriginSettingClient originSettingClient = new OriginSettingClient(client(), ML_ORIGIN);
        ResultsPersisterService resultsPersisterService = new ResultsPersisterService(
            tp,
            originSettingClient,
            clusterService,
            Settings.EMPTY
        );
        jobResultsPersister = new JobResultsPersister(originSettingClient, resultsPersisterService);
        waitForMlTemplates();
    }

    public void testUpdateModelMemoryLimitOnceEstablished() {
        String jobId = "memory-limit-established";
        createJob(jobId);
        jobResultsPersister.persistModelSizeStats(
            new ModelSizeStats.Builder(jobId).setTimestamp(new Date()).setLogTime(new Date()).setModelBytes(10000000).build(),
            () -> false
        );
        jobResultsPersister.commitWrites(jobId, JobResultsPersister.CommitType.RESULTS);

        ElasticsearchStatusException iae = expectThrows(
            ElasticsearchStatusException.class,
            () -> client().execute(
                UpdateJobAction.INSTANCE,
                new UpdateJobAction.Request(jobId, new JobUpdate.Builder(jobId).setAnalysisLimits(new AnalysisLimits(5L, 0L)).build())
            ).actionGet()
        );
        assertThat(iae.getMessage(), containsString("model_memory_limit cannot be decreased below current usage"));

        // Shouldn't throw
        client().execute(
            UpdateJobAction.INSTANCE,
            new UpdateJobAction.Request(jobId, new JobUpdate.Builder(jobId).setAnalysisLimits(new AnalysisLimits(30L, 0L)).build())
        ).actionGet();

    }

    public void testCreateWithExistingCategorizerDocs() {
        String jobId = "job-id-with-existing-docs";
        testCreateWithExistingDocs(
            prepareIndex(".ml-state-000001").setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                .setId(jobId + "_categorizer_state#1")
                .setSource("{}", XContentType.JSON)
                .request(),
            jobId
        );
    }

    public void testCreateWithExistingQuantilesDocs() {
        String jobId = "job-id-with-existing-docs";
        testCreateWithExistingDocs(
            prepareIndex(".ml-state-000001").setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                .setId(jobId + "_quantiles")
                .setSource("{}", XContentType.JSON)
                .request(),
            jobId
        );
    }

    public void testCreateWithExistingResultsDocs() {
        String jobId = "job-id-with-existing-docs";
        testCreateWithExistingDocs(
            prepareIndex(".ml-anomalies-shared").setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                .setId(jobId + "_1464739200000_1")
                .setSource("{\"job_id\": \"" + jobId + "\"}", XContentType.JSON)
                .request(),
            jobId
        );
    }

    public void testPutJobWithClosedResultsIndex() {
        String jobId = "job-with-closed-results-index";
        client().admin().indices().prepareCreate(".ml-anomalies-shared").get();
        client().admin().indices().prepareClose(".ml-anomalies-shared").get();
        ElasticsearchStatusException ex = expectThrows(ElasticsearchStatusException.class, () -> createJob(jobId));
        assertThat(
            ex.getMessage(),
            containsString("Cannot create job [job-with-closed-results-index] as it requires closed index [.ml-anomalies-*]")
        );
        client().admin().indices().prepareDelete(".ml-anomalies-shared").get();
    }

    public void testPutJobWithClosedStateIndex() {
        String jobId = "job-with-closed-results-index";
        client().admin().indices().prepareCreate(".ml-state-000001").get();
        client().admin().indices().prepareClose(".ml-state-000001").setWaitForActiveShards(0).get();
        ElasticsearchStatusException ex = expectThrows(ElasticsearchStatusException.class, () -> createJob(jobId));
        assertThat(
            ex.getMessage(),
            containsString("Cannot create job [job-with-closed-results-index] as it requires closed index [.ml-state*]")
        );
        client().admin().indices().prepareDelete(".ml-state-000001").get();
    }

    public void testOpenJobWithOldSnapshot() {
        String jobId = "open-job-with-old-model-snapshot";
        Date timestamp = new Date();
        createJob(jobId);
        ModelSnapshot snapshot = new ModelSnapshot.Builder(jobId).setMinVersion("6.0.0")
            .setSnapshotId("snap_1")
            .setQuantiles(new Quantiles(jobId, timestamp, "quantiles-1"))
            .setSnapshotDocCount(1)
            .setModelSizeStats(new ModelSizeStats.Builder(jobId).setTimestamp(timestamp).setLogTime(timestamp))
            .build();
        indexModelSnapshot(snapshot);
        GetModelSnapshotsAction.Response getResponse = client().execute(
            GetModelSnapshotsAction.INSTANCE,
            new GetModelSnapshotsAction.Request(jobId, "snap_1")
        ).actionGet();
        assertThat(getResponse.getResources().results(), hasSize(1));
        client().execute(RevertModelSnapshotAction.INSTANCE, new RevertModelSnapshotAction.Request(jobId, "snap_1")).actionGet();

        // should fail?
        ElasticsearchStatusException ex = expectThrows(
            ElasticsearchStatusException.class,
            () -> client().execute(OpenJobAction.INSTANCE, new OpenJobAction.Request(jobId)).actionGet()
        );
        assertThat(
            ex.getMessage(),
            containsString(
                "[open-job-with-old-model-snapshot] job model snapshot [snap_1] has min version before [8.3.0], "
                    + "please revert to a newer model snapshot or reset the job"
            )
        );
        assertThat(ex.status(), is(RestStatus.BAD_REQUEST));
    }

    private void indexModelSnapshot(ModelSnapshot snapshot) {
        jobResultsPersister.persistModelSnapshot(snapshot, WriteRequest.RefreshPolicy.IMMEDIATE, () -> true);
    }

    private void testCreateWithExistingDocs(IndexRequest indexRequest, String jobId) {
        OriginSettingClient client = new OriginSettingClient(client(), ML_ORIGIN);
        client.index(indexRequest).actionGet();
        ElasticsearchStatusException ex = expectThrows(ElasticsearchStatusException.class, () -> createJob(jobId));
        assertThat(ex.getMessage(), containsString("state documents exist for a prior job with Id [job-id-with-existing-docs]"));
    }

    private Job.Builder createJob(String jobId) {
        Job.Builder builder = new Job.Builder(jobId);
        AnalysisConfig.Builder ac = createAnalysisConfig("by_field");
        DataDescription.Builder dc = new DataDescription.Builder();
        builder.setAnalysisConfig(ac);
        builder.setDataDescription(dc);

        PutJobAction.Request request = new PutJobAction.Request(builder);
        client().execute(PutJobAction.INSTANCE, request).actionGet();
        return builder;
    }

    private AnalysisConfig.Builder createAnalysisConfig(String byFieldName) {
        Detector.Builder detector = new Detector.Builder("mean", "field");
        detector.setByFieldName(byFieldName);
        List<DetectionRule> rules = new ArrayList<>();

        detector.setRules(rules);

        return new AnalysisConfig.Builder(Collections.singletonList(detector.build()));
    }

}
