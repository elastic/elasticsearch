/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.integration;

import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.internal.OriginSettingClient;
import org.elasticsearch.cluster.routing.OperationRouting;
import org.elasticsearch.cluster.service.ClusterApplierService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.cluster.service.MasterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.exception.ElasticsearchStatusException;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.ml.MlConfigVersion;
import org.elasticsearch.xpack.core.ml.action.DeleteModelSnapshotAction;
import org.elasticsearch.xpack.core.ml.action.GetModelSnapshotsAction;
import org.elasticsearch.xpack.core.ml.action.PutJobAction;
import org.elasticsearch.xpack.core.ml.action.RevertModelSnapshotAction;
import org.elasticsearch.xpack.core.ml.action.UpdateModelSnapshotAction;
import org.elasticsearch.xpack.core.ml.action.UpgradeJobModelSnapshotAction;
import org.elasticsearch.xpack.core.ml.job.config.AnalysisConfig;
import org.elasticsearch.xpack.core.ml.job.config.DataDescription;
import org.elasticsearch.xpack.core.ml.job.config.DetectionRule;
import org.elasticsearch.xpack.core.ml.job.config.Detector;
import org.elasticsearch.xpack.core.ml.job.config.Job;
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

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

public class JobModelSnapshotCRUDIT extends MlSingleNodeTestCase {

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

        OriginSettingClient originSettingClient = new OriginSettingClient(client(), ClientHelper.ML_ORIGIN);
        ResultsPersisterService resultsPersisterService = new ResultsPersisterService(
            tp,
            originSettingClient,
            clusterService,
            Settings.EMPTY
        );
        jobResultsPersister = new JobResultsPersister(originSettingClient, resultsPersisterService);
        waitForMlTemplates();
    }

    public void testUpgradeAlreadyUpgradedSnapshot() {
        String jobId = "job-with-current-snapshot";

        createJob(jobId);
        ModelSnapshot snapshot = new ModelSnapshot.Builder(jobId).setMinVersion(MlConfigVersion.CURRENT).setSnapshotId("snap_1").build();
        indexModelSnapshot(snapshot);

        ElasticsearchStatusException ex = expectThrows(
            ElasticsearchStatusException.class,
            () -> client().execute(
                UpgradeJobModelSnapshotAction.INSTANCE,
                new UpgradeJobModelSnapshotAction.Request(jobId, "snap_1", TimeValue.timeValueMinutes(10), true)
            ).actionGet()
        );
        assertThat(ex.status(), equalTo(RestStatus.CONFLICT));
        assertThat(
            ex.getMessage(),
            containsString(
                "Cannot upgrade job [job-with-current-snapshot] snapshot [snap_1] as it is already compatible with current version"
            )
        );
    }

    public void testUpdateModelSnapshot() {
        String jobId = "update-job-model-snapshot";
        createJob(jobId);
        Date timestamp = new Date();
        ModelSnapshot snapshot = new ModelSnapshot.Builder(jobId).setMinVersion(MlConfigVersion.CURRENT)
            .setTimestamp(timestamp)
            .setSnapshotId("snap_1")
            .build();
        indexModelSnapshot(snapshot);

        UpdateModelSnapshotAction.Request request = new UpdateModelSnapshotAction.Request(jobId, "snap_1");
        request.setDescription("new_description");
        request.setRetain(true);
        UpdateModelSnapshotAction.Response response = client().execute(UpdateModelSnapshotAction.INSTANCE, request).actionGet();
        assertThat(response.getModel().isRetain(), is(true));
        assertThat(response.getModel().getDescription(), equalTo("new_description"));

        GetModelSnapshotsAction.Response getResponse = client().execute(
            GetModelSnapshotsAction.INSTANCE,
            new GetModelSnapshotsAction.Request(jobId, "snap_1")
        ).actionGet();
        assertThat(getResponse.getResources().results().get(0).isRetain(), is(true));
        assertThat(getResponse.getResources().results().get(0).getDescription(), equalTo("new_description"));
        assertThat(getResponse.getResources().results().get(0).getTimestamp(), equalTo(timestamp));
    }

    public void testDeleteUnusedModelSnapshot() {
        String jobId = "delete-job-model-snapshot-unused";
        createJob(jobId);
        ModelSnapshot snapshot = new ModelSnapshot.Builder(jobId).setMinVersion(MlConfigVersion.CURRENT).setSnapshotId("snap_1").build();
        indexModelSnapshot(snapshot);
        GetModelSnapshotsAction.Response getResponse = client().execute(
            GetModelSnapshotsAction.INSTANCE,
            new GetModelSnapshotsAction.Request(jobId, "snap_1")
        ).actionGet();
        assertThat(getResponse.getResources().results(), hasSize(1));

        client().execute(DeleteModelSnapshotAction.INSTANCE, new DeleteModelSnapshotAction.Request(jobId, "snap_1")).actionGet();

        getResponse = client().execute(GetModelSnapshotsAction.INSTANCE, new GetModelSnapshotsAction.Request(jobId, "snap_1")).actionGet();
        assertThat(getResponse.getResources().results(), hasSize(0));
    }

    public void testDeleteUsedModelSnapshot() {
        String jobId = "delete-job-model-snapshot-used";
        Date timestamp = new Date();
        createJob(jobId);
        ModelSnapshot snapshot = new ModelSnapshot.Builder(jobId).setMinVersion(MlConfigVersion.CURRENT)
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
        IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> client().execute(DeleteModelSnapshotAction.INSTANCE, new DeleteModelSnapshotAction.Request(jobId, "snap_1")).actionGet()
        );
        assertThat(
            ex.getMessage(),
            containsString("Model snapshot 'snap_1' is the active snapshot for job 'delete-job-model-snapshot-used', so cannot be deleted")
        );
    }

    private void indexModelSnapshot(ModelSnapshot snapshot) {
        jobResultsPersister.persistModelSnapshot(snapshot, WriteRequest.RefreshPolicy.IMMEDIATE, () -> true);
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
