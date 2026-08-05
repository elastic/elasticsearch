/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.integration;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.internal.OriginSettingClient;
import org.elasticsearch.cluster.routing.OperationRouting;
import org.elasticsearch.cluster.service.ClusterApplierService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.cluster.service.MasterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.crossproject.ProjectRoutingResolver;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.ml.MlConfigVersion;
import org.elasticsearch.xpack.core.ml.action.GetDatafeedsAction;
import org.elasticsearch.xpack.core.ml.action.GetModelSnapshotsAction;
import org.elasticsearch.xpack.core.ml.action.OpenJobAction;
import org.elasticsearch.xpack.core.ml.action.PutDatafeedAction;
import org.elasticsearch.xpack.core.ml.action.PutJobAction;
import org.elasticsearch.xpack.core.ml.action.UpdateDatafeedAction;
import org.elasticsearch.xpack.core.ml.action.UpdateJobAction;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedConfig;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedUpdate;
import org.elasticsearch.xpack.core.ml.job.config.AnalysisConfig;
import org.elasticsearch.xpack.core.ml.job.config.DataDescription;
import org.elasticsearch.xpack.core.ml.job.config.Detector;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.core.ml.job.config.JobUpdate;
import org.elasticsearch.xpack.core.ml.job.messages.Messages;
import org.elasticsearch.xpack.core.ml.job.process.autodetect.state.ModelSizeStats;
import org.elasticsearch.xpack.core.ml.job.process.autodetect.state.ModelSnapshot;
import org.elasticsearch.xpack.core.security.cloud.CloudCredentialsExtension;
import org.elasticsearch.xpack.ml.MlSingleNodeTestCase;
import org.elasticsearch.xpack.ml.inference.ingest.InferenceProcessor;
import org.elasticsearch.xpack.ml.job.persistence.JobResultsPersister;
import org.elasticsearch.xpack.ml.utils.persistence.ResultsPersisterService;
import org.junit.Before;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assume.assumeTrue;

public class DatafeedScopeChangeSnapshotIT extends MlSingleNodeTestCase {

    private JobResultsPersister jobResultsPersister;

    @Override
    protected Settings nodeSettings() {
        return Settings.builder()
            .put(super.nodeSettings())
            .put(XPackSettings.SECURITY_ENABLED.getKey(), false)
            .put("serverless.cross_project.enabled", true)
            .build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return Stream.concat(super.getPlugins().stream(), Stream.of(DatafeedCrossProjectIT.CpsPlugin.class)).toList();
    }

    @Before
    public void createComponents() throws Exception {
        assumeTrue("CPS feature flag must be enabled", CloudCredentialsExtension.ML_CROSS_PROJECT.isEnabled());
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
                    ClusterApplierService.CLUSTER_SERVICE_SLOW_TASK_THREAD_DUMP_TIMEOUT_SETTING,
                    ClusterApplierService.CLUSTER_APPLIER_THREAD_WATCHDOG_INTERVAL,
                    ClusterApplierService.CLUSTER_APPLIER_THREAD_WATCHDOG_QUIET_TIME
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

    public void testUserInitiatedProjectRoutingChangeWithOpenJobShouldReject() throws Exception {
        String jobId = "scope-change-open-job";
        String datafeedId = jobId + "-datafeed";
        setupJobDatafeedAndSnapshot(jobId, datafeedId, ProjectRoutingResolver.LOCAL_ONLY);

        client().execute(OpenJobAction.INSTANCE, new OpenJobAction.Request(jobId)).actionGet();

        ElasticsearchStatusException ex = expectThrows(
            ElasticsearchStatusException.class,
            () -> updateProjectRouting(datafeedId, "_alias:prod-*")
        );
        assertThat(ex.status(), equalTo(RestStatus.CONFLICT));
        assertThat(ex.getMessage(), containsString(datafeedId));
        assertThat(ex.getMessage(), containsString(jobId));
    }

    public void testUserInitiatedProjectRoutingChangeWithClosedJobShouldRetainSnapshotAndUpdateRouting() throws Exception {
        String jobId = "scope-change-closed-job";
        String datafeedId = jobId + "-datafeed";
        String snapshotId = "rollback-snap";
        setupJobDatafeedAndSnapshot(jobId, datafeedId, ProjectRoutingResolver.LOCAL_ONLY, snapshotId);

        String newRouting = "_alias:prod-*";
        client().execute(
            UpdateDatafeedAction.INSTANCE,
            new UpdateDatafeedAction.Request(new DatafeedUpdate.Builder(datafeedId).setProjectRouting(newRouting).build())
        ).actionGet();

        GetDatafeedsAction.Response getDatafeedResponse = client().execute(
            GetDatafeedsAction.INSTANCE,
            new GetDatafeedsAction.Request(datafeedId)
        ).actionGet();
        assertThat(getDatafeedResponse.getResources().results().get(0).getProjectRouting(), equalTo(newRouting));

        GetModelSnapshotsAction.Response snapshotResponse = client().execute(
            GetModelSnapshotsAction.INSTANCE,
            new GetModelSnapshotsAction.Request(jobId, snapshotId)
        ).actionGet();
        ModelSnapshot retained = snapshotResponse.getResources().results().get(0);
        assertThat(retained.isRetain(), is(true));
        assertThat(
            retained.getDescription(),
            equalTo(
                Messages.getMessage(
                    Messages.DATAFEED_SCOPE_CHANGE_ROLLBACK_SNAPSHOT_DESCRIPTION,
                    ProjectRoutingResolver.LOCAL_ONLY,
                    newRouting
                )
            )
        );
    }

    private void setupJobDatafeedAndSnapshot(String jobId, String datafeedId, String projectRouting) throws Exception {
        setupJobDatafeedAndSnapshot(jobId, datafeedId, projectRouting, "snap-1");
    }

    private void setupJobDatafeedAndSnapshot(String jobId, String datafeedId, String projectRouting, String snapshotId) throws Exception {
        Job.Builder jobBuilder = new Job.Builder(jobId);
        jobBuilder.setAnalysisConfig(new AnalysisConfig.Builder(Collections.singletonList(new Detector.Builder("mean", "field").build())));
        jobBuilder.setDataDescription(new DataDescription.Builder());
        client().execute(PutJobAction.INSTANCE, new PutJobAction.Request(jobBuilder)).actionGet();

        Date timestamp = new Date();
        ModelSnapshot snapshot = new ModelSnapshot.Builder(jobId).setMinVersion(MlConfigVersion.CURRENT)
            .setTimestamp(timestamp)
            .setSnapshotId(snapshotId)
            .setSnapshotDocCount(1)
            .setModelSizeStats(new ModelSizeStats.Builder(jobId).setTimestamp(timestamp).setLogTime(timestamp))
            .build();
        jobResultsPersister.persistModelSnapshot(snapshot, WriteRequest.RefreshPolicy.IMMEDIATE, () -> true);

        client().execute(
            UpdateJobAction.INSTANCE,
            new UpdateJobAction.Request(jobId, new JobUpdate.Builder(jobId).setModelSnapshotId(snapshotId).build())
        ).actionGet();

        DatafeedConfig datafeed = new DatafeedConfig.Builder(datafeedId, jobId).setIndices(List.of("logs-*"))
            .setProjectRouting(projectRouting)
            .build();
        client().execute(PutDatafeedAction.INSTANCE, new PutDatafeedAction.Request(datafeed)).actionGet();
    }

    private void updateProjectRouting(String datafeedId, String newRouting) {
        client().execute(
            UpdateDatafeedAction.INSTANCE,
            new UpdateDatafeedAction.Request(new DatafeedUpdate.Builder(datafeedId).setProjectRouting(newRouting).build())
        ).actionGet();
    }
}
