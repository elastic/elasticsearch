/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.job.snapshot.upgrader;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.persistent.AllocatedPersistentTask;
import org.elasticsearch.persistent.PersistentTaskState;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.xpack.core.ml.MlConfigIndex;
import org.elasticsearch.xpack.core.ml.MlTasks;
import org.elasticsearch.xpack.core.ml.annotations.AnnotationIndex;
import org.elasticsearch.xpack.core.ml.job.persistence.AnomalyDetectorsIndex;
import org.elasticsearch.xpack.core.ml.job.persistence.ElasticsearchMappings;
import org.elasticsearch.xpack.core.ml.job.process.autodetect.state.ModelSnapshot;
import org.elasticsearch.xpack.core.ml.job.results.Result;
import org.elasticsearch.xpack.core.ml.job.snapshot.upgrade.SnapshotUpgradeState;
import org.elasticsearch.xpack.core.ml.job.snapshot.upgrade.SnapshotUpgradeTaskState;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.ml.MachineLearning;
import org.elasticsearch.xpack.ml.job.JobNodeSelector;
import org.elasticsearch.xpack.ml.job.persistence.JobDataDeleter;
import org.elasticsearch.xpack.ml.job.persistence.JobResultsProvider;
import org.elasticsearch.xpack.ml.job.process.autodetect.AutodetectProcessManager;
import org.elasticsearch.xpack.ml.notifications.AnomalyDetectionAuditor;
import org.elasticsearch.xpack.ml.process.MlMemoryTracker;
import org.elasticsearch.xpack.ml.task.AbstractJobPersistentTasksExecutor;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;


public class SnapshotUpgradeTaskExecutor extends AbstractJobPersistentTasksExecutor<SnapshotUpgradeTaskParams> {

    private static final Logger logger = LogManager.getLogger(SnapshotUpgradeTaskExecutor.class);
    private final AutodetectProcessManager autodetectProcessManager;
    private final AnomalyDetectionAuditor auditor;
    private final JobResultsProvider jobResultsProvider;
    private final XPackLicenseState licenseState;
    private volatile ClusterState clusterState;
    private final Client client;

    public SnapshotUpgradeTaskExecutor(Settings settings,
                                       ClusterService clusterService,
                                       AutodetectProcessManager autodetectProcessManager,
                                       MlMemoryTracker memoryTracker,
                                       IndexNameExpressionResolver expressionResolver,
                                       Client client,
                                       XPackLicenseState licenseState) {
        super(MlTasks.JOB_SNAPSHOT_UPGRADE_TASK_NAME,
            MachineLearning.UTILITY_THREAD_POOL_NAME,
            settings,
            clusterService,
            memoryTracker,
            expressionResolver);
        this.autodetectProcessManager = autodetectProcessManager;
        this.auditor = new AnomalyDetectionAuditor(client, clusterService);
        this.jobResultsProvider = new JobResultsProvider(client, settings, expressionResolver);
        this.client = client;
        this.licenseState = licenseState;
        clusterService.addListener(event -> clusterState = event.state());
    }

    @Override
    public PersistentTasksCustomMetadata.Assignment getAssignment(SnapshotUpgradeTaskParams params,
                                                                  Collection<DiscoveryNode> candidateNodes,
                                                                  ClusterState clusterState) {
        boolean isMemoryTrackerRecentlyRefreshed = memoryTracker.isRecentlyRefreshed();
        Optional<PersistentTasksCustomMetadata.Assignment> optionalAssignment =
            getPotentialAssignment(params, clusterState, isMemoryTrackerRecentlyRefreshed);
        // NOTE: this will return here if isMemoryTrackerRecentlyRefreshed is false, we don't allow assignment with stale memory
        if (optionalAssignment.isPresent()) {
            return optionalAssignment.get();
        }
        JobNodeSelector jobNodeSelector = new JobNodeSelector(
            clusterState,
            candidateNodes,
            params.getJobId(),
            // Use the job_task_name for the appropriate job size
            MlTasks.JOB_TASK_NAME,
            memoryTracker,
            0,
            node -> null);
        return jobNodeSelector.selectNode(
            Integer.MAX_VALUE,
            Integer.MAX_VALUE,
            maxMachineMemoryPercent,
            Long.MAX_VALUE,
            useAutoMemoryPercentage);
    }

    @Override
    protected void nodeOperation(AllocatedPersistentTask task, SnapshotUpgradeTaskParams params, PersistentTaskState state) {
        SnapshotUpgradeTaskState jobTaskState = (SnapshotUpgradeTaskState) state;
        SnapshotUpgradeState jobState = jobTaskState == null ? null : jobTaskState.getState();
        logger.info("[{}] [{}] starting to execute task",
            params.getJobId(),
            params.getSnapshotId());

        // This means that we have loaded the snapshot and possibly snapshot was partially updated
        // This is no good, we should remove the snapshot
        if (SnapshotUpgradeState.SAVING_NEW_STATE.equals(jobState)) {
            deleteSnapshotAndFailTask(task, params.getJobId(), params.getSnapshotId());
            return;
        }
        // if the task is failed, that means it was set that way purposefully. So, assuming there is no bad snapshot state
        if (SnapshotUpgradeState.FAILED.equals(jobState)) {
            logger.warn(
                "[{}] [{}] upgrade task reassigned to another node while failed",
                params.getJobId(),
                params.getSnapshotId());
            task.markAsFailed(new ElasticsearchStatusException(
                "Task to upgrade job [{}] snapshot [{}] got reassigned while failed. Reason [{}]",
                RestStatus.INTERNAL_SERVER_ERROR,
                params.getJobId(),
                params.getSnapshotId(),
                jobTaskState.getReason() == null ? "__unknown__" : jobTaskState.getReason()));
            return;
        }
        final String jobId = params.getJobId();
        final String snapshotId = params.getSnapshotId();

        ActionListener<Boolean> stateAliasHandler = ActionListener.wrap(
            r -> autodetectProcessManager.upgradeSnapshot((SnapshotUpgradeTask)task, e -> {
                if (e == null) {
                    auditor.info(jobId, "Finished upgrading snapshot [" + snapshotId + "]");
                    logger.info("[{}] [{}] finished upgrading snapshot", jobId, snapshotId);
                    task.markAsCompleted();
                } else {
                    logger.warn(
                        () -> new ParameterizedMessage(
                            "[{}] failed upgrading snapshot [{}]",
                            jobId,
                            snapshotId),
                        e);
                    auditor.warning(jobId,
                        "failed upgrading snapshot ["
                            + snapshotId
                            + "] with exception "
                            + ExceptionsHelper.unwrapCause(e).getMessage());
                    task.markAsFailed(e);
                }
            }),
            e -> {
                logger.warn(
                    () -> new ParameterizedMessage(
                        "[{}] failed upgrading snapshot [{}] as ml state alias creation failed",
                        jobId,
                        snapshotId),
                    e);
                auditor.warning(jobId,
                    "failed upgrading snapshot ["
                        + snapshotId
                        + "] with exception "
                        + ExceptionsHelper.unwrapCause(e).getMessage());
                // We need to update cluster state so the API caller can be notified and exit
                // As we have not set the task state to STARTED, it might still be waiting.
                task.updatePersistentTaskState(
                    new SnapshotUpgradeTaskState(SnapshotUpgradeState.FAILED, -1, e.getMessage()),
                    ActionListener.wrap(
                        r -> task.markAsFailed(e),
                        failure -> {
                            logger.warn(
                                new ParameterizedMessage(
                                    "[{}] [{}] failed to set task to failed",
                                    jobId,
                                    snapshotId),
                                failure);
                            task.markAsFailed(e);
                        }
                    ));
            }
        );

        // Make sure the state index and alias exist
        ActionListener<Boolean> resultsMappingUpdateHandler = ActionListener.wrap(
            ack -> AnomalyDetectorsIndex.createStateIndexAndAliasIfNecessary(client, clusterState, expressionResolver,
                MlTasks.PERSISTENT_TASK_MASTER_NODE_TIMEOUT, stateAliasHandler),
            task::markAsFailed
        );

        // Try adding the results doc mapping - this updates to the latest version if an old mapping is present
        ActionListener<Boolean> annotationsIndexUpdateHandler = ActionListener.wrap(
            ack -> ElasticsearchMappings.addDocMappingIfMissing(
                AnomalyDetectorsIndex.jobResultsAliasedName(jobId),
                AnomalyDetectorsIndex::wrappedResultsMapping,
                client,
                clusterState,
                MlTasks.PERSISTENT_TASK_MASTER_NODE_TIMEOUT,
                resultsMappingUpdateHandler),
            e -> {
                // Due to a bug in 7.9.0 it's possible that the annotations index already has incorrect mappings
                // and it would cause more harm than good to block jobs from opening in subsequent releases
                logger.warn(new ParameterizedMessage("[{}] ML annotations index could not be updated with latest mappings", jobId), e);
                ElasticsearchMappings.addDocMappingIfMissing(
                    AnomalyDetectorsIndex.jobResultsAliasedName(jobId),
                    AnomalyDetectorsIndex::wrappedResultsMapping,
                    client,
                    clusterState,
                    MlTasks.PERSISTENT_TASK_MASTER_NODE_TIMEOUT,
                    resultsMappingUpdateHandler);
            }
        );

        // Create the annotations index if necessary - this also updates the mappings if an old mapping is present
        AnnotationIndex.createAnnotationsIndexIfNecessaryAndWaitForYellow(client, clusterState, MlTasks.PERSISTENT_TASK_MASTER_NODE_TIMEOUT,
            annotationsIndexUpdateHandler);
    }

    @Override
    protected AllocatedPersistentTask createTask(long id, String type, String action, TaskId parentTaskId,
                                                 PersistentTasksCustomMetadata.PersistentTask<SnapshotUpgradeTaskParams> persistentTask,
                                                 Map<String, String> headers) {
        return new SnapshotUpgradeTask(persistentTask.getParams().getJobId(),
            persistentTask.getParams().getSnapshotId(),
            id,
            type,
            action,
            parentTaskId,
            headers,
            licenseState);
    }

    @Override
    protected boolean allowsMissingIndices() {
        return false;
    }

    @Override
    protected String[] indicesOfInterest(SnapshotUpgradeTaskParams params) {
        return new String[]{
            AnomalyDetectorsIndex.jobStateIndexPattern(),
            MlConfigIndex.indexName(),
            AnomalyDetectorsIndex.resultsWriteAlias(params.getJobId())
        };
    }

    @Override
    protected String getJobId(SnapshotUpgradeTaskParams params) {
        return params.getJobId();
    }

    private void deleteSnapshotAndFailTask(AllocatedPersistentTask task, String jobId, String snapshotId) {
        ActionListener<Result<ModelSnapshot>> modelSnapshotListener = ActionListener.wrap(
            result -> {
                if (result == null) {
                    task.markAsFailed(new ElasticsearchStatusException(
                        "Task to upgrade job [{}] snapshot [{}] got reassigned while running leaving an unknown snapshot state. " +
                            "Snapshot is deleted",
                        RestStatus.INTERNAL_SERVER_ERROR,
                        jobId,
                        snapshotId));
                    return;
                }
                ModelSnapshot snapshot = result.result;
                JobDataDeleter jobDataDeleter = new JobDataDeleter(client, jobId);
                jobDataDeleter.deleteModelSnapshots(Collections.singletonList(snapshot), ActionListener.wrap(
                    deleteResponse -> {
                        auditor.warning(
                            jobId,
                            "Task to upgrade snapshot exited in unknown state. Deleted snapshot [" + snapshotId + "]");
                        task.markAsFailed(new ElasticsearchStatusException(
                            "Task to upgrade job [{}] snapshot [{}] got reassigned while running leaving an unknown snapshot state. " +
                                "Corrupted snapshot deleted",
                            RestStatus.INTERNAL_SERVER_ERROR,
                            jobId,
                            snapshotId));
                    },
                    failure -> {
                        logger.warn(
                            () -> new ParameterizedMessage(
                                "[{}] [{}] failed to clean up potentially bad snapshot",
                                jobId,
                                snapshotId),
                            failure);
                        task.markAsFailed(new ElasticsearchStatusException(
                            "Task to upgrade job [{}] snapshot [{}] got reassigned while running leaving an unknown snapshot state. " +
                                "Unable to cleanup potentially corrupted snapshot",
                            RestStatus.INTERNAL_SERVER_ERROR,
                            jobId,
                            snapshotId));
                    }
                ));
            },
            e -> {
                if (ExceptionsHelper.unwrapCause(e) instanceof ResourceNotFoundException) {
                    task.markAsFailed(new ElasticsearchStatusException(
                        "Task to upgrade job [{}] snapshot [{}] got reassigned while running leaving an unknown snapshot state. " +
                            "Snapshot is deleted",
                        RestStatus.INTERNAL_SERVER_ERROR,
                        jobId,
                        snapshotId));
                    return;
                }
                logger.warn(
                    () -> new ParameterizedMessage(
                        "[{}] [{}] failed to load bad snapshot for deletion",
                        jobId,
                        snapshotId
                    ),
                    e);
                task.markAsFailed(new ElasticsearchStatusException(
                    "Task to upgrade job [{}] snapshot [{}] got reassigned while running leaving an unknown snapshot state. " +
                        "Unable to cleanup potentially corrupted snapshot",
                    RestStatus.INTERNAL_SERVER_ERROR,
                    jobId,
                    snapshotId));

            }
        );
        jobResultsProvider.getModelSnapshot(jobId, snapshotId, modelSnapshotListener::onResponse, modelSnapshotListener::onFailure);
    }
}
