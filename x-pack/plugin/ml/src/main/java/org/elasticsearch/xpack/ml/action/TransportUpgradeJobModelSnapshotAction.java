/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.action;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.license.LicenseUtils;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata.PersistentTask;
import org.elasticsearch.persistent.PersistentTasksService;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.XPackField;
import org.elasticsearch.xpack.core.ml.MachineLearningField;
import org.elasticsearch.xpack.core.ml.MlConfigIndex;
import org.elasticsearch.xpack.core.ml.MlTasks;
import org.elasticsearch.xpack.core.ml.action.UpgradeJobModelSnapshotAction;
import org.elasticsearch.xpack.core.ml.action.UpgradeJobModelSnapshotAction.Request;
import org.elasticsearch.xpack.core.ml.action.UpgradeJobModelSnapshotAction.Response;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.core.ml.job.config.JobState;
import org.elasticsearch.xpack.core.ml.job.messages.Messages;
import org.elasticsearch.xpack.core.ml.job.persistence.ElasticsearchMappings;
import org.elasticsearch.xpack.core.ml.job.process.autodetect.state.ModelSnapshot;
import org.elasticsearch.xpack.core.ml.job.results.Result;
import org.elasticsearch.xpack.core.ml.job.snapshot.upgrade.SnapshotUpgradeTaskParams;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.core.ml.utils.TransportVersionUtils;
import org.elasticsearch.xpack.ml.job.persistence.JobConfigProvider;
import org.elasticsearch.xpack.ml.job.persistence.JobResultsProvider;
import org.elasticsearch.xpack.ml.job.snapshot.upgrader.SnapshotUpgradePredicate;
import org.elasticsearch.xpack.ml.process.MlMemoryTracker;

import static org.elasticsearch.core.Strings.format;

public class TransportUpgradeJobModelSnapshotAction extends TransportMasterNodeAction<Request, Response> {

    private static final Logger logger = LogManager.getLogger(TransportUpgradeJobModelSnapshotAction.class);

    private final XPackLicenseState licenseState;
    private final PersistentTasksService persistentTasksService;
    private final JobConfigProvider jobConfigProvider;
    private final JobResultsProvider jobResultsProvider;
    private final MlMemoryTracker memoryTracker;
    private final Client client;

    @Inject
    public TransportUpgradeJobModelSnapshotAction(
        TransportService transportService,
        ThreadPool threadPool,
        XPackLicenseState licenseState,
        ClusterService clusterService,
        PersistentTasksService persistentTasksService,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        JobConfigProvider jobConfigProvider,
        MlMemoryTracker memoryTracker,
        JobResultsProvider jobResultsProvider,
        Client client
    ) {
        super(
            UpgradeJobModelSnapshotAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            Request::new,
            indexNameExpressionResolver,
            Response::new,
            ThreadPool.Names.SAME
        );
        this.licenseState = licenseState;
        this.persistentTasksService = persistentTasksService;
        this.jobConfigProvider = jobConfigProvider;
        this.jobResultsProvider = jobResultsProvider;
        this.memoryTracker = memoryTracker;
        this.client = client;
    }

    @Override
    protected ClusterBlockException checkBlock(Request request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }

    @Override
    protected void masterOperation(Task task, Request request, ClusterState state, ActionListener<Response> listener) {
        if (MachineLearningField.ML_API_FEATURE.check(licenseState) == false) {
            listener.onFailure(LicenseUtils.newComplianceException(XPackField.MACHINE_LEARNING));
            return;
        }

        if (TransportVersionUtils.areAllTransformVersionsTheSame(state) == false) {
            listener.onFailure(
                ExceptionsHelper.conflictStatusException(
                    "Cannot upgrade job [{}] snapshot [{}] as not all transport versions are {}. All transport versions must be the same",
                    request.getJobId(),
                    request.getSnapshotId(),
                    TransportVersionUtils.getMaxTransportVersion(state).toString()
                )
            );
            return;
        }

        PersistentTasksCustomMetadata customMetadata = state.getMetadata().custom(PersistentTasksCustomMetadata.TYPE);
        if (customMetadata != null
            && (customMetadata.findTasks(
                MlTasks.JOB_SNAPSHOT_UPGRADE_TASK_NAME,
                t -> t.getParams() instanceof SnapshotUpgradeTaskParams
                    && ((SnapshotUpgradeTaskParams) t.getParams()).getJobId().equals(request.getJobId())
            ).isEmpty() == false)) {
            listener.onFailure(
                ExceptionsHelper.conflictStatusException(
                    "Cannot upgrade job [{}] snapshot [{}] as there is currently a snapshot for this job being upgraded",
                    request.getJobId(),
                    request.getSnapshotId()
                )
            );
            return;
        }

        final SnapshotUpgradeTaskParams params = new SnapshotUpgradeTaskParams(request.getJobId(), request.getSnapshotId());
        // Wait for job to be started
        ActionListener<PersistentTask<SnapshotUpgradeTaskParams>> waitForJobToStart = ActionListener.wrap(
            persistentTask -> waitForJobStarted(persistentTask.getId(), params, request, listener),
            e -> {
                if (ExceptionsHelper.unwrapCause(e) instanceof ResourceAlreadyExistsException) {
                    e = ExceptionsHelper.conflictStatusException(
                        "Cannot upgrade job [{}] snapshot [{}] because upgrade is already in progress",
                        e,
                        request.getJobId(),
                        request.getSnapshotId()
                    );
                }
                listener.onFailure(e);
            }
        );

        // Start job task
        ActionListener<Boolean> configIndexMappingUpdaterListener = ActionListener.wrap(_unused -> {
            logger.info("[{}] [{}] sending start upgrade request", params.getJobId(), params.getSnapshotId());
            persistentTasksService.sendStartRequest(
                MlTasks.snapshotUpgradeTaskId(params.getJobId(), params.getSnapshotId()),
                MlTasks.JOB_SNAPSHOT_UPGRADE_TASK_NAME,
                params,
                waitForJobToStart
            );
        }, listener::onFailure);

        // Update config index if necessary
        ActionListener<Long> memoryRequirementRefreshListener = ActionListener.wrap(
            mem -> ElasticsearchMappings.addDocMappingIfMissing(
                MlConfigIndex.indexName(),
                MlConfigIndex::mapping,
                client,
                state,
                request.masterNodeTimeout(),
                configIndexMappingUpdaterListener
            ),
            listener::onFailure
        );

        // Check that model snapshot exists and should actually be upgraded
        // Then refresh the memory
        ActionListener<Result<ModelSnapshot>> getSnapshotHandler = ActionListener.wrap(response -> {
            if (response == null) {
                listener.onFailure(
                    new ResourceNotFoundException(
                        Messages.getMessage(Messages.REST_NO_SUCH_MODEL_SNAPSHOT, request.getSnapshotId(), request.getJobId())
                    )
                );
                return;
            }
            if (Version.CURRENT.equals(response.result.getMinVersion())) {
                listener.onFailure(
                    ExceptionsHelper.conflictStatusException(
                        "Cannot upgrade job [{}] snapshot [{}] as it is already compatible with current version {}",
                        request.getJobId(),
                        request.getSnapshotId(),
                        Version.CURRENT
                    )
                );
                return;
            }
            memoryTracker.refreshAnomalyDetectorJobMemoryAndAllOthers(params.getJobId(), memoryRequirementRefreshListener);
        }, listener::onFailure);

        ActionListener<Job> getJobHandler = ActionListener.wrap(job -> {
            if (request.getSnapshotId().equals(job.getModelSnapshotId())
                && (JobState.CLOSED.equals(MlTasks.getJobState(request.getJobId(), customMetadata)) == false)) {
                listener.onFailure(
                    ExceptionsHelper.conflictStatusException(
                        "Cannot upgrade snapshot [{}] for job [{}] as it is the current primary job snapshot and the job's state is [{}]",
                        request.getSnapshotId(),
                        request.getJobId(),
                        MlTasks.getJobState(request.getJobId(), customMetadata)
                    )
                );
                return;
            }
            jobResultsProvider.getModelSnapshot(
                request.getJobId(),
                request.getSnapshotId(),
                getSnapshotHandler::onResponse,
                getSnapshotHandler::onFailure
            );
        }, listener::onFailure);

        // Get the job config to verify it exists
        jobConfigProvider.getJob(
            request.getJobId(),
            null,
            ActionListener.wrap(builder -> getJobHandler.onResponse(builder.build()), listener::onFailure)
        );
    }

    private void waitForJobStarted(
        String taskId,
        SnapshotUpgradeTaskParams params,
        Request request,
        ActionListener<UpgradeJobModelSnapshotAction.Response> listener
    ) {
        SnapshotUpgradePredicate predicate = new SnapshotUpgradePredicate(request.isWaitForCompletion(), logger);
        persistentTasksService.waitForPersistentTaskCondition(
            taskId,
            predicate,
            request.getTimeout(),
            new PersistentTasksService.WaitForPersistentTaskListener<SnapshotUpgradeTaskParams>() {
                @Override
                public void onResponse(PersistentTask<SnapshotUpgradeTaskParams> persistentTask) {
                    if (predicate.getException() != null) {
                        if (predicate.isShouldCancel()) {
                            // We want to return to the caller without leaving an unassigned persistent task, to match
                            // what would have happened if the error had been detected in the "fast fail" validation
                            cancelJobStart(persistentTask, predicate.getException(), listener);
                        } else {
                            listener.onFailure(predicate.getException());
                        }
                    } else {
                        listener.onResponse(new Response(predicate.isCompleted(), predicate.getNode()));
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    listener.onFailure(e);
                }

                @Override
                public void onTimeout(TimeValue timeout) {
                    listener.onFailure(
                        new ElasticsearchException(
                            "snapshot upgrader request [{}] [{}] timed out after [{}]",
                            params.getJobId(),
                            params.getSnapshotId(),
                            timeout
                        )
                    );
                }
            }
        );
    }

    private void cancelJobStart(
        PersistentTask<SnapshotUpgradeTaskParams> persistentTask,
        Exception exception,
        ActionListener<Response> listener
    ) {
        persistentTasksService.sendRemoveRequest(persistentTask.getId(), ActionListener.wrap(t -> listener.onFailure(exception), e -> {
            logger.error(
                () -> format(
                    "[%s] [%s] Failed to cancel persistent task that could not be assigned due to %s",
                    persistentTask.getParams().getJobId(),
                    persistentTask.getParams().getSnapshotId(),
                    exception.getMessage()
                ),
                e
            );
            listener.onFailure(exception);
        }));
    }

}
