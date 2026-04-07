/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.job.task;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.RetryableAction;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.engine.DocumentMissingException;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.persistent.AllocatedPersistentTask;
import org.elasticsearch.persistent.PersistentTaskState;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata.Assignment;
import org.elasticsearch.persistent.decider.EnableAssignmentDecider;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.xpack.core.ml.MlConfigIndex;
import org.elasticsearch.xpack.core.ml.MlConfigVersion;
import org.elasticsearch.xpack.core.ml.MlMetaIndex;
import org.elasticsearch.xpack.core.ml.MlTasks;
import org.elasticsearch.xpack.core.ml.action.FinalizeJobExecutionAction;
import org.elasticsearch.xpack.core.ml.action.GetJobsAction;
import org.elasticsearch.xpack.core.ml.action.GetModelSnapshotsAction;
import org.elasticsearch.xpack.core.ml.action.OpenJobAction;
import org.elasticsearch.xpack.core.ml.action.ResetJobAction;
import org.elasticsearch.xpack.core.ml.action.RevertModelSnapshotAction;
import org.elasticsearch.xpack.core.ml.action.StopDatafeedAction;
import org.elasticsearch.xpack.core.ml.job.config.Blocked;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.core.ml.job.config.JobState;
import org.elasticsearch.xpack.core.ml.job.config.JobTaskState;
import org.elasticsearch.xpack.core.ml.job.persistence.AnomalyDetectorsIndex;
import org.elasticsearch.xpack.core.ml.job.persistence.ElasticsearchMappings;
import org.elasticsearch.xpack.core.ml.job.process.autodetect.state.ModelSnapshot;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.ml.MachineLearning;
import org.elasticsearch.xpack.ml.datafeed.persistence.DatafeedConfigProvider;
import org.elasticsearch.xpack.ml.job.JobNodeSelector;
import org.elasticsearch.xpack.ml.job.persistence.JobResultsProvider;
import org.elasticsearch.xpack.ml.job.process.autodetect.AutodetectProcessManager;
import org.elasticsearch.xpack.ml.notifications.AnomalyDetectionAuditor;
import org.elasticsearch.xpack.ml.process.MlMemoryTracker;
import org.elasticsearch.xpack.ml.task.AbstractJobPersistentTasksExecutor;

import java.time.Instant;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import static org.elasticsearch.core.Strings.format;
import static org.elasticsearch.xpack.core.ClientHelper.ML_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.executeAsyncWithOrigin;
import static org.elasticsearch.xpack.core.ml.MachineLearningField.MIN_SUPPORTED_SNAPSHOT_VERSION;
import static org.elasticsearch.xpack.core.ml.MlTasks.AWAITING_UPGRADE;
import static org.elasticsearch.xpack.core.ml.MlTasks.PERSISTENT_TASK_MASTER_NODE_TIMEOUT;
import static org.elasticsearch.xpack.ml.job.JobNodeSelector.AWAITING_LAZY_ASSIGNMENT;

public class OpenJobPersistentTasksExecutor extends AbstractJobPersistentTasksExecutor<OpenJobAction.JobParams> {

    private static final Logger logger = LogManager.getLogger(OpenJobPersistentTasksExecutor.class);

    public static String[] indicesOfInterest(String resultsIndex) {
        if (resultsIndex == null) {
            return new String[] { AnomalyDetectorsIndex.jobStateIndexPattern(), MlMetaIndex.indexName(), MlConfigIndex.indexName() };
        }
        return new String[] {
            AnomalyDetectorsIndex.jobStateIndexPattern(),
            resultsIndex,
            MlMetaIndex.indexName(),
            MlConfigIndex.indexName() };
    }

    private final AutodetectProcessManager autodetectProcessManager;
    private final DatafeedConfigProvider datafeedConfigProvider;
    private final Client client;
    private final JobResultsProvider jobResultsProvider;
    private final AnomalyDetectionAuditor auditor;
    private final XPackLicenseState licenseState;
    private final ClusterSettings clusterSettings;

    private volatile ClusterState clusterState;

    public OpenJobPersistentTasksExecutor(
        Settings settings,
        ClusterService clusterService,
        AutodetectProcessManager autodetectProcessManager,
        DatafeedConfigProvider datafeedConfigProvider,
        MlMemoryTracker memoryTracker,
        Client client,
        IndexNameExpressionResolver expressionResolver,
        XPackLicenseState licenseState,
        AnomalyDetectionAuditor auditor
    ) {
        super(MlTasks.JOB_TASK_NAME, MachineLearning.UTILITY_THREAD_POOL_NAME, settings, clusterService, memoryTracker, expressionResolver);
        this.autodetectProcessManager = Objects.requireNonNull(autodetectProcessManager);
        this.datafeedConfigProvider = Objects.requireNonNull(datafeedConfigProvider);
        this.client = Objects.requireNonNull(client);
        this.jobResultsProvider = new JobResultsProvider(client, settings, expressionResolver);
        this.auditor = auditor;
        this.licenseState = licenseState;
        this.clusterSettings = clusterService.getClusterSettings();
        clusterService.addListener(event -> clusterState = event.state());
    }

    @Override
    protected Assignment doGetAssignment(
        OpenJobAction.JobParams params,
        Collection<DiscoveryNode> candidateNodes,
        ClusterState clusterState,
        @Nullable ProjectId projectId
    ) {
        Job job = params.getJob();
        // If the task parameters do not have a job field then the job
        // was first opened on a pre v6.6 node and has not been migrated
        // out of cluster state - this should be impossible in version 8
        assert job != null;
        boolean isMemoryTrackerRecentlyRefreshed = memoryTracker.isRecentlyRefreshed();
        Optional<Assignment> optionalAssignment = getPotentialAssignment(params, clusterState, isMemoryTrackerRecentlyRefreshed);
        // NOTE: this will return here if isMemoryTrackerRecentlyRefreshed is false, we don't allow assignment with stale memory
        if (optionalAssignment.isPresent()) {
            return optionalAssignment.get();
        }

        JobNodeSelector jobNodeSelector = new JobNodeSelector(
            clusterState,
            candidateNodes,
            params.getJobId(),
            MlTasks.JOB_TASK_NAME,
            memoryTracker,
            job.allowLazyOpen() ? Integer.MAX_VALUE : maxLazyMLNodes,
            node -> nodeFilter(node, job)
        );
        Assignment assignment = jobNodeSelector.selectNode(
            maxOpenJobs,
            maxConcurrentJobAllocations,
            maxMachineMemoryPercent,
            maxNodeMemory,
            useAutoMemoryPercentage
        );
        auditRequireMemoryIfNecessary(params.getJobId(), auditor, assignment, jobNodeSelector, isMemoryTrackerRecentlyRefreshed);
        return assignment;
    }

    private static boolean nodeSupportsModelSnapshotVersion(DiscoveryNode node, Job job) {
        if (job.getModelSnapshotId() == null || job.getModelSnapshotMinVersion() == null) {
            // There is no snapshot to restore or the min model snapshot version is 5.5.0
            // which is OK as we have already checked the node is >= 5.5.0.
            return true;
        }
        return MlConfigVersion.getMlConfigVersionForNode(node).onOrAfter(job.getModelSnapshotMinVersion());
    }

    public static String nodeFilter(DiscoveryNode node, Job job) {

        String jobId = job.getId();

        if (nodeSupportsModelSnapshotVersion(node, job) == false) {
            return "Not opening job ["
                + jobId
                + "] on node ["
                + JobNodeSelector.nodeNameAndVersion(node)
                + "], because the job's model snapshot requires a node with ML config version ["
                + job.getModelSnapshotMinVersion()
                + "] or higher";
        }

        if (Job.getCompatibleJobTypes(MlConfigVersion.getMlConfigVersionForNode(node)).contains(job.getJobType()) == false) {
            return "Not opening job ["
                + jobId
                + "] on node ["
                + JobNodeSelector.nodeNameAndVersion(node)
                + "], because this node does not support jobs of type ["
                + job.getJobType()
                + "]";
        }

        return null;
    }

    static void validateJobAndId(String jobId, Job job) {
        if (job == null) {
            throw ExceptionsHelper.missingJobException(jobId);
        }
        if (job.getBlocked().getReason() != Blocked.Reason.NONE) {
            throw ExceptionsHelper.conflictStatusException(
                "Cannot open job [{}] because it is executing [{}]",
                jobId,
                job.getBlocked().getReason()
            );
        }
        if (job.getJobVersion() == null) {
            throw ExceptionsHelper.badRequestException(
                "Cannot open job [{}] because jobs created prior to version 5.5 are not supported",
                jobId
            );
        }
    }

    @Override
    public void validate(OpenJobAction.JobParams params, ClusterState clusterState, @Nullable ProjectId projectId) {
        final Job job = params.getJob();
        final String jobId = params.getJobId();
        validateJobAndId(jobId, job);
        // If we already know that we can't find an ml node because all ml nodes are running at capacity or
        // simply because there are no ml nodes in the cluster then we fail quickly here:
        var assignment = getAssignment(params, clusterState.nodes().getAllNodes(), clusterState, projectId);
        if (assignment.equals(AWAITING_UPGRADE)) {
            throw makeCurrentlyBeingUpgradedException(logger, params.getJobId());
        }

        if (assignment.getExecutorNode() == null && assignment.equals(AWAITING_LAZY_ASSIGNMENT) == false) {
            throw makeNoSuitableNodesException(logger, params.getJobId(), assignment.getExplanation());
        }
    }

    @Override
    // Exceptions that occur while the node is dying, i.e. after the JVM has received a SIGTERM,
    // are ignored. Core services will be stopping in response to the SIGTERM and we want the
    // job to try to open again on another node, not spuriously fail on the dying node.
    protected void nodeOperation(AllocatedPersistentTask task, OpenJobAction.JobParams params, PersistentTaskState state) {
        JobTask jobTask = (JobTask) task;
        jobTask.setAutodetectProcessManager(autodetectProcessManager);
        JobTaskState jobTaskState = (JobTaskState) state;

        // This indicates a system-initiated reassignment as an opposite to a user-initiated open.
        boolean isReassignment = jobTaskState != null && jobTaskState.getAllocationId() != task.getAllocationId();

        if (isReassignment) {
            // System-initiated reassignment: wrap the pipeline in retry logic with exponential backoff.
            // The task was previously assigned; this is a reopen after node failure/upgrade.
            createOpenJobRetryableAction(
                jobTask,
                jobTaskState,
                params,
                ActionListener.wrap(success -> { /* job is OPENED; state transition handled inside pipeline */ }, e -> {
                    if (autodetectProcessManager.isNodeDying() == false) {
                        failTask(jobTask, "failed after retries: " + e.getMessage());
                    }
                })
            ).run();
        } else {
            // User-initiated (new open or fresh state): run pipeline directly (fail-fast on any error)
            executeOpenJobPipeline(jobTask, jobTaskState, params, ActionListener.wrap(success -> { /* job is OPENED */ }, e -> {
                if (autodetectProcessManager.isNodeDying() == false) {
                    failTask(jobTask, e.getMessage());
                }
            }));
        }
    }

    /**
     * Executes the full job opening pipeline: mapping updates -> snapshot verification ->
     * forecast cleanup -> datafeed check -> revert -> openJob.
     *
     * Errors are routed to {@code listener.onFailure()} rather than calling {@code failTask()}
     * directly, so that the caller (e.g. {@link OpenJobRetryableAction}) can decide whether
     * to retry or fail.
     */
    void executeOpenJobPipeline(
        JobTask jobTask,
        JobTaskState jobTaskState,
        OpenJobAction.JobParams params,
        ActionListener<Boolean> listener
    ) {
        JobState jobState = jobTaskState == null ? null : jobTaskState.getState();
        ActionListener<Boolean> checkSnapshotVersionListener = ActionListener.wrap(
            mappingsUpdate -> jobResultsProvider.setRunningForecastsToFailed(
                params.getJobId(),
                ActionListener.wrap(r -> runJob(jobTask, jobState, params, listener), e -> {
                    if (autodetectProcessManager.isNodeDying() == false) {
                        logger.warn(() -> "[" + params.getJobId() + "] failed to set forecasts to failed", e);
                        runJob(jobTask, jobState, params, listener);
                    }
                })
            ),
            e -> {
                if (autodetectProcessManager.isNodeDying() == false) {
                    logger.error(() -> "[" + params.getJobId() + "] Failed verifying snapshot version", e);
                    listener.onFailure(e);
                }
            }
        );

        ActionListener<Boolean> resultsMappingUpdateHandler = ActionListener.wrap(
            mappingsUpdate -> verifyCurrentSnapshotVersion(params.getJobId(), checkSnapshotVersionListener),
            e -> {
                if (autodetectProcessManager.isNodeDying() == false) {
                    logger.error(() -> "[" + params.getJobId() + "] Failed to update results mapping", e);
                    listener.onFailure(e);
                }
            }
        );

        // We need to update the results index as we MAY update the current forecast results, setting the running forecasts to failed
        // This writes to the results index, which might need updating
        ElasticsearchMappings.addDocMappingIfMissing(
            AnomalyDetectorsIndex.jobResultsAliasedName(params.getJobId()),
            AnomalyDetectorsIndex::wrappedResultsMapping,
            client,
            clusterState,
            PERSISTENT_TASK_MASTER_NODE_TIMEOUT,
            resultsMappingUpdateHandler,
            AnomalyDetectorsIndex.RESULTS_INDEX_MAPPINGS_VERSION
        );
    }

    /**
     * Factory method for {@link OpenJobRetryableAction}. Protected to allow test spies to intercept creation.
     */
    protected OpenJobRetryableAction createOpenJobRetryableAction(
        JobTask jobTask,
        JobTaskState jobTaskState,
        OpenJobAction.JobParams params,
        ActionListener<Boolean> listener
    ) {
        return new OpenJobRetryableAction(jobTask, jobTaskState, params, listener);
    }

    // Exceptions that occur while the node is dying, i.e. after the JVM has received a SIGTERM,
    // are ignored. Core services will be stopping in response to the SIGTERM and we want the
    // job to try to open again on another node, not spuriously fail on the dying node.
    private void runJob(JobTask jobTask, JobState jobState, OpenJobAction.JobParams params, ActionListener<Boolean> listener) {
        // If the node is already running its exit handlers then do nothing - shortly
        // the persistent task will get assigned to a new node and the code below will
        // run there instead.
        if (autodetectProcessManager.isNodeDying()) {
            return;
        }
        // If the job is closing, simply stop and return
        if (JobState.CLOSING.equals(jobState)) {
            // Mark as completed instead of using `stop` as stop assumes native processes have started
            logger.info("[{}] job got reassigned while stopping. Marking as completed", params.getJobId());
            jobTask.markAsCompleted();
            return;
        }
        // If the job is failed then the Persistent Task Service will
        // try to restart it on a node restart. Exiting here leaves the
        // job in the failed state and it must be force closed.
        if (JobState.FAILED.equals(jobState)) {
            return;
        }

        ActionListener<String> getRunningDatafeedListener = ActionListener.wrap(runningDatafeedId -> {
            if (runningDatafeedId != null) {
                // This job has a running datafeed attached to it.
                // In order to prevent gaps in the model we revert to the current snapshot deleting intervening results.
                revertToCurrentSnapshot(jobTask, ActionListener.wrap(response -> openJob(jobTask, listener), e -> {
                    if (autodetectProcessManager.isNodeDying() == false) {
                        logger.error(() -> "[" + jobTask.getJobId() + "] failed to revert to current snapshot", e);
                        listener.onFailure(e);
                    }
                }));
            } else {
                openJob(jobTask, listener);
            }
        }, e -> {
            if (autodetectProcessManager.isNodeDying() == false) {
                logger.error(() -> "[" + jobTask.getJobId() + "] failed to search for associated datafeed", e);
                listener.onFailure(e);
            }
        });

        getRunningDatafeed(jobTask.getJobId(), getRunningDatafeedListener);
    }

    private void revertToCurrentSnapshot(JobTask jobTask, ActionListener<Boolean> listener) {
        ActionListener<GetJobsAction.Response> jobListener = ActionListener.wrap(jobResponse -> {
            List<Job> jobPage = jobResponse.getResponse().results();
            assert jobPage.size() == 1;

            String jobSnapshotId = jobPage.get(0).getModelSnapshotId();
            if (jobSnapshotId == null) {
                logger.info("[{}] job has running datafeed task; resetting as no snapshot exists", jobTask.getJobId());
                ResetJobAction.Request request = new ResetJobAction.Request(jobTask.getJobId());
                request.setSkipJobStateValidation(true);
                request.masterNodeTimeout(PERSISTENT_TASK_MASTER_NODE_TIMEOUT);
                request.ackTimeout(PERSISTENT_TASK_MASTER_NODE_TIMEOUT);
                executeAsyncWithOrigin(client, ML_ORIGIN, ResetJobAction.INSTANCE, request, listener.map(response -> true));
            } else {
                logger.info("[{}] job has running datafeed task; reverting to current snapshot", jobTask.getJobId());
                RevertModelSnapshotAction.Request request = new RevertModelSnapshotAction.Request(jobTask.getJobId(), jobSnapshotId);
                request.setForce(true);
                request.setDeleteInterveningResults(true);
                request.masterNodeTimeout(PERSISTENT_TASK_MASTER_NODE_TIMEOUT);
                request.ackTimeout(PERSISTENT_TASK_MASTER_NODE_TIMEOUT);
                executeAsyncWithOrigin(client, ML_ORIGIN, RevertModelSnapshotAction.INSTANCE, request, listener.map(response -> true));
            }
        }, error -> listener.onFailure(ExceptionsHelper.serverError("[{}] error getting job", error, jobTask.getJobId())));

        GetJobsAction.Request request = new GetJobsAction.Request(jobTask.getJobId());
        request.masterNodeTimeout(PERSISTENT_TASK_MASTER_NODE_TIMEOUT);
        executeAsyncWithOrigin(client, ML_ORIGIN, GetJobsAction.INSTANCE, request, jobListener);
    }

    void failTask(JobTask jobTask, String reason) {
        String jobId = jobTask.getJobId();
        auditor.error(jobId, reason);
        JobTaskState failedState = new JobTaskState(JobState.FAILED, jobTask.getAllocationId(), reason, Instant.now());
        new UpdateStateRetryableAction(
            logger,
            client.threadPool(),
            jobTask,
            failedState,
            clusterSettings.get(MachineLearning.JOB_OPEN_RETRY_TIMEOUT),
            ActionListener.wrap(r -> {
                logger.debug("[{}] updated task state to failed", jobId);
                stopAssociatedDatafeedForFailedJob(jobId);
            }, e -> {
                logger.error(() -> "[" + jobId + "] failed to set task state to failed after retries", e);
                jobTask.markAsFailed(e);
                stopAssociatedDatafeedForFailedJob(jobId);
            })
        ).run();
    }

    private void stopAssociatedDatafeedForFailedJob(String jobId) {

        if (autodetectProcessManager.isNodeDying()) {
            // The node shutdown caught us at a bad time, and we cannot stop the datafeed
            return;
        }

        ActionListener<String> getRunningDatafeedListener = ActionListener.wrap(runningDatafeedId -> {
            if (runningDatafeedId == null) {
                return;
            }
            StopDatafeedAction.Request request = new StopDatafeedAction.Request(runningDatafeedId);
            request.setForce(true);
            executeAsyncWithOrigin(
                client,
                ML_ORIGIN,
                StopDatafeedAction.INSTANCE,
                request,
                ActionListener.wrap(
                    // StopDatafeedAction will audit the stopping of the datafeed if it succeeds so we don't need to do that here
                    r -> logger.info("[{}] stopped associated datafeed [{}] after job failure", jobId, runningDatafeedId),
                    e -> {
                        if (autodetectProcessManager.isNodeDying() == false) {
                            logger.error(
                                () -> format("[%s] failed to stop associated datafeed [%s] after job failure", jobId, runningDatafeedId),
                                e
                            );
                            auditor.error(jobId, "failed to stop associated datafeed after job failure");
                        }
                    }
                )
            );
        }, e -> {
            if (autodetectProcessManager.isNodeDying() == false) {
                logger.error(() -> "[" + jobId + "] failed to search for associated datafeed", e);
            }
        });

        getRunningDatafeed(jobId, getRunningDatafeedListener);
    }

    private void getRunningDatafeed(String jobId, ActionListener<String> listener) {
        ActionListener<Set<String>> datafeedListener = listener.delegateFailureAndWrap((delegate, datafeeds) -> {
            assert datafeeds.size() <= 1;
            if (datafeeds.isEmpty()) {
                delegate.onResponse(null);
                return;
            }

            String datafeedId = datafeeds.iterator().next();
            PersistentTasksCustomMetadata tasks = clusterState.getMetadata().getProject().custom(PersistentTasksCustomMetadata.TYPE);
            PersistentTasksCustomMetadata.PersistentTask<?> datafeedTask = MlTasks.getDatafeedTask(datafeedId, tasks);
            delegate.onResponse(datafeedTask != null ? datafeedId : null);
        });

        datafeedConfigProvider.findDatafeedIdsForJobIds(Collections.singleton(jobId), datafeedListener);
    }

    private void verifyCurrentSnapshotVersion(String jobId, ActionListener<Boolean> listener) {
        ActionListener<GetJobsAction.Response> jobListener = ActionListener.wrap(jobResponse -> {
            List<Job> jobPage = jobResponse.getResponse().results();
            // We requested a single concrete job so if it didn't exist we would get an error
            assert jobPage.size() == 1;
            String jobSnapshotId = jobPage.get(0).getModelSnapshotId();
            if (jobSnapshotId == null) {
                listener.onResponse(true);
                return;
            }
            executeAsyncWithOrigin(
                client,
                ML_ORIGIN,
                GetModelSnapshotsAction.INSTANCE,
                new GetModelSnapshotsAction.Request(jobId, jobSnapshotId),
                ActionListener.wrap(snapshot -> {
                    if (snapshot.getPage().count() == 0) {
                        listener.onResponse(true);
                        return;
                    }
                    assert snapshot.getPage().results().size() == 1;
                    ModelSnapshot snapshotObj = snapshot.getPage().results().get(0);
                    if (snapshotObj.getMinVersion().onOrAfter(MIN_SUPPORTED_SNAPSHOT_VERSION)) {
                        listener.onResponse(true);
                        return;
                    }
                    listener.onFailure(
                        ExceptionsHelper.badRequestException(
                            "[{}] job model snapshot [{}] has min version before [{}], "
                                + "please revert to a newer model snapshot or reset the job",
                            jobId,
                            jobSnapshotId,
                            MIN_SUPPORTED_SNAPSHOT_VERSION.toString()
                        )
                    );
                }, snapshotFailure -> {
                    if (ExceptionsHelper.unwrapCause(snapshotFailure) instanceof ResourceNotFoundException) {
                        listener.onResponse(true);
                        return;
                    }
                    listener.onFailure(
                        ExceptionsHelper.serverError("[{}] failed finding snapshot [{}]", snapshotFailure, jobId, jobSnapshotId)
                    );
                })
            );
        }, error -> listener.onFailure(ExceptionsHelper.serverError("[{}] error getting job", error, jobId)));
        GetJobsAction.Request request = new GetJobsAction.Request(jobId).masterNodeTimeout(PERSISTENT_TASK_MASTER_NODE_TIMEOUT);
        executeAsyncWithOrigin(client, ML_ORIGIN, GetJobsAction.INSTANCE, request, jobListener);
    }

    // Exceptions that occur while the node is dying, i.e. after the JVM has received a SIGTERM,
    // are ignored. Core services will be stopping in response to the SIGTERM and we want the
    // job to try to open again on another node, not spuriously fail on the dying node.
    private void openJob(JobTask jobTask, ActionListener<Boolean> listener) {
        String jobId = jobTask.getJobId();
        autodetectProcessManager.openJob(jobTask, clusterState, PERSISTENT_TASK_MASTER_NODE_TIMEOUT, (e2, shouldFinalizeJob) -> {
            if (e2 == null) {
                // Beyond this point it's too late to change our minds about whether we're closing or vacating
                if (jobTask.isVacating()) {
                    jobTask.markAsLocallyAborted(
                        "previously assigned node [" + clusterState.nodes().getLocalNode().getName() + "] is shutting down"
                    );
                } else if (shouldFinalizeJob) {
                    FinalizeJobExecutionAction.Request finalizeRequest = new FinalizeJobExecutionAction.Request(new String[] { jobId });
                    finalizeRequest.masterNodeTimeout(PERSISTENT_TASK_MASTER_NODE_TIMEOUT);
                    executeAsyncWithOrigin(
                        client,
                        ML_ORIGIN,
                        FinalizeJobExecutionAction.INSTANCE,
                        finalizeRequest,
                        ActionListener.wrap(response -> {
                            jobTask.markAsCompleted();
                            listener.onResponse(true);
                        }, e -> {
                            // This error is logged even if the node is dying. This is a nasty place for the node to get killed,
                            // as most of the job's close sequence has executed, just not the finalization step. The job will
                            // restart on a different node. If the coordinating node for the close request notices that the job
                            // changed nodes while waiting for it to close then it will remove the persistent task, which should
                            // stop the job doing anything significant on its new node. However, the finish time of the job will
                            // not be set correctly.
                            logger.error(() -> "[" + jobId + "] error finalizing job", e);
                            Throwable unwrapped = ExceptionsHelper.unwrapCause(e);
                            if (unwrapped instanceof DocumentMissingException || unwrapped instanceof ResourceNotFoundException) {
                                jobTask.markAsCompleted();
                                listener.onResponse(true);
                            } else if (autodetectProcessManager.isNodeDying() == false) {
                                // In this case we prefer to mark the task as failed, which means the job
                                // will appear closed. The reason is that the job closed successfully and
                                // we just failed to update some fields like the finish time. It is preferable
                                // to let the job close than setting it to failed.
                                jobTask.markAsFailed(e);
                                listener.onFailure(e);
                            }
                        })
                    );
                } else {
                    jobTask.markAsCompleted();
                    listener.onResponse(true);
                }
            } else if (autodetectProcessManager.isNodeDying() == false) {
                logger.error(() -> "[" + jobTask.getJobId() + "] failed to open job", e2);
                listener.onFailure(e2);
            }
        });
    }

    @Override
    protected AllocatedPersistentTask createTask(
        long id,
        String type,
        String action,
        TaskId parentTaskId,
        PersistentTasksCustomMetadata.PersistentTask<OpenJobAction.JobParams> persistentTask,
        Map<String, String> headers
    ) {
        return new JobTask(persistentTask.getParams().getJobId(), id, type, action, parentTaskId, headers, licenseState);
    }

    public static Optional<ElasticsearchException> checkAssignmentState(
        PersistentTasksCustomMetadata.Assignment assignment,
        String jobId,
        Logger logger
    ) {
        if (assignment != null
            && assignment.equals(PersistentTasksCustomMetadata.INITIAL_ASSIGNMENT) == false
            && assignment.isAssigned() == false) {
            // Assignment has failed on the master node despite passing our "fast fail" validation
            if (assignment.equals(AWAITING_UPGRADE)) {
                return Optional.of(makeCurrentlyBeingUpgradedException(logger, jobId));
            } else if (assignment.getExplanation().contains("[" + EnableAssignmentDecider.ALLOCATION_NONE_EXPLANATION + "]")) {
                return Optional.of(makeAssignmentsNotAllowedException(logger, jobId));
            } else {
                return Optional.of(makeNoSuitableNodesException(logger, jobId, assignment.getExplanation()));
            }
        }
        return Optional.empty();
    }

    static ElasticsearchException makeNoSuitableNodesException(Logger logger, String jobId, String explanation) {
        String msg = "Could not open job because no suitable nodes were found, allocation explanation [" + explanation + "]";
        logger.warn("[{}] {}", jobId, msg);
        Exception detail = new IllegalStateException(msg);
        return new ElasticsearchStatusException(
            "Could not open job because no ML nodes with sufficient capacity were found",
            RestStatus.TOO_MANY_REQUESTS,
            detail
        );
    }

    static ElasticsearchException makeAssignmentsNotAllowedException(Logger logger, String jobId) {
        String msg = "Cannot open jobs because persistent task assignment is disabled by the ["
            + EnableAssignmentDecider.CLUSTER_TASKS_ALLOCATION_ENABLE_SETTING.getKey()
            + "] setting";
        logger.warn("[{}] {}", jobId, msg);
        return new ElasticsearchStatusException(msg, RestStatus.TOO_MANY_REQUESTS);
    }

    static ElasticsearchException makeCurrentlyBeingUpgradedException(Logger logger, String jobId) {
        String msg = "Cannot open jobs when upgrade mode is enabled";
        logger.warn("[{}] {}", jobId, msg);
        return new ElasticsearchStatusException(msg, RestStatus.TOO_MANY_REQUESTS);
    }

    @Override
    protected String[] indicesOfInterest(OpenJobAction.JobParams params) {
        return indicesOfInterest(AnomalyDetectorsIndex.resultsWriteAlias(params.getJobId()));
    }

    @Override
    protected String getJobId(OpenJobAction.JobParams params) {
        return params.getJobId();
    }

    /**
     * Retries the entire job opening pipeline for system-initiated reassignments.
     *
     * Uses exponential backoff with:
     * - Initial delay: 5 seconds
     * - Max delay: 5 minutes
     * - Total timeout: from {@link MachineLearning#JOB_OPEN_RETRY_TIMEOUT} (default 60 minutes)
     *
     * Only applies to reassignments (detected via {@link JobTaskState#isStatusStale(PersistentTask)}).
     * User-initiated opens fail fast.
     */
    class OpenJobRetryableAction extends RetryableAction<Boolean> {

        private final JobTask jobTask;
        private final JobTaskState jobTaskState;
        private final OpenJobAction.JobParams params;

        OpenJobRetryableAction(
            JobTask jobTask,
            JobTaskState jobTaskState,
            OpenJobAction.JobParams params,
            ActionListener<Boolean> listener
        ) {
            super(
                logger,
                client.threadPool(),
                TimeValue.timeValueSeconds(5),
                TimeValue.timeValueMinutes(5),
                clusterSettings.get(MachineLearning.JOB_OPEN_RETRY_TIMEOUT),
                listener,
                client.threadPool().generic()
            );
            this.jobTask = Objects.requireNonNull(jobTask);
            this.jobTaskState = jobTaskState;
            this.params = Objects.requireNonNull(params);
        }

        @Override
        public void tryAction(ActionListener<Boolean> listener) {
            executeOpenJobPipeline(jobTask, jobTaskState, params, listener);
        }

        @Override
        public boolean shouldRetry(Exception e) {
            if (jobTask.isClosing() || jobTask.isVacating()) {
                return false;
            }
            return org.elasticsearch.xpack.ml.utils.MlRecoverableErrorClassifier.isRecoverable(e);
        }
    }
}
