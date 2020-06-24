/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.action;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.engine.DocumentMissingException;
import org.elasticsearch.license.LicenseUtils;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.persistent.AllocatedPersistentTask;
import org.elasticsearch.persistent.PersistentTaskState;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.persistent.PersistentTasksExecutor;
import org.elasticsearch.persistent.PersistentTasksService;
import org.elasticsearch.persistent.decider.EnableAssignmentDecider;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.XPackField;
import org.elasticsearch.xpack.core.ml.MlConfigIndex;
import org.elasticsearch.xpack.core.ml.MlMetaIndex;
import org.elasticsearch.xpack.core.ml.MlMetadata;
import org.elasticsearch.xpack.core.ml.MlTasks;
import org.elasticsearch.xpack.core.ml.action.FinalizeJobExecutionAction;
import org.elasticsearch.xpack.core.ml.action.NodeAcknowledgedResponse;
import org.elasticsearch.xpack.core.ml.action.OpenJobAction;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.core.ml.job.config.JobState;
import org.elasticsearch.xpack.core.ml.job.config.JobTaskState;
import org.elasticsearch.xpack.core.ml.job.config.JobUpdate;
import org.elasticsearch.xpack.core.ml.job.persistence.AnomalyDetectorsIndex;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.ml.MachineLearning;
import org.elasticsearch.xpack.ml.MlConfigMigrationEligibilityCheck;
import org.elasticsearch.xpack.ml.job.JobNodeSelector;
import org.elasticsearch.xpack.ml.job.persistence.JobConfigProvider;
import org.elasticsearch.xpack.ml.job.persistence.JobResultsProvider;
import org.elasticsearch.xpack.ml.job.process.autodetect.AutodetectProcessManager;
import org.elasticsearch.xpack.ml.process.MlMemoryTracker;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Predicate;

import static org.elasticsearch.xpack.core.ClientHelper.ML_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.executeAsyncWithOrigin;
import static org.elasticsearch.xpack.core.ml.MlTasks.AWAITING_UPGRADE;
import static org.elasticsearch.xpack.ml.MachineLearning.MAX_OPEN_JOBS_PER_NODE;

/*
 This class extends from TransportMasterNodeAction for cluster state observing purposes.
 The close job api also redirect the elected master node.
 The master node will wait for the job to be opened by checking the persistent task's status and then return.
 To ensure that a subsequent close job call will see that same task status (and sanity validation doesn't fail)
 both open and close job apis redirect to the elected master node.
 In case of instability persistent tasks checks may fail and that is ok, in that case all bets are off.
 The open job api is a low through put api, so the fact that we redirect to elected master node shouldn't be an issue.
*/
public class TransportOpenJobAction extends TransportMasterNodeAction<OpenJobAction.Request, NodeAcknowledgedResponse> {

    private static final Logger logger = LogManager.getLogger(TransportOpenJobAction.class);

    static final PersistentTasksCustomMetadata.Assignment AWAITING_MIGRATION =
            new PersistentTasksCustomMetadata.Assignment(null, "job cannot be assigned until it has been migrated.");

    private final XPackLicenseState licenseState;
    private final PersistentTasksService persistentTasksService;
    private final JobConfigProvider jobConfigProvider;
    private final MlMemoryTracker memoryTracker;
    private final MlConfigMigrationEligibilityCheck migrationEligibilityCheck;

    @Inject
    public TransportOpenJobAction(Settings settings, TransportService transportService, ThreadPool threadPool,
                                  XPackLicenseState licenseState, ClusterService clusterService,
                                  PersistentTasksService persistentTasksService, ActionFilters actionFilters,
                                  IndexNameExpressionResolver indexNameExpressionResolver,
                                  JobConfigProvider jobConfigProvider, MlMemoryTracker memoryTracker) {
        super(OpenJobAction.NAME, transportService, clusterService, threadPool, actionFilters,OpenJobAction.Request::new,
            indexNameExpressionResolver);
        this.licenseState = licenseState;
        this.persistentTasksService = persistentTasksService;
        this.jobConfigProvider = jobConfigProvider;
        this.memoryTracker = memoryTracker;
        this.migrationEligibilityCheck = new MlConfigMigrationEligibilityCheck(settings, clusterService);
    }

    /**
     * Validations to fail fast before trying to update the job state on master node:
     * <ul>
     *     <li>check job exists</li>
     *     <li>check job is not marked as deleted</li>
     *     <li>check job's version is supported</li>
     * </ul>
     */
    static void validate(String jobId, Job job) {
        if (job == null) {
            throw ExceptionsHelper.missingJobException(jobId);
        }
        if (job.isDeleting()) {
            throw ExceptionsHelper.conflictStatusException("Cannot open job [" + jobId + "] because it is being deleted");
        }
        if (job.getJobVersion() == null) {
            throw ExceptionsHelper.badRequestException("Cannot open job [" + jobId
                    + "] because jobs created prior to version 5.5 are not supported");
        }
    }

    static String[] indicesOfInterest(String resultsIndex) {
        if (resultsIndex == null) {
            return new String[]{AnomalyDetectorsIndex.jobStateIndexPattern(), MlMetaIndex.indexName(),
                MlConfigIndex.indexName()};
        }
        return new String[]{AnomalyDetectorsIndex.jobStateIndexPattern(), resultsIndex, MlMetaIndex.indexName(),
            MlConfigIndex.indexName()};
    }

    static List<String> verifyIndicesPrimaryShardsAreActive(String resultsWriteIndex, ClusterState clusterState,
                                                            IndexNameExpressionResolver expressionResolver) {
        String[] indices = expressionResolver.concreteIndexNames(clusterState, IndicesOptions.lenientExpandOpen(),
            indicesOfInterest(resultsWriteIndex));
        List<String> unavailableIndices = new ArrayList<>(indices.length);
        for (String index : indices) {
            // Indices are created on demand from templates.
            // It is not an error if the index doesn't exist yet
            if (clusterState.metadata().hasIndex(index) == false) {
                continue;
            }
            IndexRoutingTable routingTable = clusterState.getRoutingTable().index(index);
            if (routingTable == null || routingTable.allPrimaryShardsActive() == false) {
                unavailableIndices.add(index);
            }
        }
        return unavailableIndices;
    }

    private static boolean nodeSupportsModelSnapshotVersion(DiscoveryNode node, Job job) {
        if (job.getModelSnapshotId() == null || job.getModelSnapshotMinVersion() == null) {
            // There is no snapshot to restore or the min model snapshot version is 5.5.0
            // which is OK as we have already checked the node is >= 5.5.0.
            return true;
        }
        return node.getVersion().onOrAfter(job.getModelSnapshotMinVersion());
    }

    public static String nodeFilter(DiscoveryNode node, Job job) {

        String jobId = job.getId();

        if (TransportOpenJobAction.nodeSupportsModelSnapshotVersion(node, job) == false) {
            return "Not opening job [" + jobId + "] on node [" + JobNodeSelector.nodeNameAndVersion(node)
                + "], because the job's model snapshot requires a node of version ["
                + job.getModelSnapshotMinVersion() + "] or higher";
        }

        if (Job.getCompatibleJobTypes(node.getVersion()).contains(job.getJobType()) == false) {
            return "Not opening job [" + jobId + "] on node [" + JobNodeSelector.nodeNameAndVersion(node) +
                "], because this node does not support jobs of type [" + job.getJobType() + "]";
        }

        return null;
    }

    @Override
    protected String executor() {
        // This api doesn't do heavy or blocking operations (just delegates PersistentTasksService),
        // so we can do this on the network thread
        return ThreadPool.Names.SAME;
    }

    @Override
    protected NodeAcknowledgedResponse read(StreamInput in) throws IOException {
        return new NodeAcknowledgedResponse(in);
    }

    @Override
    protected ClusterBlockException checkBlock(OpenJobAction.Request request, ClusterState state) {
        // We only delegate here to PersistentTasksService, but if there is a metadata writeblock,
        // then delegating to PersistentTasksService doesn't make a whole lot of sense,
        // because PersistentTasksService will then fail.
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }

    @Override
    protected void masterOperation(Task task, OpenJobAction.Request request, ClusterState state,
                                   ActionListener<NodeAcknowledgedResponse> listener) {
        if (migrationEligibilityCheck.jobIsEligibleForMigration(request.getJobParams().getJobId(), state)) {
            listener.onFailure(ExceptionsHelper.configHasNotBeenMigrated("open job", request.getJobParams().getJobId()));
            return;
        }

        OpenJobAction.JobParams jobParams = request.getJobParams();
        if (licenseState.isAllowed(XPackLicenseState.Feature.MACHINE_LEARNING)) {

            // Clear job finished time once the job is started and respond
            ActionListener<NodeAcknowledgedResponse> clearJobFinishTime = ActionListener.wrap(
                response -> {
                    if (response.isAcknowledged()) {
                        clearJobFinishedTime(response, jobParams.getJobId(), listener);
                    } else {
                        listener.onResponse(response);
                    }
                },
                listener::onFailure
            );

            // Wait for job to be started
            ActionListener<PersistentTasksCustomMetadata.PersistentTask<OpenJobAction.JobParams>> waitForJobToStart =
                    new ActionListener<PersistentTasksCustomMetadata.PersistentTask<OpenJobAction.JobParams>>() {
                @Override
                public void onResponse(PersistentTasksCustomMetadata.PersistentTask<OpenJobAction.JobParams> task) {
                    waitForJobStarted(task.getId(), jobParams, clearJobFinishTime);
                }

                @Override
                public void onFailure(Exception e) {
                    if (ExceptionsHelper.unwrapCause(e) instanceof ResourceAlreadyExistsException) {
                        e = new ElasticsearchStatusException("Cannot open job [" + jobParams.getJobId() +
                                "] because it has already been opened", RestStatus.CONFLICT, e);
                    }
                    listener.onFailure(e);
                }
            };

            // Start job task
            ActionListener<Long> memoryRequirementRefreshListener = ActionListener.wrap(
                mem -> persistentTasksService.sendStartRequest(MlTasks.jobTaskId(jobParams.getJobId()), MlTasks.JOB_TASK_NAME, jobParams,
                    waitForJobToStart),
                listener::onFailure
            );

            // Tell the job tracker to refresh the memory requirement for this job and all other jobs that have persistent tasks
            ActionListener<Boolean> getJobHandler = ActionListener.wrap(
                response -> memoryTracker.refreshAnomalyDetectorJobMemoryAndAllOthers(jobParams.getJobId(),
                    memoryRequirementRefreshListener),
                listener::onFailure
            );

            // Get the job config
            jobConfigProvider.getJob(jobParams.getJobId(), ActionListener.wrap(
                    builder -> {
                        jobParams.setJob(builder.build());
                        getJobHandler.onResponse(null);
                    },
                    listener::onFailure
            ));
        } else {
            listener.onFailure(LicenseUtils.newComplianceException(XPackField.MACHINE_LEARNING));
        }
    }

    private void waitForJobStarted(String taskId, OpenJobAction.JobParams jobParams, ActionListener<NodeAcknowledgedResponse> listener) {
        JobPredicate predicate = new JobPredicate();
        persistentTasksService.waitForPersistentTaskCondition(taskId, predicate, jobParams.getTimeout(),
                new PersistentTasksService.WaitForPersistentTaskListener<OpenJobAction.JobParams>() {
            @Override
            public void onResponse(PersistentTasksCustomMetadata.PersistentTask<OpenJobAction.JobParams> persistentTask) {
                if (predicate.exception != null) {
                    if (predicate.shouldCancel) {
                        // We want to return to the caller without leaving an unassigned persistent task, to match
                        // what would have happened if the error had been detected in the "fast fail" validation
                        cancelJobStart(persistentTask, predicate.exception, listener);
                    } else {
                        listener.onFailure(predicate.exception);
                    }
                } else {
                    listener.onResponse(new NodeAcknowledgedResponse(true, predicate.node));
                }
            }

            @Override
            public void onFailure(Exception e) {
                listener.onFailure(e);
            }

            @Override
            public void onTimeout(TimeValue timeout) {
                listener.onFailure(new ElasticsearchException("Opening job ["
                        + jobParams.getJobId() + "] timed out after [" + timeout + "]"));
            }
        });
    }

    private void clearJobFinishedTime(NodeAcknowledgedResponse response, String jobId, ActionListener<NodeAcknowledgedResponse> listener) {
        JobUpdate update = new JobUpdate.Builder(jobId).setClearFinishTime(true).build();

        jobConfigProvider.updateJob(jobId, update, null, ActionListener.wrap(
                job -> listener.onResponse(response),
                e  -> {
                    logger.error("[" + jobId + "] Failed to clear finished_time", e);
                    // Not a critical error so continue
                    listener.onResponse(response);
                }
        ));
    }

    private void cancelJobStart(PersistentTasksCustomMetadata.PersistentTask<OpenJobAction.JobParams> persistentTask, Exception exception,
                                ActionListener<NodeAcknowledgedResponse> listener) {
        persistentTasksService.sendRemoveRequest(persistentTask.getId(),
                new ActionListener<PersistentTasksCustomMetadata.PersistentTask<?>>() {
                    @Override
                    public void onResponse(PersistentTasksCustomMetadata.PersistentTask<?> task) {
                        // We succeeded in cancelling the persistent task, but the
                        // problem that caused us to cancel it is the overall result
                        listener.onFailure(exception);
                    }

                    @Override
                    public void onFailure(Exception e) {
                        logger.error("[" + persistentTask.getParams().getJobId() + "] Failed to cancel persistent task that could " +
                                "not be assigned due to [" + exception.getMessage() + "]", e);
                        listener.onFailure(exception);
                    }
                }
        );
    }

    public static class OpenJobPersistentTasksExecutor extends PersistentTasksExecutor<OpenJobAction.JobParams> {

        private static final Logger logger = LogManager.getLogger(OpenJobPersistentTasksExecutor.class);

        private final AutodetectProcessManager autodetectProcessManager;
        private final MlMemoryTracker memoryTracker;
        private final Client client;
        private final IndexNameExpressionResolver expressionResolver;
        private final JobResultsProvider jobResultsProvider;

        private volatile int maxConcurrentJobAllocations;
        private volatile int maxMachineMemoryPercent;
        private volatile int maxLazyMLNodes;
        private volatile int maxOpenJobs;
        private volatile ClusterState clusterState;

        public OpenJobPersistentTasksExecutor(Settings settings, ClusterService clusterService,
                                              AutodetectProcessManager autodetectProcessManager, MlMemoryTracker memoryTracker,
                                              Client client, IndexNameExpressionResolver expressionResolver) {
            super(MlTasks.JOB_TASK_NAME, MachineLearning.UTILITY_THREAD_POOL_NAME);
            this.autodetectProcessManager = Objects.requireNonNull(autodetectProcessManager);
            this.memoryTracker = Objects.requireNonNull(memoryTracker);
            this.client = Objects.requireNonNull(client);
            this.expressionResolver = Objects.requireNonNull(expressionResolver);
            this.jobResultsProvider = new JobResultsProvider(client, settings, expressionResolver);
            this.maxConcurrentJobAllocations = MachineLearning.CONCURRENT_JOB_ALLOCATIONS.get(settings);
            this.maxMachineMemoryPercent = MachineLearning.MAX_MACHINE_MEMORY_PERCENT.get(settings);
            this.maxLazyMLNodes = MachineLearning.MAX_LAZY_ML_NODES.get(settings);
            this.maxOpenJobs = MAX_OPEN_JOBS_PER_NODE.get(settings);
            clusterService.getClusterSettings()
                    .addSettingsUpdateConsumer(MachineLearning.CONCURRENT_JOB_ALLOCATIONS, this::setMaxConcurrentJobAllocations);
            clusterService.getClusterSettings()
                    .addSettingsUpdateConsumer(MachineLearning.MAX_MACHINE_MEMORY_PERCENT, this::setMaxMachineMemoryPercent);
            clusterService.getClusterSettings().addSettingsUpdateConsumer(MachineLearning.MAX_LAZY_ML_NODES, this::setMaxLazyMLNodes);
            clusterService.getClusterSettings().addSettingsUpdateConsumer(MAX_OPEN_JOBS_PER_NODE, this::setMaxOpenJobs);
            clusterService.addListener(event -> clusterState = event.state());
        }

        @Override
        public PersistentTasksCustomMetadata.Assignment getAssignment(OpenJobAction.JobParams params, ClusterState clusterState) {

            // If the task parameters do not have a job field then the job
            // was first opened on a pre v6.6 node and has not been migrated
            Job job = params.getJob();
            if (job == null) {
                return AWAITING_MIGRATION;
            }

            // If we are waiting for an upgrade to complete, we should not assign to a node
            if (MlMetadata.getMlMetadata(clusterState).isUpgradeMode()) {
                return AWAITING_UPGRADE;
            }

            String jobId = params.getJobId();
            String resultsWriteAlias = AnomalyDetectorsIndex.resultsWriteAlias(jobId);
            List<String> unavailableIndices = verifyIndicesPrimaryShardsAreActive(resultsWriteAlias, clusterState, expressionResolver);
            if (unavailableIndices.size() != 0) {
                String reason = "Not opening job [" + jobId + "], because not all primary shards are active for the following indices [" +
                    String.join(",", unavailableIndices) + "]";
                logger.debug(reason);
                return new PersistentTasksCustomMetadata.Assignment(null, reason);
            }

            boolean isMemoryTrackerRecentlyRefreshed = memoryTracker.isRecentlyRefreshed();
            if (isMemoryTrackerRecentlyRefreshed == false) {
                boolean scheduledRefresh = memoryTracker.asyncRefresh();
                if (scheduledRefresh) {
                    String reason = "Not opening job [" + jobId + "] because job memory requirements are stale - refresh requested";
                    logger.debug(reason);
                    return new PersistentTasksCustomMetadata.Assignment(null, reason);
                }
            }

            JobNodeSelector jobNodeSelector = new JobNodeSelector(clusterState, jobId, MlTasks.JOB_TASK_NAME, memoryTracker,
                job.allowLazyOpen() ? Integer.MAX_VALUE : maxLazyMLNodes, node -> nodeFilter(node, job));
            return jobNodeSelector.selectNode(
                maxOpenJobs, maxConcurrentJobAllocations, maxMachineMemoryPercent, isMemoryTrackerRecentlyRefreshed);
        }

        @Override
        public void validate(OpenJobAction.JobParams params, ClusterState clusterState) {

            TransportOpenJobAction.validate(params.getJobId(), params.getJob());

            // If we already know that we can't find an ml node because all ml nodes are running at capacity or
            // simply because there are no ml nodes in the cluster then we fail quickly here:
            PersistentTasksCustomMetadata.Assignment assignment = getAssignment(params, clusterState);
            if (assignment.equals(AWAITING_UPGRADE)) {
                throw makeCurrentlyBeingUpgradedException(logger, params.getJobId(), assignment.getExplanation());
            }

            if (assignment.getExecutorNode() == null && assignment.equals(JobNodeSelector.AWAITING_LAZY_ASSIGNMENT) == false) {
                throw makeNoSuitableNodesException(logger, params.getJobId(), assignment.getExplanation());
            }
        }

        @Override
        protected void nodeOperation(AllocatedPersistentTask task, OpenJobAction.JobParams params, PersistentTaskState state) {
            JobTask jobTask = (JobTask) task;
            jobTask.autodetectProcessManager = autodetectProcessManager;
            JobTaskState jobTaskState = (JobTaskState) state;
            JobState jobState = jobTaskState == null ? null : jobTaskState.getState();
            jobResultsProvider.setRunningForecastsToFailed(params.getJobId(), ActionListener.wrap(
                r -> runJob(jobTask, jobState, params),
                e -> {
                    logger.warn(new ParameterizedMessage("[{}] failed to set forecasts to failed", params.getJobId()), e);
                    runJob(jobTask, jobState, params);
                }
            ));
        }

        private void runJob(JobTask jobTask, JobState jobState, OpenJobAction.JobParams params) {
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

            String jobId = jobTask.getJobId();
            autodetectProcessManager.openJob(jobTask, clusterState, (e2, shouldFinalizeJob) -> {
                if (e2 == null) {
                    if (shouldFinalizeJob) {
                        FinalizeJobExecutionAction.Request finalizeRequest = new FinalizeJobExecutionAction.Request(new String[]{jobId});
                        executeAsyncWithOrigin(client, ML_ORIGIN, FinalizeJobExecutionAction.INSTANCE, finalizeRequest,
                            ActionListener.wrap(
                                response -> jobTask.markAsCompleted(),
                                e -> {
                                    logger.error("error finalizing job [" + jobId + "]", e);
                                    Throwable unwrapped = ExceptionsHelper.unwrapCause(e);
                                    if (unwrapped instanceof DocumentMissingException || unwrapped instanceof ResourceNotFoundException) {
                                        jobTask.markAsCompleted();
                                    } else {
                                        jobTask.markAsFailed(e);
                                    }
                                }
                            ));
                    } else {
                        jobTask.markAsCompleted();
                    }
                } else {
                    jobTask.markAsFailed(e2);
                }
            });
        }

        @Override
        protected AllocatedPersistentTask createTask(long id, String type, String action, TaskId parentTaskId,
                                                     PersistentTasksCustomMetadata.PersistentTask<OpenJobAction.JobParams> persistentTask,
                                                     Map<String, String> headers) {
            return new JobTask(persistentTask.getParams().getJobId(), id, type, action, parentTaskId, headers);
        }

        void setMaxConcurrentJobAllocations(int maxConcurrentJobAllocations) {
            this.maxConcurrentJobAllocations = maxConcurrentJobAllocations;
        }

        void setMaxMachineMemoryPercent(int maxMachineMemoryPercent) {
            this.maxMachineMemoryPercent = maxMachineMemoryPercent;
        }

        void setMaxLazyMLNodes(int maxLazyMLNodes) {
            this.maxLazyMLNodes = maxLazyMLNodes;
        }

        void setMaxOpenJobs(int maxOpenJobs) {
            this.maxOpenJobs = maxOpenJobs;
        }
    }

    public static class JobTask extends AllocatedPersistentTask implements OpenJobAction.JobTaskMatcher {

        private static final Logger LOGGER = LogManager.getLogger(JobTask.class);

        private final String jobId;
        private volatile AutodetectProcessManager autodetectProcessManager;

        JobTask(String jobId, long id, String type, String action, TaskId parentTask, Map<String, String> headers) {
            super(id, type, action, "job-" + jobId, parentTask, headers);
            this.jobId = jobId;
        }

        public String getJobId() {
            return jobId;
        }

        @Override
        protected void onCancelled() {
            String reason = getReasonCancelled();
            LOGGER.trace("[{}] Cancelling job task because: {}", jobId, reason);
            killJob(reason);
        }

        void killJob(String reason) {
            autodetectProcessManager.killProcess(this, false, reason);
        }

        void closeJob(String reason) {
            autodetectProcessManager.closeJob(this, false, reason);
        }
    }

    /**
     * This class contains the wait logic for waiting for a job's persistent task to be allocated on
     * job opening.  It should only be used in the open job action, and never at other times the job's
     * persistent task may be assigned to a node, for example on recovery from node failures.
     *
     * Important: the methods of this class must NOT throw exceptions.  If they did then the callers
     * of endpoints waiting for a condition tested by this predicate would never get a response.
     */
    private static class JobPredicate implements Predicate<PersistentTasksCustomMetadata.PersistentTask<?>> {

        private volatile Exception exception;
        private volatile String node = "";
        private volatile boolean shouldCancel;

        @Override
        public boolean test(PersistentTasksCustomMetadata.PersistentTask<?> persistentTask) {
            JobState jobState = JobState.CLOSED;
            if (persistentTask != null) {
                JobTaskState jobTaskState = (JobTaskState) persistentTask.getState();
                jobState = jobTaskState == null ? JobState.OPENING : jobTaskState.getState();

                PersistentTasksCustomMetadata.Assignment assignment = persistentTask.getAssignment();

                // This means we are awaiting a new node to be spun up, ok to return back to the user to await node creation
                if (assignment != null && assignment.equals(JobNodeSelector.AWAITING_LAZY_ASSIGNMENT)) {
                    return true;
                }

                // This logic is only appropriate when opening a job, not when reallocating following a failure,
                // and this is why this class must only be used when opening a job
                if (assignment != null && assignment.equals(PersistentTasksCustomMetadata.INITIAL_ASSIGNMENT) == false &&
                        assignment.isAssigned() == false) {
                    OpenJobAction.JobParams params = (OpenJobAction.JobParams) persistentTask.getParams();
                    // Assignment has failed on the master node despite passing our "fast fail" validation
                    if (assignment.equals(AWAITING_UPGRADE)) {
                        exception = makeCurrentlyBeingUpgradedException(logger, params.getJobId(), assignment.getExplanation());
                    } else if (assignment.getExplanation().contains("[" + EnableAssignmentDecider.ALLOCATION_NONE_EXPLANATION + "]")) {
                        exception = makeAssignmentsNotAllowedException(logger, params.getJobId());
                    } else {
                        exception = makeNoSuitableNodesException(logger, params.getJobId(), assignment.getExplanation());
                    }
                    // The persistent task should be cancelled so that the observed outcome is the
                    // same as if the "fast fail" validation on the coordinating node had failed
                    shouldCancel = true;
                    return true;
                }
            }
            switch (jobState) {
                // The OPENING case here is expected to be incredibly short-lived, just occurring during the
                // time period when a job has successfully been assigned to a node but the request to update
                // its task state is still in-flight.  (The long-lived OPENING case when a lazy node needs to
                // be added to the cluster to accommodate the job was dealt with higher up this method when the
                // magic AWAITING_LAZY_ASSIGNMENT assignment was checked for.)
                case OPENING:
                case CLOSED:
                    return false;
                case OPENED:
                    node = persistentTask.getExecutorNode();
                    return true;
                case CLOSING:
                    exception = ExceptionsHelper.conflictStatusException("The job has been " + JobState.CLOSED + " while waiting to be "
                            + JobState.OPENED);
                    return true;
                case FAILED:
                default:
                    exception = ExceptionsHelper.serverError("Unexpected job state [" + jobState
                            + "] while waiting for job to be " + JobState.OPENED);
                    return true;
            }
        }
    }

    static ElasticsearchException makeNoSuitableNodesException(Logger logger, String jobId, String explanation) {
        String msg = "Could not open job because no suitable nodes were found, allocation explanation [" + explanation + "]";
        logger.warn("[{}] {}", jobId, msg);
        Exception detail = new IllegalStateException(msg);
        return new ElasticsearchStatusException("Could not open job because no ML nodes with sufficient capacity were found",
            RestStatus.TOO_MANY_REQUESTS, detail);
    }

    static ElasticsearchException makeAssignmentsNotAllowedException(Logger logger, String jobId) {
        String msg = "Cannot open jobs because persistent task assignment is disabled by the ["
            + EnableAssignmentDecider.CLUSTER_TASKS_ALLOCATION_ENABLE_SETTING.getKey() + "] setting";
        logger.warn("[{}] {}", jobId, msg);
        return new ElasticsearchStatusException(msg, RestStatus.TOO_MANY_REQUESTS);
    }

    static ElasticsearchException makeCurrentlyBeingUpgradedException(Logger logger, String jobId, String explanation) {
        String msg = "Cannot open jobs when upgrade mode is enabled";
        logger.warn("[{}] {}", jobId, msg);
        return new ElasticsearchStatusException(msg, RestStatus.TOO_MANY_REQUESTS);
    }
}
