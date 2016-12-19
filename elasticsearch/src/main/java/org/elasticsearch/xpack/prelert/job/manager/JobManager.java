/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
import org.elasticsearch.xpack.prelert.action.DeleteJobAction;
import org.elasticsearch.xpack.prelert.action.OpenJobAction;
import org.elasticsearch.xpack.prelert.action.PutJobAction;
import org.elasticsearch.xpack.prelert.action.RevertModelSnapshotAction;
import org.elasticsearch.xpack.prelert.action.UpdateJobStatusAction;
import org.elasticsearch.xpack.prelert.action.UpdateSchedulerStatusAction;
import org.elasticsearch.xpack.prelert.job.IgnoreDowntime;
import org.elasticsearch.xpack.prelert.job.Job;
import org.elasticsearch.xpack.prelert.job.JobStatus;
import org.elasticsearch.xpack.prelert.job.ModelSnapshot;
import org.elasticsearch.xpack.prelert.scheduler.SchedulerStatus;
import org.elasticsearch.xpack.prelert.job.audit.Auditor;
import org.elasticsearch.xpack.prelert.job.messages.Messages;
import org.elasticsearch.xpack.prelert.job.metadata.Allocation;
import org.elasticsearch.xpack.prelert.job.metadata.PrelertMetadata;
import org.elasticsearch.xpack.prelert.job.persistence.JobProvider;
import org.elasticsearch.xpack.prelert.job.persistence.JobResultsPersister;
import org.elasticsearch.xpack.prelert.job.persistence.QueryPage;
import org.elasticsearch.xpack.prelert.job.results.AnomalyRecord;
import org.elasticsearch.xpack.prelert.utils.ExceptionsHelper;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Allows interactions with jobs. The managed interactions include:
 * <ul>
 * <li>creation</li>
 * <li>deletion</li>
 * <li>updating</li>
 * <li>starting/stopping of scheduled jobs</li>
 * </ul>
 */
public class JobManager extends AbstractComponent {

    private static final Logger LOGGER = Loggers.getLogger(JobManager.class);

    /**
     * Field name in which to store the API version in the usage info
     */
    public static final String APP_VER_FIELDNAME = "appVer";

    public static final String DEFAULT_RECORD_SORT_FIELD = AnomalyRecord.PROBABILITY.getPreferredName();
    private final JobProvider jobProvider;
    private final ClusterService clusterService;
    private final JobResultsPersister jobResultsPersister;


    /**
     * Create a JobManager
     */
    public JobManager(Settings settings, JobProvider jobProvider, JobResultsPersister jobResultsPersister,
                      ClusterService clusterService) {
        super(settings);
        this.jobProvider = Objects.requireNonNull(jobProvider);
        this.clusterService = clusterService;
        this.jobResultsPersister = jobResultsPersister;
    }

    /**
     * Get the details of the specific job wrapped in a <code>Optional</code>
     *
     * @param jobId
     *            the jobId
     * @return An {@code Optional} containing the {@code Job} if a job
     *         with the given {@code jobId} exists, or an empty {@code Optional}
     *         otherwise
     */
    public QueryPage<Job> getJob(String jobId, ClusterState clusterState) {
        PrelertMetadata prelertMetadata = clusterState.getMetaData().custom(PrelertMetadata.TYPE);
        Job job = prelertMetadata.getJobs().get(jobId);
        if (job == null) {
            throw QueryPage.emptyQueryPage(Job.RESULTS_FIELD);
        }

        return new QueryPage<>(Collections.singletonList(job), 1, Job.RESULTS_FIELD);
    }

    /**
     * Get details of all Jobs.
     *
     * @param from
     *            Skip the first N Jobs. This parameter is for paging results if
     *            not required set to 0.
     * @param size
     *            Take only this number of Jobs
     * @return A query page object with hitCount set to the total number of jobs
     *         not the only the number returned here as determined by the
     *         <code>size</code> parameter.
     */
    public QueryPage<Job> getJobs(int from, int size, ClusterState clusterState) {
        PrelertMetadata prelertMetadata = clusterState.getMetaData().custom(PrelertMetadata.TYPE);
        List<Job> jobs = prelertMetadata.getJobs().entrySet().stream()
                .skip(from)
                .limit(size)
                .map(Map.Entry::getValue)
                .collect(Collectors.toList());
        return new QueryPage<>(jobs, prelertMetadata.getJobs().size(), Job.RESULTS_FIELD);
    }

    /**
     * Returns the non-null {@code Job} object for the given
     * {@code jobId} or throws
     * {@link org.elasticsearch.ResourceNotFoundException}
     *
     * @param jobId
     *            the jobId
     * @return the {@code Job} if a job with the given {@code jobId}
     *         exists
     * @throws org.elasticsearch.ResourceNotFoundException
     *             if there is no job with matching the given {@code jobId}
     */
    public Job getJobOrThrowIfUnknown(String jobId) {
        return getJobOrThrowIfUnknown(clusterService.state(), jobId);
    }

    public Allocation getJobAllocation(String jobId) {
        return getAllocation(clusterService.state(), jobId);
    }

    /**
     * Returns the non-null {@code Job} object for the given
     * {@code jobId} or throws
     * {@link org.elasticsearch.ResourceNotFoundException}
     *
     * @param jobId
     *            the jobId
     * @return the {@code Job} if a job with the given {@code jobId}
     *         exists
     * @throws org.elasticsearch.ResourceNotFoundException
     *             if there is no job with matching the given {@code jobId}
     */
    Job getJobOrThrowIfUnknown(ClusterState clusterState, String jobId) {
        PrelertMetadata prelertMetadata = clusterState.metaData().custom(PrelertMetadata.TYPE);
        Job job = prelertMetadata.getJobs().get(jobId);
        if (job == null) {
            throw ExceptionsHelper.missingJobException(jobId);
        }
        return job;
    }

    /**
     * Stores a job in the cluster state
     */
    public void putJob(PutJobAction.Request request, ActionListener<PutJobAction.Response> actionListener) {
        Job job = request.getJob();
        ActionListener<Boolean> delegateListener = ActionListener.wrap(jobSaved ->
                jobProvider.createJobRelatedIndices(job, new ActionListener<Boolean>() {
            @Override
            public void onResponse(Boolean indicesCreated) {
                audit(job.getId()).info(Messages.getMessage(Messages.JOB_AUDIT_CREATED));

                // Also I wonder if we need to audit log infra
                // structure in prelert as when we merge into xpack
                // we can use its audit trailing. See:
                // https://github.com/elastic/prelert-legacy/issues/48
                actionListener.onResponse(new PutJobAction.Response(jobSaved && indicesCreated, job));
            }

            @Override
            public void onFailure(Exception e) {
                actionListener.onFailure(e);

            }
        }), actionListener::onFailure);
        clusterService.submitStateUpdateTask("put-job-" + job.getId(),
                new AckedClusterStateUpdateTask<Boolean>(request, delegateListener) {

            @Override
            protected Boolean newResponse(boolean acknowledged) {
                return acknowledged;
            }

            @Override
            public ClusterState execute(ClusterState currentState) throws Exception {
                return innerPutJob(job, request.isOverwrite(), currentState);
            }

        });
    }

    ClusterState innerPutJob(Job job, boolean overwrite, ClusterState currentState) {
        PrelertMetadata.Builder builder = createPrelertMetadataBuilder(currentState);
        builder.putJob(job, overwrite);
        return buildNewClusterState(currentState, builder);
    }

    /**
     * Deletes a job.
     *
     * The clean-up involves:
     * <ul>
     * <li>Deleting the index containing job results</li>
     * <li>Deleting the job logs</li>
     * <li>Removing the job from the cluster state</li>
     * </ul>
     *
     * @param request
     *            the delete job request
     * @param actionListener
     *            the action listener
     */
    public void deleteJob(DeleteJobAction.Request request, ActionListener<DeleteJobAction.Response> actionListener) {
        String jobId = request.getJobId();
        LOGGER.debug("Deleting job '" + jobId + "'");

        ActionListener<Boolean> delegateListener = ActionListener.wrap(jobDeleted -> {
            if (jobDeleted) {
                jobProvider.deleteJobRelatedIndices(request.getJobId(), actionListener);
                audit(jobId).info(Messages.getMessage(Messages.JOB_AUDIT_DELETED));
            } else {
                actionListener.onResponse(new DeleteJobAction.Response(false));
            }
        }, actionListener::onFailure);
        clusterService.submitStateUpdateTask("delete-job-" + jobId,
                new AckedClusterStateUpdateTask<Boolean>(request, delegateListener) {

            @Override
            protected Boolean newResponse(boolean acknowledged) {
                return acknowledged;
            }

            @Override
            public ClusterState execute(ClusterState currentState) throws Exception {
                return removeJobFromClusterState(jobId, currentState);
            }
        });
    }

    ClusterState removeJobFromClusterState(String jobId, ClusterState currentState) {
        PrelertMetadata.Builder builder = createPrelertMetadataBuilder(currentState);
        builder.removeJob(jobId);
        return buildNewClusterState(currentState, builder);
    }

    public Optional<SchedulerStatus> getSchedulerStatus(String jobId) {
        PrelertMetadata prelertMetadata = clusterService.state().metaData().custom(PrelertMetadata.TYPE);
        return prelertMetadata.getSchedulerStatusByJobId(jobId);
    }

    public void updateSchedulerStatus(UpdateSchedulerStatusAction.Request request,
                                      ActionListener<UpdateSchedulerStatusAction.Response> actionListener) {
        String schedulerId = request.getSchedulerId();
        SchedulerStatus newStatus = request.getSchedulerStatus();
        clusterService.submitStateUpdateTask("update-scheduler-status-" + schedulerId,
                new AckedClusterStateUpdateTask<UpdateSchedulerStatusAction.Response>(request, actionListener) {

            @Override
            public ClusterState execute(ClusterState currentState) throws Exception {
                PrelertMetadata.Builder builder = createPrelertMetadataBuilder(currentState);
                builder.updateSchedulerStatus(schedulerId, newStatus);
                return buildNewClusterState(currentState, builder);
            }

            @Override
            protected UpdateSchedulerStatusAction.Response newResponse(boolean acknowledged) {
                return new UpdateSchedulerStatusAction.Response(acknowledged);
            }
        });
    }

    private Allocation getAllocation(ClusterState state, String jobId) {
        PrelertMetadata prelertMetadata = state.metaData().custom(PrelertMetadata.TYPE);
        Allocation allocation = prelertMetadata.getAllocations().get(jobId);
        if (allocation == null) {
            throw new ResourceNotFoundException("No allocation found for job with id [" + jobId + "]");
        }
        return allocation;
    }

    public Auditor audit(String jobId) {
        return jobProvider.audit(jobId);
    }

    public void revertSnapshot(RevertModelSnapshotAction.Request request, ActionListener<RevertModelSnapshotAction.Response> actionListener,
            ModelSnapshot modelSnapshot) {

        clusterService.submitStateUpdateTask("revert-snapshot-" + request.getJobId(),
                new AckedClusterStateUpdateTask<RevertModelSnapshotAction.Response>(request, actionListener) {

            @Override
            protected RevertModelSnapshotAction.Response newResponse(boolean acknowledged) {
                if (acknowledged) {
                    audit(request.getJobId())
                            .info(Messages.getMessage(Messages.JOB_AUDIT_REVERTED, modelSnapshot.getDescription()));
                    return new RevertModelSnapshotAction.Response(modelSnapshot);
                }
                throw new IllegalStateException("Could not revert modelSnapshot on job ["
                        + request.getJobId() + "], not acknowledged by master.");
            }

            @Override
            public ClusterState execute(ClusterState currentState) throws Exception {
                Job job = getJobOrThrowIfUnknown(currentState, request.getJobId());
                Job.Builder builder = new Job.Builder(job);
                builder.setModelSnapshotId(modelSnapshot.getSnapshotId());
                if (request.getDeleteInterveningResults()) {
                    builder.setIgnoreDowntime(IgnoreDowntime.NEVER);
                } else {
                    builder.setIgnoreDowntime(IgnoreDowntime.ONCE);
                }

                return innerPutJob(builder.build(), true, currentState);
            }
        });
    }

    public void openJob(OpenJobAction.Request request, ActionListener<OpenJobAction.Response> actionListener) {
        clusterService.submitStateUpdateTask("open-job-" + request.getJobId(),
                new AckedClusterStateUpdateTask<OpenJobAction.Response>(request, actionListener) {
            @Override
            public ClusterState execute(ClusterState currentState) throws Exception {
                PrelertMetadata.Builder builder = new PrelertMetadata.Builder(currentState.metaData().custom(PrelertMetadata.TYPE));
                builder.updateStatus(request.getJobId(), JobStatus.OPENING, null);
                if (request.isIgnoreDowntime()) {
                    builder.setIgnoreDowntime(request.getJobId());
                }
                return ClusterState.builder(currentState)
                        .metaData(MetaData.builder(currentState.metaData()).putCustom(PrelertMetadata.TYPE, builder.build()))
                        .build();
            }

            @Override
            protected OpenJobAction.Response newResponse(boolean acknowledged) {
                return new OpenJobAction.Response(acknowledged);
            }
        });
    }

    public JobStatus getJobStatus(String jobId) {
        return getJobAllocation(jobId).getStatus();
    }

    public void setJobStatus(UpdateJobStatusAction.Request request, ActionListener<UpdateJobStatusAction.Response> actionListener) {
        clusterService.submitStateUpdateTask("set-job-status-" + request.getStatus() + "-" + request.getJobId(),
                new AckedClusterStateUpdateTask<UpdateJobStatusAction.Response>(request, actionListener) {

                    @Override
                    public ClusterState execute(ClusterState currentState) throws Exception {
                        PrelertMetadata.Builder builder = new PrelertMetadata.Builder(currentState.metaData().custom(PrelertMetadata.TYPE));
                        builder.updateStatus(request.getJobId(), request.getStatus(), request.getReason());
                        return ClusterState.builder(currentState)
                                .metaData(MetaData.builder(currentState.metaData()).putCustom(PrelertMetadata.TYPE, builder.build()))
                                .build();
                    }

                    @Override
                    protected UpdateJobStatusAction.Response newResponse(boolean acknowledged) {
                        return new UpdateJobStatusAction.Response(acknowledged);
                    }
                });
    }

    /**
     * Update a persisted model snapshot metadata document to match the
     * argument supplied.
     *
     * @param jobId                 the job id
     * @param modelSnapshot         the updated model snapshot object to be stored
     * @param restoreModelSizeStats should the model size stats in this
     *                              snapshot be made the current ones for this job?
     */
    public void updateModelSnapshot(String jobId, ModelSnapshot modelSnapshot, boolean restoreModelSizeStats) {
        // For Elasticsearch the update can be done in exactly the same way as
        // the original persist
        jobResultsPersister.persistModelSnapshot(modelSnapshot);
        if (restoreModelSizeStats) {
            if (modelSnapshot.getModelSizeStats() != null) {
                jobResultsPersister.persistModelSizeStats(modelSnapshot.getModelSizeStats());
            }
            if (modelSnapshot.getQuantiles() != null) {
                jobResultsPersister.persistQuantiles(modelSnapshot.getQuantiles());
            }
        }
        // Commit so that when the REST API call that triggered the update
        // returns the updated document is searchable
        jobResultsPersister.commitWrites(jobId);
    }

    private static PrelertMetadata.Builder createPrelertMetadataBuilder(ClusterState currentState) {
        PrelertMetadata currentPrelertMetadata = currentState.metaData().custom(PrelertMetadata.TYPE);
        return new PrelertMetadata.Builder(currentPrelertMetadata);
    }

    private static ClusterState buildNewClusterState(ClusterState currentState, PrelertMetadata.Builder builder) {
        ClusterState.Builder newState = ClusterState.builder(currentState);
        newState.metaData(MetaData.builder(currentState.getMetaData()).putCustom(PrelertMetadata.TYPE, builder.build()).build());
        return newState.build();
    }

}
