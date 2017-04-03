/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.AckedClusterStateUpdateTask;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.ack.AckedRequest;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.CheckedConsumer;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.xpack.ml.MlMetadata;
import org.elasticsearch.xpack.ml.action.DeleteJobAction;
import org.elasticsearch.xpack.ml.action.PutJobAction;
import org.elasticsearch.xpack.ml.action.RevertModelSnapshotAction;
import org.elasticsearch.xpack.ml.action.util.QueryPage;
import org.elasticsearch.xpack.ml.job.config.Job;
import org.elasticsearch.xpack.ml.job.config.JobState;
import org.elasticsearch.xpack.ml.job.config.JobUpdate;
import org.elasticsearch.xpack.ml.job.messages.Messages;
import org.elasticsearch.xpack.ml.job.persistence.AnomalyDetectorsIndex;
import org.elasticsearch.xpack.ml.job.persistence.JobProvider;
import org.elasticsearch.xpack.ml.job.persistence.JobResultsPersister;
import org.elasticsearch.xpack.ml.job.persistence.JobStorageDeletionTask;
import org.elasticsearch.xpack.ml.job.process.autodetect.state.ModelSnapshot;
import org.elasticsearch.xpack.ml.notifications.Auditor;
import org.elasticsearch.xpack.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.persistent.PersistentTasksCustomMetaData;

import java.io.IOException;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * Allows interactions with jobs. The managed interactions include:
 * <ul>
 * <li>creation</li>
 * <li>deletion</li>
 * <li>updating</li>
 * <li>starting/stopping of datafeed jobs</li>
 * </ul>
 */
public class JobManager extends AbstractComponent {

    private final JobProvider jobProvider;
    private final ClusterService clusterService;
    private final Auditor auditor;
    private final Client client;
    private final UpdateJobProcessNotifier updateJobProcessNotifier;

    /**
     * Create a JobManager
     */
    public JobManager(Settings settings, JobProvider jobProvider, ClusterService clusterService, Auditor auditor, Client client,
                      UpdateJobProcessNotifier updateJobProcessNotifier) {
        super(settings);
        this.jobProvider = Objects.requireNonNull(jobProvider);
        this.clusterService = Objects.requireNonNull(clusterService);
        this.auditor = Objects.requireNonNull(auditor);
        this.client = Objects.requireNonNull(client);
        this.updateJobProcessNotifier = updateJobProcessNotifier;
    }

    /**
     * Get the jobs that match the given {@code jobId}.
     * Note that when the {@code jocId} is {@link Job#ALL} all jobs are returned.
     *
     * @param jobId
     *            the jobId
     * @return A {@link QueryPage} containing the matching {@code Job}s
     */
    public QueryPage<Job> getJob(String jobId, ClusterState clusterState) {
        if (jobId.equals(Job.ALL)) {
            return getJobs(clusterState);
        }
        MlMetadata mlMetadata = clusterState.getMetaData().custom(MlMetadata.TYPE);
        Job job = mlMetadata.getJobs().get(jobId);
        if (job == null) {
            logger.debug(String.format(Locale.ROOT, "Cannot find job '%s'", jobId));
            throw ExceptionsHelper.missingJobException(jobId);
        }

        logger.debug("Returning job [" + jobId + "]");
        return new QueryPage<>(Collections.singletonList(job), 1, Job.RESULTS_FIELD);
    }

    /**
     * Get details of all Jobs.
     *
     * @return A query page object with hitCount set to the total number of jobs
     *         not the only the number returned here as determined by the
     *         <code>size</code> parameter.
     */
    public QueryPage<Job> getJobs(ClusterState clusterState) {
        MlMetadata mlMetadata = clusterState.getMetaData().custom(MlMetadata.TYPE);
        List<Job> jobs = mlMetadata.getJobs().entrySet().stream()
                .map(Map.Entry::getValue)
                .collect(Collectors.toList());
        return new QueryPage<>(jobs, mlMetadata.getJobs().size(), Job.RESULTS_FIELD);
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

    public JobState getJobState(String jobId) {
        PersistentTasksCustomMetaData tasks = clusterService.state().getMetaData().custom(PersistentTasksCustomMetaData.TYPE);
        return MlMetadata.getJobState(jobId, tasks);
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
    public static Job getJobOrThrowIfUnknown(ClusterState clusterState, String jobId) {
        MlMetadata mlMetadata = clusterState.metaData().custom(MlMetadata.TYPE);
        Job job = mlMetadata.getJobs().get(jobId);
        if (job == null) {
            throw ExceptionsHelper.missingJobException(jobId);
        }
        return job;
    }

    /**
     * Stores a job in the cluster state
     */
    public void putJob(PutJobAction.Request request, ClusterState state, ActionListener<PutJobAction.Response> actionListener) {
        Job job = request.getJob().build(new Date());

        ActionListener<Boolean> createResultsIndexListener = ActionListener.wrap(jobSaved ->
                jobProvider.createJobResultIndex(job, state, new ActionListener<Boolean>() {
            @Override
            public void onResponse(Boolean indicesCreated) {
                auditor.info(job.getId(), Messages.getMessage(Messages.JOB_AUDIT_CREATED));

                // Also I wonder if we need to audit log infra
                // structure in ml as when we merge into xpack
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
                new AckedClusterStateUpdateTask<Boolean>(request, createResultsIndexListener) {
                    @Override
                    protected Boolean newResponse(boolean acknowledged) {
                        return acknowledged;
                    }

                    @Override
                    public ClusterState execute(ClusterState currentState) throws Exception {
                        return updateClusterState(job, false, currentState);
                    }
                });
    }

    public void updateJob(String jobId, JobUpdate jobUpdate, AckedRequest request, ActionListener<PutJobAction.Response> actionListener) {
        Job job = getJobOrThrowIfUnknown(jobId);
        validate(jobUpdate, job, isValid -> {
            if (isValid) {
                internalJobUpdate(jobId, jobUpdate, request, actionListener);
            } else {
                actionListener.onFailure(new IllegalArgumentException("Invalid update to job [" + jobId + "]"));
            }
        }, actionListener::onFailure);
    }

    private void validate(JobUpdate jobUpdate, Job job, Consumer<Boolean> handler, Consumer<Exception> errorHandler) {
        if (jobUpdate.getModelSnapshotId() != null) {
            jobProvider.getModelSnapshot(job.getId(), jobUpdate.getModelSnapshotId(), newModelSnapshot -> {
                if (newModelSnapshot == null) {
                    String message = Messages.getMessage(Messages.REST_NO_SUCH_MODEL_SNAPSHOT, jobUpdate.getModelSnapshotId(),
                            job.getId());
                    errorHandler.accept(new ResourceNotFoundException(message));
                }
                jobProvider.getModelSnapshot(job.getId(), job.getModelSnapshotId(), oldModelSnapshot -> {
                    if (oldModelSnapshot != null && newModelSnapshot.getTimestamp().before(oldModelSnapshot.getTimestamp())) {
                        String message = "Job [" + job.getId() + "] has a more recent model snapshot [" +
                                oldModelSnapshot.getSnapshotId() + "]";
                        errorHandler.accept(new IllegalArgumentException(message));
                    }
                    handler.accept(true);
                }, errorHandler);
            }, errorHandler);
        } else {
            handler.accept(true);
        }
    }

    private void internalJobUpdate(String jobId, JobUpdate jobUpdate, AckedRequest request,
                                   ActionListener<PutJobAction.Response> actionListener) {
        clusterService.submitStateUpdateTask("update-job-" + jobId,
                new AckedClusterStateUpdateTask<PutJobAction.Response>(request, actionListener) {
                    private volatile Job updatedJob;

                    @Override
                    protected PutJobAction.Response newResponse(boolean acknowledged) {
                        return new PutJobAction.Response(acknowledged, updatedJob);
                    }

                    @Override
                    public ClusterState execute(ClusterState currentState) throws Exception {
                        Job job = getJob(jobId, currentState).results().get(0);
                        updatedJob = jobUpdate.mergeWithJob(job);
                        return updateClusterState(updatedJob, true, currentState);
                    }

                    @Override
                    public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                        PersistentTasksCustomMetaData persistentTasks =
                                newState.metaData().custom(PersistentTasksCustomMetaData.TYPE);
                        JobState jobState = MlMetadata.getJobState(jobId, persistentTasks);
                        if (jobState == JobState.OPENED) {
                            updateJobProcessNotifier.submitJobUpdate(jobUpdate);
                        }
                    }
                });
    }

    ClusterState updateClusterState(Job job, boolean overwrite, ClusterState currentState) {
        MlMetadata.Builder builder = createMlMetadataBuilder(currentState);
        builder.putJob(job, overwrite);
        return buildNewClusterState(currentState, builder);
    }

    public void deleteJob(DeleteJobAction.Request request, JobStorageDeletionTask task,
                          ActionListener<DeleteJobAction.Response> actionListener) {

        String jobId = request.getJobId();
        logger.debug("Deleting job '" + jobId + "'");

        // Step 3. When the job has been removed from the cluster state, return a response
        // -------
        CheckedConsumer<Boolean, Exception> apiResponseHandler = jobDeleted -> {
            if (jobDeleted) {
                logger.info("Job [" + jobId + "] deleted.");
                actionListener.onResponse(new DeleteJobAction.Response(true));
                auditor.info(jobId, Messages.getMessage(Messages.JOB_AUDIT_DELETED));
            } else {
                actionListener.onResponse(new DeleteJobAction.Response(false));
            }
        };

        // Step 2. When the physical storage has been deleted, remove from Cluster State
        // -------
        CheckedConsumer<Boolean, Exception> deleteJobStateHandler = response -> clusterService.submitStateUpdateTask("delete-job-" + jobId,
                new AckedClusterStateUpdateTask<Boolean>(request, ActionListener.wrap(apiResponseHandler, actionListener::onFailure)) {

                    @Override
                    protected Boolean newResponse(boolean acknowledged) {
                        return acknowledged && response;
                    }

                @Override
                public ClusterState execute(ClusterState currentState) throws Exception {
                    MlMetadata.Builder builder = createMlMetadataBuilder(currentState);
                    builder.deleteJob(jobId, currentState.getMetaData().custom(PersistentTasksCustomMetaData.TYPE));
                    return buildNewClusterState(currentState, builder);
                }
            });

        // Step 1. When the job has been marked as deleted then begin deleting the physical storage
        // -------
        CheckedConsumer<Boolean, Exception> updateHandler = response -> {
            // Successfully updated the status to DELETING, begin actually deleting
            if (response) {
                logger.info("Job [" + jobId + "] is successfully marked as deleted");
            } else {
                logger.warn("Job [" + jobId + "] marked as deleted wan't acknowledged");
            }

            // This task manages the physical deletion of the job (removing the results, then the index)
            task.delete(jobId, client, clusterService.state(),
                    deleteJobStateHandler::accept, actionListener::onFailure);
        };

        // Step 0. Kick off the chain of callbacks with the initial UpdateStatus call
        // -------
        clusterService.submitStateUpdateTask("mark-job-as-deleted", new ClusterStateUpdateTask() {
            @Override
            public ClusterState execute(ClusterState currentState) throws Exception {
                MlMetadata currentMlMetadata = currentState.metaData().custom(MlMetadata.TYPE);
                PersistentTasksCustomMetaData tasks = currentState.metaData().custom(PersistentTasksCustomMetaData.TYPE);
                MlMetadata.Builder builder = new MlMetadata.Builder(currentMlMetadata);
                builder.markJobAsDeleted(jobId, tasks);
                return buildNewClusterState(currentState, builder);
            }

            @Override
            public void onFailure(String source, Exception e) {
                actionListener.onFailure(e);
            }

            @Override
            public void clusterStatePublished(ClusterChangedEvent clusterChangedEvent) {
                try {
                    updateHandler.accept(true);
                } catch (Exception e) {
                    actionListener.onFailure(e);
                }
            }
        });
    }

    public void revertSnapshot(RevertModelSnapshotAction.Request request, ActionListener<RevertModelSnapshotAction.Response> actionListener,
            ModelSnapshot modelSnapshot) {

        clusterService.submitStateUpdateTask("revert-snapshot-" + request.getJobId(),
                new AckedClusterStateUpdateTask<RevertModelSnapshotAction.Response>(request, actionListener) {

            @Override
            protected RevertModelSnapshotAction.Response newResponse(boolean acknowledged) {
                if (acknowledged) {
                    auditor.info(request.getJobId(), Messages.getMessage(Messages.JOB_AUDIT_REVERTED, modelSnapshot.getDescription()));
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
                return updateClusterState(builder.build(), true, currentState);
            }
        });
    }

    /**
     * Update a persisted model snapshot metadata document to match the
     * argument supplied.
     *
     * @param modelSnapshot         the updated model snapshot object to be stored
     */
    public void updateModelSnapshot(ModelSnapshot modelSnapshot, Consumer<Boolean> handler, Consumer<Exception> errorHandler) {
        String index = AnomalyDetectorsIndex.jobResultsAliasedName(modelSnapshot.getJobId());
        IndexRequest indexRequest = new IndexRequest(index, ModelSnapshot.TYPE.getPreferredName(), ModelSnapshot.documentId(modelSnapshot));
        try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
            modelSnapshot.toXContent(builder, ToXContent.EMPTY_PARAMS);
            indexRequest.source(builder);
        } catch (IOException e) {
            errorHandler.accept(e);
        }
        client.index(indexRequest, ActionListener.wrap(r -> handler.accept(true), errorHandler));
    }

    private static MlMetadata.Builder createMlMetadataBuilder(ClusterState currentState) {
        MlMetadata currentMlMetadata = currentState.metaData().custom(MlMetadata.TYPE);
        return new MlMetadata.Builder(currentMlMetadata);
    }

    private static ClusterState buildNewClusterState(ClusterState currentState, MlMetadata.Builder builder) {
        ClusterState.Builder newState = ClusterState.builder(currentState);
        newState.metaData(MetaData.builder(currentState.getMetaData()).putCustom(MlMetadata.TYPE, builder.build()).build());
        return newState.build();
    }

}
