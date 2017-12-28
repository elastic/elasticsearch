/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.AckedClusterStateUpdateTask;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ack.AckedRequest;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.CheckedConsumer;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.xpack.ml.MLMetadataField;
import org.elasticsearch.xpack.ml.MachineLearningClientActionPlugin;
import org.elasticsearch.xpack.ml.MlMetadata;
import org.elasticsearch.xpack.ml.action.DeleteJobAction;
import org.elasticsearch.xpack.ml.action.PutJobAction;
import org.elasticsearch.xpack.ml.action.RevertModelSnapshotAction;
import org.elasticsearch.xpack.ml.action.util.QueryPage;
import org.elasticsearch.xpack.ml.job.config.DataDescription;
import org.elasticsearch.xpack.ml.job.config.Job;
import org.elasticsearch.xpack.ml.job.config.JobState;
import org.elasticsearch.xpack.ml.job.config.JobUpdate;
import org.elasticsearch.xpack.ml.job.messages.Messages;
import org.elasticsearch.xpack.ml.job.persistence.JobProvider;
import org.elasticsearch.xpack.ml.job.persistence.JobResultsPersister;
import org.elasticsearch.xpack.ml.job.persistence.JobStorageDeletionTask;
import org.elasticsearch.xpack.ml.job.process.autodetect.state.ModelSizeStats;
import org.elasticsearch.xpack.ml.job.process.autodetect.state.ModelSnapshot;
import org.elasticsearch.xpack.ml.notifications.Auditor;
import org.elasticsearch.xpack.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.persistent.PersistentTasksCustomMetaData;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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

    private static final DeprecationLogger DEPRECATION_LOGGER =
            new DeprecationLogger(Loggers.getLogger(JobManager.class));

    private final JobProvider jobProvider;
    private final ClusterService clusterService;
    private final Auditor auditor;
    private final Client client;
    private final UpdateJobProcessNotifier updateJobProcessNotifier;

    private volatile ByteSizeValue maxModelMemoryLimit;

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

        maxModelMemoryLimit = MachineLearningClientActionPlugin.MAX_MODEL_MEMORY_LIMIT.get(settings);
        clusterService.getClusterSettings()
                .addSettingsUpdateConsumer(MachineLearningClientActionPlugin.MAX_MODEL_MEMORY_LIMIT, this::setMaxModelMemoryLimit);
    }

    private void setMaxModelMemoryLimit(ByteSizeValue maxModelMemoryLimit) {
        this.maxModelMemoryLimit = maxModelMemoryLimit;
    }

    /**
     * Gets the job that matches the given {@code jobId}.
     *
     * @param jobId the jobId
     * @return The {@link Job} matching the given {code jobId}
     * @throws ResourceNotFoundException if no job matches {@code jobId}
     */
    public Job getJobOrThrowIfUnknown(String jobId) {
        return getJobOrThrowIfUnknown(jobId, clusterService.state());
    }

    /**
     * Gets the job that matches the given {@code jobId}.
     *
     * @param jobId the jobId
     * @param clusterState the cluster state
     * @return The {@link Job} matching the given {code jobId}
     * @throws ResourceNotFoundException if no job matches {@code jobId}
     */
    public static Job getJobOrThrowIfUnknown(String jobId, ClusterState clusterState) {
        MlMetadata mlMetadata = clusterState.getMetaData().custom(MLMetadataField.TYPE);
        Job job = (mlMetadata == null) ? null : mlMetadata.getJobs().get(jobId);
        if (job == null) {
            throw ExceptionsHelper.missingJobException(jobId);
        }
        return job;
    }

    /**
     * Get the jobs that match the given {@code expression}.
     * Note that when the {@code jobId} is {@link MetaData#ALL} all jobs are returned.
     *
     * @param expression   the jobId or an expression matching jobIds
     * @param clusterState the cluster state
     * @param allowNoJobs  if {@code false}, an error is thrown when no job matches the {@code jobId}
     * @return A {@link QueryPage} containing the matching {@code Job}s
     */
    public QueryPage<Job> expandJobs(String expression, boolean allowNoJobs, ClusterState clusterState) {
        MlMetadata mlMetadata = clusterState.getMetaData().custom(MLMetadataField.TYPE);
        if (mlMetadata == null) {
            mlMetadata = MlMetadata.EMPTY_METADATA;
        }
        Set<String> expandedJobIds = mlMetadata.expandJobIds(expression, allowNoJobs);
        List<Job> jobs = new ArrayList<>();
        for (String expandedJobId : expandedJobIds) {
            jobs.add(mlMetadata.getJobs().get(expandedJobId));
        }
        logger.debug("Returning jobs matching [" + expression + "]");
        return new QueryPage<>(jobs, jobs.size(), Job.RESULTS_FIELD);
    }

    public JobState getJobState(String jobId) {
        PersistentTasksCustomMetaData tasks = clusterService.state().getMetaData().custom(PersistentTasksCustomMetaData.TYPE);
        return MlMetadata.getJobState(jobId, tasks);
    }

    /**
     * Stores a job in the cluster state
     */
    public void putJob(PutJobAction.Request request, ClusterState state, ActionListener<PutJobAction.Response> actionListener) {
        // In 6.1 we want to make the model memory size limit more prominent, and also reduce the default from
        // 4GB to 1GB.  However, changing the meaning of a null model memory limit for existing jobs would be a
        // breaking change, so instead we add an explicit limit to newly created jobs that didn't have one when
        // submitted
        request.getJobBuilder().validateModelMemoryLimit(maxModelMemoryLimit);


        Job job = request.getJobBuilder().build(new Date());
        if (job.getDataDescription() != null && job.getDataDescription().getFormat() == DataDescription.DataFormat.DELIMITED) {
            DEPRECATION_LOGGER.deprecated("Creating jobs with delimited data format is deprecated. Please use JSON instead.");
        }

        MlMetadata currentMlMetadata = state.metaData().custom(MLMetadataField.TYPE);
        if (currentMlMetadata != null && currentMlMetadata.getJobs().containsKey(job.getId())) {
            actionListener.onFailure(ExceptionsHelper.jobAlreadyExists(job.getId()));
            return;
        }

        ActionListener<Boolean> putJobListener = new ActionListener<Boolean>() {
            @Override
            public void onResponse(Boolean indicesCreated) {

                clusterService.submitStateUpdateTask("put-job-" + job.getId(),
                        new AckedClusterStateUpdateTask<PutJobAction.Response>(request, actionListener) {
                            @Override
                            protected PutJobAction.Response newResponse(boolean acknowledged) {
                                auditor.info(job.getId(), Messages.getMessage(Messages.JOB_AUDIT_CREATED));
                                return new PutJobAction.Response(acknowledged, job);
                            }

                            @Override
                            public ClusterState execute(ClusterState currentState) throws Exception {
                                return updateClusterState(job, false, currentState);
                            }
                        });
            }

            @Override
            public void onFailure(Exception e) {
                if (e instanceof IllegalArgumentException) {
                    // the underlying error differs depending on which way around the clashing fields are seen
                    Matcher matcher = Pattern.compile("(?:mapper|Can't merge a non object mapping) \\[(.*)\\] (?:of different type, " +
                            "current_type \\[.*\\], merged_type|with an object mapping) \\[.*\\]").matcher(e.getMessage());
                    if (matcher.matches()) {
                        String msg = Messages.getMessage(Messages.JOB_CONFIG_MAPPING_TYPE_CLASH, matcher.group(1));
                        actionListener.onFailure(ExceptionsHelper.badRequestException(msg, e));
                        return;
                    }
                }
                actionListener.onFailure(e);
            }
        };

        ActionListener<Boolean> checkForLeftOverDocs = ActionListener.wrap(
                response -> {
                    jobProvider.createJobResultIndex(job, state, putJobListener);
                },
                actionListener::onFailure
        );

        jobProvider.checkForLeftOverDocuments(job, checkForLeftOverDocs);
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
                    if (oldModelSnapshot != null && newModelSnapshot.result.getTimestamp().before(oldModelSnapshot.result.getTimestamp())) {
                        String message = "Job [" + job.getId() + "] has a more recent model snapshot [" +
                                oldModelSnapshot.result.getSnapshotId() + "]";
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
                    private volatile boolean changeWasRequired;

                    @Override
                    protected PutJobAction.Response newResponse(boolean acknowledged) {
                        return new PutJobAction.Response(acknowledged, updatedJob);
                    }

                    @Override
                    public ClusterState execute(ClusterState currentState) throws Exception {
                        Job job = getJobOrThrowIfUnknown(jobId, currentState);
                        updatedJob = jobUpdate.mergeWithJob(job, maxModelMemoryLimit);
                        if (updatedJob.equals(job)) {
                            // nothing to do
                            return currentState;
                        }
                        changeWasRequired = true;
                        return updateClusterState(updatedJob, true, currentState);
                    }

                    @Override
                    public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                        if (changeWasRequired) {
                            PersistentTasksCustomMetaData persistentTasks =
                                    newState.metaData().custom(PersistentTasksCustomMetaData.TYPE);
                            JobState jobState = MlMetadata.getJobState(jobId, persistentTasks);
                            if (jobState == JobState.OPENED) {
                                updateJobProcessNotifier.submitJobUpdate(jobUpdate);
                            }
                        } else {
                            logger.debug("[{}] Ignored job update with no changes: {}", () -> jobId, () -> {
                                try {
                                    XContentBuilder jsonBuilder = XContentFactory.jsonBuilder();
                                    jobUpdate.toXContent(jsonBuilder, ToXContent.EMPTY_PARAMS);
                                    return jsonBuilder.string();
                                } catch (IOException e) {
                                    return "(unprintable due to " + e.getMessage() + ")";
                                }
                            });
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

        // Step 4. When the job has been removed from the cluster state, return a response
        // -------
        CheckedConsumer<Boolean, Exception> apiResponseHandler = jobDeleted -> {
            if (jobDeleted) {
                logger.info("Job [" + jobId + "] deleted");
                auditor.info(jobId, Messages.getMessage(Messages.JOB_AUDIT_DELETED));
                actionListener.onResponse(new DeleteJobAction.Response(true));
            } else {
                actionListener.onResponse(new DeleteJobAction.Response(false));
            }
        };

        // Step 3. When the physical storage has been deleted, remove from Cluster State
        // -------
        CheckedConsumer<Boolean, Exception> deleteJobStateHandler = response -> clusterService.submitStateUpdateTask("delete-job-" + jobId,
                new AckedClusterStateUpdateTask<Boolean>(request, ActionListener.wrap(apiResponseHandler, actionListener::onFailure)) {

                    @Override
                    protected Boolean newResponse(boolean acknowledged) {
                        return acknowledged && response;
                    }

                    @Override
                    public ClusterState execute(ClusterState currentState) throws Exception {
                        MlMetadata currentMlMetadata = currentState.metaData().custom(MLMetadataField.TYPE);
                        if (currentMlMetadata.getJobs().containsKey(jobId) == false) {
                            // We wouldn't have got here if the job never existed so
                            // the Job must have been deleted by another action.
                            // Don't error in this case
                            return currentState;
                        }

                        MlMetadata.Builder builder = new MlMetadata.Builder(currentMlMetadata);
                        builder.deleteJob(jobId, currentState.getMetaData().custom(PersistentTasksCustomMetaData.TYPE));
                        return buildNewClusterState(currentState, builder);
                    }
            });


        // Step 2. Remove the job from any calendars
        CheckedConsumer<Boolean, Exception> removeFromCalendarsHandler = response -> {
            jobProvider.removeJobFromCalendars(jobId, ActionListener.<Boolean>wrap(deleteJobStateHandler::accept,
                    actionListener::onFailure ));
        };


        // Step 1. Delete the physical storage

        // This task manages the physical deletion of the job state and results
        task.delete(jobId, client, clusterService.state(), removeFromCalendarsHandler, actionListener::onFailure);
    }

    public void revertSnapshot(RevertModelSnapshotAction.Request request, ActionListener<RevertModelSnapshotAction.Response> actionListener,
            ModelSnapshot modelSnapshot) {

        final ModelSizeStats modelSizeStats = modelSnapshot.getModelSizeStats();
        final JobResultsPersister persister = new JobResultsPersister(settings, client);

        // Step 3. After the model size stats is persisted, also persist the snapshot's quantiles and respond
        // -------
        CheckedConsumer<IndexResponse, Exception> modelSizeStatsResponseHandler = response -> {
            persister.persistQuantiles(modelSnapshot.getQuantiles(), WriteRequest.RefreshPolicy.IMMEDIATE,
                    ActionListener.wrap(quantilesResponse -> {
                        // The quantiles can be large, and totally dominate the output -
                        // it's clearer to remove them as they are not necessary for the revert op
                        ModelSnapshot snapshotWithoutQuantiles = new ModelSnapshot.Builder(modelSnapshot).setQuantiles(null).build();
                        actionListener.onResponse(new RevertModelSnapshotAction.Response(snapshotWithoutQuantiles));
                    }, actionListener::onFailure));
        };

        // Step 2. When the model_snapshot_id is updated on the job, persist the snapshot's model size stats with a touched log time
        // so that a search for the latest model size stats returns the reverted one.
        // -------
        CheckedConsumer<Boolean, Exception> updateHandler = response -> {
            if (response) {
                ModelSizeStats revertedModelSizeStats = new ModelSizeStats.Builder(modelSizeStats).setLogTime(new Date()).build();
                persister.persistModelSizeStats(revertedModelSizeStats, WriteRequest.RefreshPolicy.IMMEDIATE, ActionListener.wrap(
                        modelSizeStatsResponseHandler::accept, actionListener::onFailure));
            }
        };

        // Step 1. Do the cluster state update
        // -------
        Consumer<Long> clusterStateHandler = response -> clusterService.submitStateUpdateTask("revert-snapshot-" + request.getJobId(),
                new AckedClusterStateUpdateTask<Boolean>(request, ActionListener.wrap(updateHandler, actionListener::onFailure)) {

            @Override
            protected Boolean newResponse(boolean acknowledged) {
                if (acknowledged) {
                    auditor.info(request.getJobId(), Messages.getMessage(Messages.JOB_AUDIT_REVERTED, modelSnapshot.getDescription()));
                    return true;
                }
                actionListener.onFailure(new IllegalStateException("Could not revert modelSnapshot on job ["
                        + request.getJobId() + "], not acknowledged by master."));
                return false;
            }

            @Override
            public ClusterState execute(ClusterState currentState) throws Exception {
                Job job = getJobOrThrowIfUnknown(request.getJobId(), currentState);
                Job.Builder builder = new Job.Builder(job);
                builder.setModelSnapshotId(modelSnapshot.getSnapshotId());
                builder.setEstablishedModelMemory(response);
                return updateClusterState(builder.build(), true, currentState);
            }
        });

        // Step 0. Find the appropriate established model memory for the reverted job
        // -------
        jobProvider.getEstablishedMemoryUsage(request.getJobId(), modelSizeStats.getTimestamp(), modelSizeStats, clusterStateHandler,
                actionListener::onFailure);
    }

    private static MlMetadata.Builder createMlMetadataBuilder(ClusterState currentState) {
        MlMetadata currentMlMetadata = currentState.metaData().custom(MLMetadataField.TYPE);
        return new MlMetadata.Builder(currentMlMetadata);
    }

    private static ClusterState buildNewClusterState(ClusterState currentState, MlMetadata.Builder builder) {
        ClusterState.Builder newState = ClusterState.builder(currentState);
        newState.metaData(MetaData.builder(currentState.getMetaData()).putCustom(MLMetadataField.TYPE, builder.build()).build());
        return newState.build();
    }

}
