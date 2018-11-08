/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.AckedClusterStateUpdateTask;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.CheckedConsumer;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.analysis.AnalysisRegistry;
import org.elasticsearch.persistent.PersistentTasksCustomMetaData;
import org.elasticsearch.xpack.core.XPackPlugin;
import org.elasticsearch.xpack.core.ml.MachineLearningField;
import org.elasticsearch.xpack.core.ml.MlMetadata;
import org.elasticsearch.xpack.core.ml.MlTasks;
import org.elasticsearch.xpack.core.ml.action.PutJobAction;
import org.elasticsearch.xpack.core.ml.action.RevertModelSnapshotAction;
import org.elasticsearch.xpack.core.ml.action.UpdateJobAction;
import org.elasticsearch.xpack.core.ml.action.util.QueryPage;
import org.elasticsearch.xpack.core.ml.job.config.AnalysisLimits;
import org.elasticsearch.xpack.core.ml.job.config.CategorizationAnalyzerConfig;
import org.elasticsearch.xpack.core.ml.job.config.DataDescription;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.core.ml.job.config.JobState;
import org.elasticsearch.xpack.core.ml.job.config.JobUpdate;
import org.elasticsearch.xpack.core.ml.job.config.MlFilter;
import org.elasticsearch.xpack.core.ml.job.messages.Messages;
import org.elasticsearch.xpack.core.ml.job.process.autodetect.state.ModelSizeStats;
import org.elasticsearch.xpack.core.ml.job.process.autodetect.state.ModelSnapshot;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.ml.MachineLearning;
import org.elasticsearch.xpack.ml.job.categorization.CategorizationAnalyzer;
import org.elasticsearch.xpack.ml.job.persistence.JobResultsPersister;
import org.elasticsearch.xpack.ml.job.persistence.JobResultsProvider;
import org.elasticsearch.xpack.ml.job.process.autodetect.UpdateParams;
import org.elasticsearch.xpack.ml.notifications.Auditor;
import org.elasticsearch.xpack.ml.utils.ChainTaskExecutor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
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
public class JobManager {

    private static final Logger logger = LogManager.getLogger(JobManager.class);
    private static final DeprecationLogger deprecationLogger = new DeprecationLogger(logger);

    private final Environment environment;
    private final JobResultsProvider jobResultsProvider;
    private final ClusterService clusterService;
    private final Auditor auditor;
    private final Client client;
    private final UpdateJobProcessNotifier updateJobProcessNotifier;

    private volatile ByteSizeValue maxModelMemoryLimit;

    /**
     * Create a JobManager
     */
    public JobManager(Environment environment, Settings settings, JobResultsProvider jobResultsProvider,
                      ClusterService clusterService, Auditor auditor,
                      Client client, UpdateJobProcessNotifier updateJobProcessNotifier) {
        this.environment = environment;
        this.jobResultsProvider = Objects.requireNonNull(jobResultsProvider);
        this.clusterService = Objects.requireNonNull(clusterService);
        this.auditor = Objects.requireNonNull(auditor);
        this.client = Objects.requireNonNull(client);
        this.updateJobProcessNotifier = updateJobProcessNotifier;

        maxModelMemoryLimit = MachineLearningField.MAX_MODEL_MEMORY_LIMIT.get(settings);
        clusterService.getClusterSettings()
                .addSettingsUpdateConsumer(MachineLearningField.MAX_MODEL_MEMORY_LIMIT, this::setMaxModelMemoryLimit);
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
        Job job = MlMetadata.getMlMetadata(clusterState).getJobs().get(jobId);
        if (job == null) {
            throw ExceptionsHelper.missingJobException(jobId);
        }
        return job;
    }

    private Set<String> expandJobIds(String expression, boolean allowNoJobs, ClusterState clusterState) {
        return MlMetadata.getMlMetadata(clusterState).expandJobIds(expression, allowNoJobs);
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
        Set<String> expandedJobIds = expandJobIds(expression, allowNoJobs, clusterState);
        MlMetadata mlMetadata = MlMetadata.getMlMetadata(clusterState);
        List<Job> jobs = new ArrayList<>();
        for (String expandedJobId : expandedJobIds) {
            jobs.add(mlMetadata.getJobs().get(expandedJobId));
        }
        logger.debug("Returning jobs matching [" + expression + "]");
        return new QueryPage<>(jobs, jobs.size(), Job.RESULTS_FIELD);
    }

    /**
     * Validate the char filter/tokenizer/token filter names used in the categorization analyzer config (if any).
     * This validation has to be done server-side; it cannot be done in a client as that won't have loaded the
     * appropriate analysis modules/plugins.
     * The overall structure can be validated at parse time, but the exact names need to be checked separately,
     * as plugins that provide the functionality can be installed/uninstalled.
     */
    static void validateCategorizationAnalyzer(Job.Builder jobBuilder, AnalysisRegistry analysisRegistry, Environment environment)
        throws IOException {
        CategorizationAnalyzerConfig categorizationAnalyzerConfig = jobBuilder.getAnalysisConfig().getCategorizationAnalyzerConfig();
        if (categorizationAnalyzerConfig != null) {
            CategorizationAnalyzer.verifyConfigBuilder(new CategorizationAnalyzerConfig.Builder(categorizationAnalyzerConfig),
                analysisRegistry, environment);
        }
    }

    /**
     * Stores a job in the cluster state
     */
    public void putJob(PutJobAction.Request request, AnalysisRegistry analysisRegistry, ClusterState state,
                       ActionListener<PutJobAction.Response> actionListener) throws IOException {

        request.getJobBuilder().validateAnalysisLimitsAndSetDefaults(maxModelMemoryLimit);
        validateCategorizationAnalyzer(request.getJobBuilder(), analysisRegistry, environment);

        Job job = request.getJobBuilder().build(new Date());

        if (job.getDataDescription() != null && job.getDataDescription().getFormat() == DataDescription.DataFormat.DELIMITED) {
            deprecationLogger.deprecated("Creating jobs with delimited data format is deprecated. Please use xcontent instead.");
        }

        // pre-flight check, not necessarily required, but avoids figuring this out while on the CS update thread
        XPackPlugin.checkReadyForXPackCustomMetadata(state);

        MlMetadata currentMlMetadata = MlMetadata.getMlMetadata(state);
        if (currentMlMetadata.getJobs().containsKey(job.getId())) {
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
                                return new PutJobAction.Response(job);
                            }

                            @Override
                            public ClusterState execute(ClusterState currentState) {
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
                    jobResultsProvider.createJobResultIndex(job, state, putJobListener);
                },
                actionListener::onFailure
        );

        jobResultsProvider.checkForLeftOverDocuments(job, checkForLeftOverDocs);
    }

    public void updateJob(UpdateJobAction.Request request, ActionListener<PutJobAction.Response> actionListener) {
        Job job = getJobOrThrowIfUnknown(request.getJobId());
        validate(request.getJobUpdate(), job, ActionListener.wrap(
                nullValue -> internalJobUpdate(request, actionListener),
                actionListener::onFailure));
    }

    private void validate(JobUpdate jobUpdate, Job job, ActionListener<Void> handler) {
        ChainTaskExecutor chainTaskExecutor = new ChainTaskExecutor(client.threadPool().executor(
                MachineLearning.UTILITY_THREAD_POOL_NAME), true);
        validateModelSnapshotIdUpdate(job, jobUpdate.getModelSnapshotId(), chainTaskExecutor);
        validateAnalysisLimitsUpdate(job, jobUpdate.getAnalysisLimits(), chainTaskExecutor);
        chainTaskExecutor.execute(handler);
    }

    private void validateModelSnapshotIdUpdate(Job job, String modelSnapshotId, ChainTaskExecutor chainTaskExecutor) {
        if (modelSnapshotId != null) {
            chainTaskExecutor.add(listener -> {
                jobResultsProvider.getModelSnapshot(job.getId(), modelSnapshotId, newModelSnapshot -> {
                    if (newModelSnapshot == null) {
                        String message = Messages.getMessage(Messages.REST_NO_SUCH_MODEL_SNAPSHOT, modelSnapshotId,
                                job.getId());
                        listener.onFailure(new ResourceNotFoundException(message));
                        return;
                    }
                    jobResultsProvider.getModelSnapshot(job.getId(), job.getModelSnapshotId(), oldModelSnapshot -> {
                        if (oldModelSnapshot != null
                                && newModelSnapshot.result.getTimestamp().before(oldModelSnapshot.result.getTimestamp())) {
                            String message = "Job [" + job.getId() + "] has a more recent model snapshot [" +
                                    oldModelSnapshot.result.getSnapshotId() + "]";
                            listener.onFailure(new IllegalArgumentException(message));
                        }
                        listener.onResponse(null);
                    }, listener::onFailure);
                }, listener::onFailure);
            });
        }
    }

    private void validateAnalysisLimitsUpdate(Job job, AnalysisLimits newLimits, ChainTaskExecutor chainTaskExecutor) {
        if (newLimits == null || newLimits.getModelMemoryLimit() == null) {
            return;
        }
        Long newModelMemoryLimit = newLimits.getModelMemoryLimit();
        chainTaskExecutor.add(listener -> {
            if (isJobOpen(clusterService.state(), job.getId())) {
                listener.onFailure(ExceptionsHelper.badRequestException("Cannot update " + Job.ANALYSIS_LIMITS.getPreferredName()
                        + " while the job is open"));
                return;
            }
            jobResultsProvider.modelSizeStats(job.getId(), modelSizeStats -> {
                if (modelSizeStats != null) {
                    ByteSizeValue modelSize = new ByteSizeValue(modelSizeStats.getModelBytes(), ByteSizeUnit.BYTES);
                    if (newModelMemoryLimit < modelSize.getMb()) {
                        listener.onFailure(ExceptionsHelper.badRequestException(
                                Messages.getMessage(Messages.JOB_CONFIG_UPDATE_ANALYSIS_LIMITS_MODEL_MEMORY_LIMIT_CANNOT_BE_DECREASED,
                                        new ByteSizeValue(modelSize.getMb(), ByteSizeUnit.MB),
                                        new ByteSizeValue(newModelMemoryLimit, ByteSizeUnit.MB))));
                        return;
                    }
                }
                listener.onResponse(null);
            }, listener::onFailure);
        });
    }

    private void internalJobUpdate(UpdateJobAction.Request request, ActionListener<PutJobAction.Response> actionListener) {
        if (request.isWaitForAck()) {
            // Use the ack cluster state update
            clusterService.submitStateUpdateTask("update-job-" + request.getJobId(),
                    new AckedClusterStateUpdateTask<PutJobAction.Response>(request, actionListener) {
                        private AtomicReference<Job> updatedJob = new AtomicReference<>();

                        @Override
                        protected PutJobAction.Response newResponse(boolean acknowledged) {
                            return new PutJobAction.Response(updatedJob.get());
                        }

                        @Override
                        public ClusterState execute(ClusterState currentState) {
                            Job job = getJobOrThrowIfUnknown(request.getJobId(), currentState);
                            updatedJob.set(request.getJobUpdate().mergeWithJob(job, maxModelMemoryLimit));
                            return updateClusterState(updatedJob.get(), true, currentState);
                        }

                        @Override
                        public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                            afterClusterStateUpdate(newState, request);
                        }
                    });
        } else {
            clusterService.submitStateUpdateTask("update-job-" + request.getJobId(), new ClusterStateUpdateTask() {
                private AtomicReference<Job> updatedJob = new AtomicReference<>();

                @Override
                public ClusterState execute(ClusterState currentState) throws Exception {
                    Job job = getJobOrThrowIfUnknown(request.getJobId(), currentState);
                    updatedJob.set(request.getJobUpdate().mergeWithJob(job, maxModelMemoryLimit));
                    return updateClusterState(updatedJob.get(), true, currentState);
                }

                @Override
                public void onFailure(String source, Exception e) {
                    actionListener.onFailure(e);
                }

                @Override
                public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                    afterClusterStateUpdate(newState, request);
                    actionListener.onResponse(new PutJobAction.Response(updatedJob.get()));
                }
            });
        }
    }

    private void afterClusterStateUpdate(ClusterState newState, UpdateJobAction.Request request) {
        JobUpdate jobUpdate = request.getJobUpdate();

        // Change is required if the fields that the C++ uses are being updated
        boolean processUpdateRequired = jobUpdate.isAutodetectProcessUpdate();

        if (processUpdateRequired && isJobOpen(newState, request.getJobId())) {
            updateJobProcessNotifier.submitJobUpdate(UpdateParams.fromJobUpdate(jobUpdate), ActionListener.wrap(
                    isUpdated -> {
                        if (isUpdated) {
                            auditJobUpdatedIfNotInternal(request);
                        }
                    }, e -> {
                        // No need to do anything
                    }
            ));
        } else {
            logger.debug("[{}] No process update required for job update: {}", () -> request.getJobId(), () -> {
                try {
                    XContentBuilder jsonBuilder = XContentFactory.jsonBuilder();
                    jobUpdate.toXContent(jsonBuilder, ToXContent.EMPTY_PARAMS);
                    return Strings.toString(jsonBuilder);
                } catch (IOException e) {
                    return "(unprintable due to " + e.getMessage() + ")";
                }
            });

            auditJobUpdatedIfNotInternal(request);
        }
    }

    private void auditJobUpdatedIfNotInternal(UpdateJobAction.Request request) {
        if (request.isInternal() == false) {
            auditor.info(request.getJobId(), Messages.getMessage(Messages.JOB_AUDIT_UPDATED, request.getJobUpdate().getUpdateFields()));
        }
    }

    private boolean isJobOpen(ClusterState clusterState, String jobId) {
        PersistentTasksCustomMetaData persistentTasks = clusterState.metaData().custom(PersistentTasksCustomMetaData.TYPE);
        JobState jobState = MlTasks.getJobState(jobId, persistentTasks);
        return jobState == JobState.OPENED;
    }

    private ClusterState updateClusterState(Job job, boolean overwrite, ClusterState currentState) {
        MlMetadata.Builder builder = createMlMetadataBuilder(currentState);
        builder.putJob(job, overwrite);
        return buildNewClusterState(currentState, builder);
    }

    public void notifyFilterChanged(MlFilter filter, Set<String> addedItems, Set<String> removedItems) {
        if (addedItems.isEmpty() && removedItems.isEmpty()) {
            return;
        }

        ClusterState clusterState = clusterService.state();
        QueryPage<Job> jobs = expandJobs("*", true, clusterService.state());
        for (Job job : jobs.results()) {
            Set<String> jobFilters = job.getAnalysisConfig().extractReferencedFilters();
            if (jobFilters.contains(filter.getId())) {
                if (isJobOpen(clusterState, job.getId())) {
                    updateJobProcessNotifier.submitJobUpdate(UpdateParams.filterUpdate(job.getId(), filter),
                            ActionListener.wrap(isUpdated -> {
                                auditFilterChanges(job.getId(), filter.getId(), addedItems, removedItems);
                            }, e -> {}));
                } else {
                    auditFilterChanges(job.getId(), filter.getId(), addedItems, removedItems);
                }
            }
        }
    }

    private void auditFilterChanges(String jobId, String filterId, Set<String> addedItems, Set<String> removedItems) {
        StringBuilder auditMsg = new StringBuilder("Filter [");
        auditMsg.append(filterId);
        auditMsg.append("] has been modified; ");

        if (addedItems.isEmpty() == false) {
            auditMsg.append("added items: ");
            appendCommaSeparatedSet(addedItems, auditMsg);
            if (removedItems.isEmpty() == false) {
                auditMsg.append(", ");
            }
        }

        if (removedItems.isEmpty() == false) {
            auditMsg.append("removed items: ");
            appendCommaSeparatedSet(removedItems, auditMsg);
        }

        auditor.info(jobId, auditMsg.toString());
    }

    private static void appendCommaSeparatedSet(Set<String> items, StringBuilder sb) {
        sb.append("[");
        Strings.collectionToDelimitedString(items, ", ", "'", "'", sb);
        sb.append("]");
    }

    public void updateProcessOnCalendarChanged(List<String> calendarJobIds) {
        ClusterState clusterState = clusterService.state();
        MlMetadata mlMetadata = MlMetadata.getMlMetadata(clusterState);

        List<String> existingJobsOrGroups =
                calendarJobIds.stream().filter(mlMetadata::isGroupOrJob).collect(Collectors.toList());

        Set<String> expandedJobIds = new HashSet<>();
        existingJobsOrGroups.forEach(jobId -> expandedJobIds.addAll(expandJobIds(jobId, true, clusterState)));
        for (String jobId : expandedJobIds) {
            if (isJobOpen(clusterState, jobId)) {
                updateJobProcessNotifier.submitJobUpdate(UpdateParams.scheduledEventsUpdate(jobId), ActionListener.wrap(
                        isUpdated -> {
                            if (isUpdated) {
                                auditor.info(jobId, Messages.getMessage(Messages.JOB_AUDIT_CALENDARS_UPDATED_ON_PROCESS));
                            }
                        }, e -> {}
                ));
            }
        }
    }

    public void revertSnapshot(RevertModelSnapshotAction.Request request, ActionListener<RevertModelSnapshotAction.Response> actionListener,
            ModelSnapshot modelSnapshot) {

        final ModelSizeStats modelSizeStats = modelSnapshot.getModelSizeStats();
        final JobResultsPersister persister = new JobResultsPersister(client);

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
                        modelSizeStatsResponseHandler, actionListener::onFailure));
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
            public ClusterState execute(ClusterState currentState) {
                Job job = getJobOrThrowIfUnknown(request.getJobId(), currentState);
                Job.Builder builder = new Job.Builder(job);
                builder.setModelSnapshotId(modelSnapshot.getSnapshotId());
                builder.setEstablishedModelMemory(response);
                return updateClusterState(builder.build(), true, currentState);
            }
        });

        // Step 0. Find the appropriate established model memory for the reverted job
        // -------
        jobResultsProvider.getEstablishedMemoryUsage(request.getJobId(), modelSizeStats.getTimestamp(), modelSizeStats, clusterStateHandler,
                actionListener::onFailure);
    }

    private static MlMetadata.Builder createMlMetadataBuilder(ClusterState currentState) {
        return new MlMetadata.Builder(MlMetadata.getMlMetadata(currentState));
    }

    private static ClusterState buildNewClusterState(ClusterState currentState, MlMetadata.Builder builder) {
        XPackPlugin.checkReadyForXPackCustomMetadata(currentState);
        ClusterState.Builder newState = ClusterState.builder(currentState);
        newState.metaData(MetaData.builder(currentState.getMetaData()).putCustom(MlMetadata.TYPE, builder.build()).build());
        return newState.build();
    }
}
