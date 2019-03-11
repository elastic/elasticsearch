/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ResourceAlreadyExistsException;
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
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.analysis.AnalysisRegistry;
import org.elasticsearch.persistent.PersistentTasksCustomMetaData;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.ml.MachineLearningField;
import org.elasticsearch.xpack.core.ml.MlMetadata;
import org.elasticsearch.xpack.core.ml.MlTasks;
import org.elasticsearch.xpack.core.ml.action.DeleteJobAction;
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
import org.elasticsearch.xpack.ml.MlConfigMigrationEligibilityCheck;
import org.elasticsearch.xpack.ml.job.categorization.CategorizationAnalyzer;
import org.elasticsearch.xpack.ml.job.persistence.ExpandedIdsMatcher;
import org.elasticsearch.xpack.ml.job.persistence.JobConfigProvider;
import org.elasticsearch.xpack.ml.job.persistence.JobResultsPersister;
import org.elasticsearch.xpack.ml.job.persistence.JobResultsProvider;
import org.elasticsearch.xpack.ml.job.process.autodetect.UpdateParams;
import org.elasticsearch.xpack.ml.notifications.Auditor;
import org.elasticsearch.xpack.ml.utils.VoidChainTaskExecutor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Allows interactions with jobs. The managed interactions include:
 * <ul>
 * <li>creation</li>
 * <li>reading</li>
 * <li>deletion</li>
 * <li>updating</li>
 * </ul>
 */
public class JobManager {

    private static final DeprecationLogger DEPRECATION_LOGGER = new DeprecationLogger(LogManager.getLogger(JobManager.class));
    private static final Logger logger = LogManager.getLogger(JobManager.class);

    private final Settings settings;
    private final Environment environment;
    private final JobResultsProvider jobResultsProvider;
    private final ClusterService clusterService;
    private final Auditor auditor;
    private final Client client;
    private final ThreadPool threadPool;
    private final UpdateJobProcessNotifier updateJobProcessNotifier;
    private final JobConfigProvider jobConfigProvider;
    private final MlConfigMigrationEligibilityCheck migrationEligibilityCheck;

    private volatile ByteSizeValue maxModelMemoryLimit;

    /**
     * Create a JobManager
     */
    public JobManager(Environment environment, Settings settings, JobResultsProvider jobResultsProvider,
                      ClusterService clusterService, Auditor auditor, ThreadPool threadPool,
                      Client client, UpdateJobProcessNotifier updateJobProcessNotifier, NamedXContentRegistry xContentRegistry) {
        this(environment, settings, jobResultsProvider, clusterService, auditor, threadPool, client,
                updateJobProcessNotifier, new JobConfigProvider(client, xContentRegistry));
    }

    JobManager(Environment environment, Settings settings, JobResultsProvider jobResultsProvider,
                      ClusterService clusterService, Auditor auditor, ThreadPool threadPool,
                      Client client, UpdateJobProcessNotifier updateJobProcessNotifier, JobConfigProvider jobConfigProvider) {
        this.settings = settings;
        this.environment = environment;
        this.jobResultsProvider = Objects.requireNonNull(jobResultsProvider);
        this.clusterService = Objects.requireNonNull(clusterService);
        this.auditor = Objects.requireNonNull(auditor);
        this.client = Objects.requireNonNull(client);
        this.threadPool = Objects.requireNonNull(threadPool);
        this.updateJobProcessNotifier = updateJobProcessNotifier;
        this.jobConfigProvider = jobConfigProvider;
        this.migrationEligibilityCheck = new MlConfigMigrationEligibilityCheck(settings, clusterService);

        maxModelMemoryLimit = MachineLearningField.MAX_MODEL_MEMORY_LIMIT.get(settings);
        clusterService.getClusterSettings()
                .addSettingsUpdateConsumer(MachineLearningField.MAX_MODEL_MEMORY_LIMIT, this::setMaxModelMemoryLimit);
    }

    private void setMaxModelMemoryLimit(ByteSizeValue maxModelMemoryLimit) {
        this.maxModelMemoryLimit = maxModelMemoryLimit;
    }

    public void groupExists(String groupId, ActionListener<Boolean> listener) {
        MlMetadata mlMetadata = MlMetadata.getMlMetadata(clusterService.state());
        boolean groupExistsInMlMetadata = mlMetadata.expandGroupIds(groupId).isEmpty() == false;
        if (groupExistsInMlMetadata) {
            listener.onResponse(Boolean.TRUE);
        } else {
            jobConfigProvider.groupExists(groupId, listener);
        }
    }

    public void jobExists(String jobId, ActionListener<Boolean> listener) {
        if (MlMetadata.getMlMetadata(clusterService.state()).getJobs().containsKey(jobId)) {
            listener.onResponse(Boolean.TRUE);
        } else {
            // check the index
            jobConfigProvider.jobExists(jobId, true, ActionListener.wrap(
                    jobFound -> listener.onResponse(jobFound),
                    listener::onFailure
            ));
        }
    }

    /**
     * Gets the job that matches the given {@code jobId}.
     *
     * @param jobId the jobId
     * @param jobListener the Job listener. If no job matches {@code jobId}
     *                    a ResourceNotFoundException is returned
     */
    public void getJob(String jobId, ActionListener<Job> jobListener) {
        Job job = MlMetadata.getMlMetadata(clusterService.state()).getJobs().get(jobId);
        if (job != null) {
            jobListener.onResponse(job);
        } else {
            jobConfigProvider.getJob(jobId, ActionListener.wrap(
                    r -> jobListener.onResponse(r.build()), // TODO JIndex we shouldn't be building the job here
                    jobListener::onFailure
            ));
        }
    }

    /**
     * Get the jobs that match the given {@code expression}.
     * Note that when the {@code jobId} is {@link MetaData#ALL} all jobs are returned.
     *
     * @param expression   the jobId or an expression matching jobIds
     * @param allowNoJobs  if {@code false}, an error is thrown when no job matches the {@code jobId}
     * @param jobsListener The jobs listener
     */
    public void expandJobs(String expression, boolean allowNoJobs, ActionListener<QueryPage<Job>> jobsListener) {
        Map<String, Job> clusterStateJobs = expandJobsFromClusterState(expression, clusterService.state());
        ExpandedIdsMatcher requiredMatches = new ExpandedIdsMatcher(expression, allowNoJobs);
        requiredMatches.filterMatchedIds(clusterStateJobs.keySet());

        // If expression contains a group Id it has been expanded to its
        // constituent job Ids but Ids matcher needs to know the group
        // has been matched
        requiredMatches.filterMatchedIds(MlMetadata.getMlMetadata(clusterService.state()).expandGroupIds(expression));

        jobConfigProvider.expandJobsWithoutMissingcheck(expression, false, ActionListener.wrap(
                jobBuilders -> {
                    Set<String> jobAndGroupIds = new HashSet<>();

                    List<Job> jobs = new ArrayList<>(clusterStateJobs.values());

                    // Duplicate configs existing in both the clusterstate and index documents are ok
                    // this may occur during migration of configs.
                    // Prefer the clusterstate configs and filter duplicates from the index
                    for (Job.Builder jb : jobBuilders) {
                        if (clusterStateJobs.containsKey(jb.getId()) == false) {
                            Job job = jb.build();
                            jobAndGroupIds.add(job.getId());
                            jobAndGroupIds.addAll(job.getGroups());
                            jobs.add(job);
                        }
                    }

                    requiredMatches.filterMatchedIds(jobAndGroupIds);

                    if (requiredMatches.hasUnmatchedIds()) {
                        jobsListener.onFailure(ExceptionsHelper.missingJobException(requiredMatches.unmatchedIdsString()));
                    } else {
                        Collections.sort(jobs, Comparator.comparing(Job::getId));
                        jobsListener.onResponse(new QueryPage<>(jobs, jobs.size(), Job.RESULTS_FIELD));
                    }
                },
                jobsListener::onFailure
        ));
    }

    private Map<String, Job> expandJobsFromClusterState(String expression, ClusterState clusterState) {
        Map<String, Job> jobIdToJob = new HashMap<>();
        Set<String> expandedJobIds = MlMetadata.getMlMetadata(clusterState).expandJobIds(expression);
        MlMetadata mlMetadata = MlMetadata.getMlMetadata(clusterState);
        for (String expandedJobId : expandedJobIds) {
            jobIdToJob.put(expandedJobId, mlMetadata.getJobs().get(expandedJobId));
        }
        return jobIdToJob;
    }

    /**
     * Get the job Ids that match the given {@code expression}.
     *
     * @param expression   the jobId or an expression matching jobIds
     * @param allowNoJobs  if {@code false}, an error is thrown when no job matches the {@code jobId}
     * @param jobsListener The jobs listener
     */
    public void expandJobIds(String expression, boolean allowNoJobs, ActionListener<SortedSet<String>> jobsListener) {
        Set<String> clusterStateJobIds = MlMetadata.getMlMetadata(clusterService.state()).expandJobIds(expression);
        ExpandedIdsMatcher requiredMatches = new ExpandedIdsMatcher(expression, allowNoJobs);
        requiredMatches.filterMatchedIds(clusterStateJobIds);
        // If expression contains a group Id it has been expanded to its
        // constituent job Ids but Ids matcher needs to know the group
        // has been matched
        requiredMatches.filterMatchedIds(MlMetadata.getMlMetadata(clusterService.state()).expandGroupIds(expression));

        jobConfigProvider.expandJobsIdsWithoutMissingCheck(expression, false, ActionListener.wrap(
                jobIdsAndGroups -> {
                    requiredMatches.filterMatchedIds(jobIdsAndGroups.getJobs());
                    requiredMatches.filterMatchedIds(jobIdsAndGroups.getGroups());
                    if (requiredMatches.hasUnmatchedIds()) {
                        jobsListener.onFailure(ExceptionsHelper.missingJobException(requiredMatches.unmatchedIdsString()));
                    } else {
                        SortedSet<String> allJobIds = new TreeSet<>(clusterStateJobIds);
                        allJobIds.addAll(jobIdsAndGroups.getJobs());
                        jobsListener.onResponse(allJobIds);
                    }
                },
                jobsListener::onFailure
        ));
    }

    /**
     * Mark the job as being deleted. First looks in the cluster state for the
     * job configuration then the index
     *
     * @param jobId     To to mark
     * @param force     Allows an open job to be marked
     * @param listener  listener
     */
    public void markJobAsDeleting(String jobId, boolean force, ActionListener<Boolean> listener) {
        if (ClusterStateJobUpdate.jobIsInClusterState(clusterService.state(), jobId)) {
            ClusterStateJobUpdate.markJobAsDeleting(jobId, force, clusterService, listener);
        } else {
            jobConfigProvider.markJobAsDeleting(jobId, listener);
        }
    }

    /**
     * First try to delete the job from the cluster state, if it does not exist
     * there try to  delete the index job.
     *
     * @param request   The delete job request
     * @param listener  Delete listener
     */
    public void deleteJob(DeleteJobAction.Request request, ActionListener<Boolean> listener) {
        if (ClusterStateJobUpdate.jobIsInClusterState(clusterService.state(), request.getJobId())) {
            ClusterStateJobUpdate.deleteJob(request, clusterService, listener);
        } else {
            jobConfigProvider.deleteJob(request.getJobId(), false, ActionListener.wrap(
                    deleteResponse -> listener.onResponse(Boolean.TRUE),
                    listener::onFailure
            ));
        }
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
     * Stores the anomaly job configuration
     */
    public void putJob(PutJobAction.Request request, AnalysisRegistry analysisRegistry, ClusterState state,
                       ActionListener<PutJobAction.Response> actionListener) throws IOException {

        request.getJobBuilder().validateAnalysisLimitsAndSetDefaults(maxModelMemoryLimit);
        validateCategorizationAnalyzer(request.getJobBuilder(), analysisRegistry, environment);

        Job job = request.getJobBuilder().build(new Date());

        if (job.getDataDescription() != null && job.getDataDescription().getFormat() == DataDescription.DataFormat.DELIMITED) {
            DEPRECATION_LOGGER.deprecated("Creating jobs with delimited data format is deprecated. Please use xcontent instead.");
        }

        // Check for the job in the cluster state first
        MlMetadata currentMlMetadata = MlMetadata.getMlMetadata(state);
        if (ClusterStateJobUpdate.jobIsInMlMetadata(currentMlMetadata, job.getId())) {
            actionListener.onFailure(ExceptionsHelper.jobAlreadyExists(job.getId()));
            return;
        }

        // Check the job id is not the same as a group Id
        if (currentMlMetadata.isGroupOrJob(job.getId())) {
            actionListener.onFailure(new
                    ResourceAlreadyExistsException(Messages.getMessage(Messages.JOB_AND_GROUP_NAMES_MUST_BE_UNIQUE, job.getId())));
            return;
        }

        // and that the new job's groups are not job Ids
        for (String group : job.getGroups()) {
            if (currentMlMetadata.getJobs().containsKey(group)) {
                actionListener.onFailure(new
                        ResourceAlreadyExistsException(Messages.getMessage(Messages.JOB_AND_GROUP_NAMES_MUST_BE_UNIQUE, group)));
                return;
            }
        }

        ActionListener<Boolean> putJobListener = new ActionListener<Boolean>() {
            @Override
            public void onResponse(Boolean indicesCreated) {

                jobConfigProvider.putJob(job, ActionListener.wrap(
                        response -> {
                            auditor.info(job.getId(), Messages.getMessage(Messages.JOB_AUDIT_CREATED));
                            actionListener.onResponse(new PutJobAction.Response(job));
                        },
                        actionListener::onFailure
                ));
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

        ActionListener<List<String>> checkForLeftOverDocs = ActionListener.wrap(
                matchedIds -> {
                    if (matchedIds.isEmpty()) {
                        jobResultsProvider.createJobResultIndex(job, state, putJobListener);
                    } else {
                        // A job has the same Id as one of the group names
                        // error with the first in the list
                        actionListener.onFailure(new ResourceAlreadyExistsException(
                                Messages.getMessage(Messages.JOB_AND_GROUP_NAMES_MUST_BE_UNIQUE, matchedIds.get(0))));
                    }
                },
                actionListener::onFailure
        );

        ActionListener<Boolean> checkNoJobsWithGroupId = ActionListener.wrap(
                groupExists -> {
                    if (groupExists) {
                        actionListener.onFailure(new ResourceAlreadyExistsException(
                                Messages.getMessage(Messages.JOB_AND_GROUP_NAMES_MUST_BE_UNIQUE, job.getId())));
                        return;
                    }
                    if (job.getGroups().isEmpty()) {
                        checkForLeftOverDocs.onResponse(Collections.emptyList());
                    } else {
                        jobConfigProvider.jobIdMatches(job.getGroups(), checkForLeftOverDocs);
                    }
                },
                actionListener::onFailure
        );

        ActionListener<Boolean> checkNoGroupWithTheJobId = ActionListener.wrap(
                ok -> {
                    jobConfigProvider.groupExists(job.getId(), checkNoJobsWithGroupId);
                },
                actionListener::onFailure
        );

        jobConfigProvider.jobExists(job.getId(), false, ActionListener.wrap(
                jobExists -> {
                    if (jobExists) {
                        actionListener.onFailure(ExceptionsHelper.jobAlreadyExists(job.getId()));
                    } else {
                        jobResultsProvider.checkForLeftOverDocuments(job, checkNoGroupWithTheJobId);
                    }
                },
                actionListener::onFailure
        ));
    }

    public void updateJob(UpdateJobAction.Request request, ActionListener<PutJobAction.Response> actionListener) {
        ClusterState clusterState = clusterService.state();
        if (migrationEligibilityCheck.jobIsEligibleForMigration(request.getJobId(), clusterState)) {
            actionListener.onFailure(ExceptionsHelper.configHasNotBeenMigrated("update job", request.getJobId()));
            return;
        }

        MlMetadata mlMetadata = MlMetadata.getMlMetadata(clusterState);

        if (request.getJobUpdate().getGroups() != null && request.getJobUpdate().getGroups().isEmpty() == false) {

            // check the new groups are not job Ids
            for (String group : request.getJobUpdate().getGroups()) {
                if (mlMetadata.getJobs().containsKey(group)) {
                    actionListener.onFailure(new ResourceAlreadyExistsException(
                            Messages.getMessage(Messages.JOB_AND_GROUP_NAMES_MUST_BE_UNIQUE, group)));
                }
            }

            jobConfigProvider.jobIdMatches(request.getJobUpdate().getGroups(), ActionListener.wrap(
                    matchingIds -> {
                        if (matchingIds.isEmpty()) {
                            updateJobPostInitialChecks(request, mlMetadata, actionListener);
                        } else {
                            actionListener.onFailure(new ResourceAlreadyExistsException(
                                    Messages.getMessage(Messages.JOB_AND_GROUP_NAMES_MUST_BE_UNIQUE, matchingIds.get(0))));
                        }
                    },
                    actionListener::onFailure
            ));
        } else {
            updateJobPostInitialChecks(request, mlMetadata, actionListener);
        }
    }

    private void updateJobPostInitialChecks(UpdateJobAction.Request request, MlMetadata mlMetadata,
                           ActionListener<PutJobAction.Response> actionListener) {

        if (ClusterStateJobUpdate.jobIsInMlMetadata(mlMetadata, request.getJobId())) {
            updateJobClusterState(request, actionListener);
        } else {
            updateJobIndex(request, ActionListener.wrap(
                    updatedJob -> {
                        postJobUpdate(clusterService.state(), request);
                        actionListener.onResponse(new PutJobAction.Response(updatedJob));
                    },
                    actionListener::onFailure
            ));
        }
    }

    private void postJobUpdate(ClusterState clusterState, UpdateJobAction.Request request) {
        JobUpdate jobUpdate = request.getJobUpdate();

        // Change is required if the fields that the C++ uses are being updated
        boolean processUpdateRequired = jobUpdate.isAutodetectProcessUpdate();

        if (processUpdateRequired && isJobOpen(clusterState, request.getJobId())) {
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

    private void updateJobIndex(UpdateJobAction.Request request, ActionListener<Job> updatedJobListener) {
        jobConfigProvider.updateJobWithValidation(request.getJobId(), request.getJobUpdate(), maxModelMemoryLimit,
                this::validate, clusterService.state().nodes().getMinNodeVersion(), updatedJobListener);
    }

    private void updateJobClusterState(UpdateJobAction.Request request, ActionListener<PutJobAction.Response> actionListener) {
        Job job = MlMetadata.getMlMetadata(clusterService.state()).getJobs().get(request.getJobId());
        validate(job, request.getJobUpdate(), ActionListener.wrap(
                nullValue -> clusterStateJobUpdate(request, actionListener),
                actionListener::onFailure));
    }

    private void clusterStateJobUpdate(UpdateJobAction.Request request, ActionListener<PutJobAction.Response> actionListener) {
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
                            Job job = MlMetadata.getMlMetadata(clusterService.state()).getJobs().get(request.getJobId());
                            updatedJob.set(request.getJobUpdate().mergeWithJob(job, maxModelMemoryLimit));
                            return ClusterStateJobUpdate.putJobInClusterState(updatedJob.get(), true, currentState);
                        }

                        @Override
                        public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                            postJobUpdate(newState, request);
                        }
                    });
        } else {
            clusterService.submitStateUpdateTask("update-job-" + request.getJobId(), new ClusterStateUpdateTask() {
                private AtomicReference<Job> updatedJob = new AtomicReference<>();

                @Override
                public ClusterState execute(ClusterState currentState) throws Exception {
                    Job job = MlMetadata.getMlMetadata(clusterService.state()).getJobs().get(request.getJobId());
                    updatedJob.set(request.getJobUpdate().mergeWithJob(job, maxModelMemoryLimit));
                    return ClusterStateJobUpdate.putJobInClusterState(updatedJob.get(), true, currentState);
                }

                @Override
                public void onFailure(String source, Exception e) {
                    actionListener.onFailure(e);
                }

                @Override
                public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                    postJobUpdate(newState, request);
                    actionListener.onResponse(new PutJobAction.Response(updatedJob.get()));
                }
            });
        }
    }

    private void validate(Job job, JobUpdate jobUpdate, ActionListener<Void> handler) {
        VoidChainTaskExecutor voidChainTaskExecutor = new VoidChainTaskExecutor(client.threadPool().executor(
                MachineLearning.UTILITY_THREAD_POOL_NAME), true);
        validateModelSnapshotIdUpdate(job, jobUpdate.getModelSnapshotId(), voidChainTaskExecutor);
        validateAnalysisLimitsUpdate(job, jobUpdate.getAnalysisLimits(), voidChainTaskExecutor);
        voidChainTaskExecutor.execute(ActionListener.wrap(aVoids -> handler.onResponse(null), handler::onFailure));
    }

    private void validateModelSnapshotIdUpdate(Job job, String modelSnapshotId, VoidChainTaskExecutor voidChainTaskExecutor) {
        if (modelSnapshotId != null) {
            voidChainTaskExecutor.add(listener -> {
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

    private void validateAnalysisLimitsUpdate(Job job, AnalysisLimits newLimits, VoidChainTaskExecutor voidChainTaskExecutor) {
        if (newLimits == null || newLimits.getModelMemoryLimit() == null) {
            return;
        }
        Long newModelMemoryLimit = newLimits.getModelMemoryLimit();
        voidChainTaskExecutor.add(listener -> {
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

    private Set<String> openJobIds(ClusterState clusterState) {
        PersistentTasksCustomMetaData persistentTasks = clusterState.metaData().custom(PersistentTasksCustomMetaData.TYPE);
        return MlTasks.openJobIds(persistentTasks);
    }

    public void notifyFilterChanged(MlFilter filter, Set<String> addedItems, Set<String> removedItems,
                                    ActionListener<Boolean> updatedListener) {
        if (addedItems.isEmpty() && removedItems.isEmpty()) {
            updatedListener.onResponse(Boolean.TRUE);
            return;
        }

        // Read both cluster state and index jobs
        Map<String, Job> clusterStateJobs = expandJobsFromClusterState(MetaData.ALL, clusterService.state());

        jobConfigProvider.findJobsWithCustomRules(ActionListener.wrap(
                indexJobs -> {
                    threadPool.executor(MachineLearning.UTILITY_THREAD_POOL_NAME).execute(() -> {

                        List<Job> allJobs = new ArrayList<>(clusterStateJobs.values());

                        // Duplicate configs existing in both the clusterstate and index documents are ok
                        // this may occur during migration of configs.
                        // Filter the duplicates so we don't update twice for duplicated jobs
                        for (Job indexJob : indexJobs) {
                            if (clusterStateJobs.containsKey(indexJob.getId()) == false) {
                                allJobs.add(indexJob);
                            }
                        }

                        for (Job job: allJobs) {
                            Set<String> jobFilters = job.getAnalysisConfig().extractReferencedFilters();
                            ClusterState clusterState = clusterService.state();
                            if (jobFilters.contains(filter.getId())) {
                                if (isJobOpen(clusterState, job.getId())) {
                                    updateJobProcessNotifier.submitJobUpdate(UpdateParams.filterUpdate(job.getId(), filter),
                                            ActionListener.wrap(isUpdated -> {
                                                auditFilterChanges(job.getId(), filter.getId(), addedItems, removedItems);
                                            }, e -> {
                                            }));
                                } else {
                                    auditFilterChanges(job.getId(), filter.getId(), addedItems, removedItems);
                                }
                            }
                        }

                        updatedListener.onResponse(Boolean.TRUE);
                    });
                },
                updatedListener::onFailure
        ));
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

    public void updateProcessOnCalendarChanged(List<String> calendarJobIds, ActionListener<Boolean> updateListener) {
        ClusterState clusterState = clusterService.state();
        Set<String> openJobIds = openJobIds(clusterState);
        if (openJobIds.isEmpty()) {
            updateListener.onResponse(Boolean.TRUE);
            return;
        }

        // Get the cluster state jobs that match
        MlMetadata mlMetadata = MlMetadata.getMlMetadata(clusterState);
        List<String> existingJobsOrGroups =
                calendarJobIds.stream().filter(mlMetadata::isGroupOrJob).collect(Collectors.toList());

        Set<String> clusterStateIds = new HashSet<>();
        existingJobsOrGroups.forEach(jobId -> clusterStateIds.addAll(mlMetadata.expandJobIds(jobId)));

        // calendarJobIds may be a group or job.
        // Expand the groups to the constituent job ids
        jobConfigProvider.expandGroupIds(calendarJobIds, ActionListener.wrap(
                expandedIds -> {
                    threadPool.executor(MachineLearning.UTILITY_THREAD_POOL_NAME).execute(() -> {
                        // Merge the expanded group members with the request Ids
                        // which are job ids rather than group Ids.
                        expandedIds.addAll(calendarJobIds);

                        // Merge in the cluster state job ids
                        expandedIds.addAll(clusterStateIds);

                        for (String jobId : expandedIds) {
                            if (isJobOpen(clusterState, jobId)) {
                                updateJobProcessNotifier.submitJobUpdate(UpdateParams.scheduledEventsUpdate(jobId), ActionListener.wrap(
                                        isUpdated -> {
                                            if (isUpdated) {
                                                auditor.info(jobId, Messages.getMessage(Messages.JOB_AUDIT_CALENDARS_UPDATED_ON_PROCESS));
                                            }
                                        },
                                        e -> logger.error("[" + jobId + "] failed submitting process update on calendar change", e)
                                ));
                            }
                        }

                        updateListener.onResponse(Boolean.TRUE);
                    });
                },
                updateListener::onFailure
        ));
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

        // Step 1. update the job
        // -------

        Consumer<Long> updateJobHandler;

        if (ClusterStateJobUpdate.jobIsInClusterState(clusterService.state(), request.getJobId())) {
            updateJobHandler = response -> clusterService.submitStateUpdateTask("revert-snapshot-" + request.getJobId(),
                    new AckedClusterStateUpdateTask<Boolean>(request, ActionListener.wrap(updateHandler, actionListener::onFailure)) {

                        @Override
                        protected Boolean newResponse(boolean acknowledged) {
                            if (acknowledged) {
                                auditor.info(request.getJobId(),
                                        Messages.getMessage(Messages.JOB_AUDIT_REVERTED, modelSnapshot.getDescription()));
                                return true;
                            }
                            actionListener.onFailure(new IllegalStateException("Could not revert modelSnapshot on job ["
                                    + request.getJobId() + "], not acknowledged by master."));
                            return false;
                        }

                        @Override
                        public ClusterState execute(ClusterState currentState) {
                            Job job = MlMetadata.getMlMetadata(currentState).getJobs().get(request.getJobId());
                            Job.Builder builder = new Job.Builder(job);
                            builder.setModelSnapshotId(modelSnapshot.getSnapshotId());
                            builder.setEstablishedModelMemory(response);
                            return ClusterStateJobUpdate.putJobInClusterState(builder.build(), true, currentState);
                        }
                    });
        } else {
            updateJobHandler = response -> {
                JobUpdate update = new JobUpdate.Builder(request.getJobId())
                        .setModelSnapshotId(modelSnapshot.getSnapshotId())
                        .setEstablishedModelMemory(response)
                        .build();

                jobConfigProvider.updateJob(request.getJobId(), update, maxModelMemoryLimit,
                    clusterService.state().nodes().getMinNodeVersion(), ActionListener.wrap(
                        job -> {
                            auditor.info(request.getJobId(),
                                    Messages.getMessage(Messages.JOB_AUDIT_REVERTED, modelSnapshot.getDescription()));
                            updateHandler.accept(Boolean.TRUE);
                        },
                        actionListener::onFailure
                ));
            };
        }

        // Step 0. Find the appropriate established model memory for the reverted job
        // -------
        jobResultsProvider.getEstablishedMemoryUsage(request.getJobId(), modelSizeStats.getTimestamp(), modelSizeStats, updateJobHandler,
                actionListener::onFailure);
    }
}
