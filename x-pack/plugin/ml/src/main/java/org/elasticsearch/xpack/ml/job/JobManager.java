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
import org.elasticsearch.cluster.ClusterState;
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
import org.elasticsearch.xpack.core.ml.action.PutJobAction;
import org.elasticsearch.xpack.core.ml.action.RevertModelSnapshotAction;
import org.elasticsearch.xpack.core.ml.action.UpdateJobAction;
import org.elasticsearch.xpack.core.action.util.QueryPage;
import org.elasticsearch.xpack.core.ml.job.config.AnalysisLimits;
import org.elasticsearch.xpack.core.ml.job.config.CategorizationAnalyzerConfig;
import org.elasticsearch.xpack.core.ml.job.config.DataDescription;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.core.ml.job.config.JobState;
import org.elasticsearch.xpack.core.ml.job.config.JobUpdate;
import org.elasticsearch.xpack.core.ml.job.config.MlFilter;
import org.elasticsearch.xpack.core.ml.job.messages.Messages;
import org.elasticsearch.xpack.core.ml.job.persistence.AnomalyDetectorsIndex;
import org.elasticsearch.xpack.core.ml.job.persistence.ElasticsearchMappings;
import org.elasticsearch.xpack.core.ml.job.process.autodetect.state.ModelSizeStats;
import org.elasticsearch.xpack.core.ml.job.process.autodetect.state.ModelSnapshot;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.ml.MachineLearning;
import org.elasticsearch.xpack.ml.MlConfigMigrationEligibilityCheck;
import org.elasticsearch.xpack.ml.job.categorization.CategorizationAnalyzer;
import org.elasticsearch.xpack.ml.job.persistence.JobConfigProvider;
import org.elasticsearch.xpack.ml.job.persistence.JobResultsPersister;
import org.elasticsearch.xpack.ml.job.persistence.JobResultsProvider;
import org.elasticsearch.xpack.ml.job.process.autodetect.UpdateParams;
import org.elasticsearch.xpack.ml.notifications.AnomalyDetectionAuditor;
import org.elasticsearch.xpack.ml.utils.VoidChainTaskExecutor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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

    private static final Logger logger = LogManager.getLogger(JobManager.class);
    private static final DeprecationLogger deprecationLogger = new DeprecationLogger(logger);

    private final Environment environment;
    private final JobResultsProvider jobResultsProvider;
    private final JobResultsPersister jobResultsPersister;
    private final ClusterService clusterService;
    private final AnomalyDetectionAuditor auditor;
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
                      JobResultsPersister jobResultsPersister, ClusterService clusterService, AnomalyDetectionAuditor auditor,
                      ThreadPool threadPool, Client client, UpdateJobProcessNotifier updateJobProcessNotifier,
                      NamedXContentRegistry xContentRegistry) {
        this.environment = environment;
        this.jobResultsProvider = Objects.requireNonNull(jobResultsProvider);
        this.jobResultsPersister = Objects.requireNonNull(jobResultsPersister);
        this.clusterService = Objects.requireNonNull(clusterService);
        this.auditor = Objects.requireNonNull(auditor);
        this.client = Objects.requireNonNull(client);
        this.threadPool = Objects.requireNonNull(threadPool);
        this.updateJobProcessNotifier = updateJobProcessNotifier;
        this.jobConfigProvider = new JobConfigProvider(client, xContentRegistry);
        this.migrationEligibilityCheck = new MlConfigMigrationEligibilityCheck(settings, clusterService);

        maxModelMemoryLimit = MachineLearningField.MAX_MODEL_MEMORY_LIMIT.get(settings);
        clusterService.getClusterSettings()
                .addSettingsUpdateConsumer(MachineLearningField.MAX_MODEL_MEMORY_LIMIT, this::setMaxModelMemoryLimit);
    }

    private void setMaxModelMemoryLimit(ByteSizeValue maxModelMemoryLimit) {
        this.maxModelMemoryLimit = maxModelMemoryLimit;
    }

    public void jobExists(String jobId, ActionListener<Boolean> listener) {
        jobConfigProvider.jobExists(jobId, true, listener);
    }

    /**
     * Gets the job that matches the given {@code jobId}.
     *
     * @param jobId the jobId
     * @param jobListener the Job listener. If no job matches {@code jobId}
     *                    a ResourceNotFoundException is returned
     */
    public void getJob(String jobId, ActionListener<Job> jobListener) {
        jobConfigProvider.getJob(jobId, ActionListener.wrap(
                r -> jobListener.onResponse(r.build()), // TODO JIndex we shouldn't be building the job here
                e -> {
                    if (e instanceof ResourceNotFoundException) {
                        // Try to get the job from the cluster state
                        getJobFromClusterState(jobId, jobListener);
                    } else {
                        jobListener.onFailure(e);
                    }
                }
        ));
    }

    /**
     * Read a job from the cluster state.
     * The job is returned on the same thread even though a listener is used.
     *
     * @param jobId the jobId
     * @param jobListener the Job listener. If no job matches {@code jobId}
     *                    a ResourceNotFoundException is returned
     */
    private void getJobFromClusterState(String jobId, ActionListener<Job> jobListener) {
        Job job = MlMetadata.getMlMetadata(clusterService.state()).getJobs().get(jobId);
        if (job == null) {
            jobListener.onFailure(ExceptionsHelper.missingJobException(jobId));
        } else {
            jobListener.onResponse(job);
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
        Map<String, Job> clusterStateJobs = expandJobsFromClusterState(expression, allowNoJobs, clusterService.state());

        jobConfigProvider.expandJobs(expression, allowNoJobs, false, ActionListener.wrap(
                jobBuilders -> {
                    // Check for duplicate jobs
                    for (Job.Builder jb : jobBuilders) {
                        if (clusterStateJobs.containsKey(jb.getId())) {
                            jobsListener.onFailure(new IllegalStateException("Job [" + jb.getId() + "] configuration " +
                                    "exists in both clusterstate and index"));
                            return;
                        }
                    }

                    // Merge cluster state and index jobs
                    List<Job> jobs = new ArrayList<>();
                    for (Job.Builder jb : jobBuilders) {
                        jobs.add(jb.build());
                    }

                    jobs.addAll(clusterStateJobs.values());
                    Collections.sort(jobs, Comparator.comparing(Job::getId));
                    jobsListener.onResponse(new QueryPage<>(jobs, jobs.size(), Job.RESULTS_FIELD));
                },
                jobsListener::onFailure
        ));
    }

    private Map<String, Job> expandJobsFromClusterState(String expression, boolean allowNoJobs, ClusterState clusterState) {
        Map<String, Job> jobIdToJob = new HashMap<>();
        try {
            Set<String> expandedJobIds = MlMetadata.getMlMetadata(clusterState).expandJobIds(expression, allowNoJobs);
            MlMetadata mlMetadata = MlMetadata.getMlMetadata(clusterState);
            for (String expandedJobId : expandedJobIds) {
                jobIdToJob.put(expandedJobId, mlMetadata.getJobs().get(expandedJobId));
            }
        } catch (Exception e) {
            // ignore
        }
        return jobIdToJob;
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
                analysisRegistry);
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
            deprecationLogger.deprecated("Creating jobs with delimited data format is deprecated. Please use xcontent instead.");
        }

        // Check for the job in the cluster state first
        MlMetadata currentMlMetadata = MlMetadata.getMlMetadata(state);
        if (currentMlMetadata.getJobs().containsKey(job.getId())) {
            actionListener.onFailure(ExceptionsHelper.jobAlreadyExists(job.getId()));
            return;
        }

        ActionListener<Boolean> putJobListener = new ActionListener<Boolean>() {
            @Override
            public void onResponse(Boolean mappingsUpdated) {

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

        ActionListener<Boolean> addDocMappingsListener = ActionListener.wrap(
            indicesCreated -> {
                if (state == null) {
                    logger.warn("Cannot update doc mapping because clusterState == null");
                    putJobListener.onResponse(false);
                    return;
                }
                ElasticsearchMappings.addDocMappingIfMissing(
                    AnomalyDetectorsIndex.configIndexName(), ElasticsearchMappings::configMapping, client, state, putJobListener);
            },
            putJobListener::onFailure
        );

        ActionListener<List<String>> checkForLeftOverDocs = ActionListener.wrap(
                matchedIds -> {
                    if (matchedIds.isEmpty()) {
                        jobResultsProvider.createJobResultIndex(job, state, addDocMappingsListener);
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

        Runnable doUpdate = () -> {
                jobConfigProvider.updateJobWithValidation(request.getJobId(), request.getJobUpdate(), maxModelMemoryLimit,
                        this::validate, ActionListener.wrap(
                                updatedJob -> postJobUpdate(request, updatedJob, actionListener),
                                actionListener::onFailure
                        ));
        };

        ClusterState clusterState = clusterService.state();
        if (migrationEligibilityCheck.jobIsEligibleForMigration(request.getJobId(), clusterState)) {
            actionListener.onFailure(ExceptionsHelper.configHasNotBeenMigrated("update job", request.getJobId()));
            return;
        }

        if (request.getJobUpdate().getGroups() != null && request.getJobUpdate().getGroups().isEmpty() == false) {

            // check the new groups are not job Ids
            jobConfigProvider.jobIdMatches(request.getJobUpdate().getGroups(), ActionListener.wrap(
                    matchingIds -> {
                        if (matchingIds.isEmpty()) {
                            doUpdate.run();
                        } else {
                            actionListener.onFailure(new ResourceAlreadyExistsException(
                                    Messages.getMessage(Messages.JOB_AND_GROUP_NAMES_MUST_BE_UNIQUE, matchingIds.get(0))));
                        }
                    },
                    actionListener::onFailure
            ));
        } else {
            doUpdate.run();
        }
    }

    private void postJobUpdate(UpdateJobAction.Request request, Job updatedJob, ActionListener<PutJobAction.Response> actionListener) {
        // Autodetect must be updated if the fields that the C++ uses are changed
        if (request.getJobUpdate().isAutodetectProcessUpdate()) {
            JobUpdate jobUpdate = request.getJobUpdate();
            if (isJobOpen(clusterService.state(), request.getJobId())) {
                updateJobProcessNotifier.submitJobUpdate(UpdateParams.fromJobUpdate(jobUpdate), ActionListener.wrap(
                        isUpdated -> {
                            if (isUpdated) {
                                auditJobUpdatedIfNotInternal(request);
                            }
                        }, e -> {
                            // No need to do anything
                        }
                ));
            }
        } else {
            logger.debug("[{}] No process update required for job update: {}", () -> request.getJobId(), () -> {
                try {
                    XContentBuilder jsonBuilder = XContentFactory.jsonBuilder();
                    request.getJobUpdate().toXContent(jsonBuilder, ToXContent.EMPTY_PARAMS);
                    return Strings.toString(jsonBuilder);
                } catch (IOException e) {
                    return "(unprintable due to " + e.getMessage() + ")";
                }
            });

            auditJobUpdatedIfNotInternal(request);
        }

        actionListener.onResponse(new PutJobAction.Response(updatedJob));
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

        jobConfigProvider.findJobsWithCustomRules(ActionListener.wrap(
                jobBuilders -> {
                    threadPool.executor(MachineLearning.UTILITY_THREAD_POOL_NAME).execute(() -> {
                        for (Job job: jobBuilders) {
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

        // calendarJobIds may be a group or job
        jobConfigProvider.expandGroupIds(calendarJobIds, ActionListener.wrap(
                expandedIds -> {
                    threadPool.executor(MachineLearning.UTILITY_THREAD_POOL_NAME).execute(() -> {
                        // Merge the expended group members with the request Ids.
                        // Ids that aren't jobs will be filtered by isJobOpen()
                        expandedIds.addAll(calendarJobIds);

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

        // Step 3. After the model size stats is persisted, also persist the snapshot's quantiles and respond
        // -------
        CheckedConsumer<IndexResponse, Exception> modelSizeStatsResponseHandler = response -> {
            jobResultsPersister.persistQuantiles(modelSnapshot.getQuantiles(), WriteRequest.RefreshPolicy.IMMEDIATE,
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
                jobResultsPersister.persistModelSizeStats(revertedModelSizeStats, WriteRequest.RefreshPolicy.IMMEDIATE, ActionListener.wrap(
                        modelSizeStatsResponseHandler, actionListener::onFailure));
            }
        };

        // Step 1. update the job
        // -------
        JobUpdate update = new JobUpdate.Builder(request.getJobId())
            .setModelSnapshotId(modelSnapshot.getSnapshotId())
            .build();

        jobConfigProvider.updateJob(request.getJobId(), update, maxModelMemoryLimit,
            ActionListener.wrap(job -> {
                auditor.info(request.getJobId(),
                    Messages.getMessage(Messages.JOB_AUDIT_REVERTED, modelSnapshot.getDescription()));
                updateHandler.accept(Boolean.TRUE);
            },
            actionListener::onFailure
        ));
    }
}
