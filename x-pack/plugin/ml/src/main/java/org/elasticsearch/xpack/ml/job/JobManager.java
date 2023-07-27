/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.job;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.analysis.AnalysisRegistry;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xpack.core.action.util.QueryPage;
import org.elasticsearch.xpack.core.ml.MlConfigIndex;
import org.elasticsearch.xpack.core.ml.MlTasks;
import org.elasticsearch.xpack.core.ml.action.CancelJobModelSnapshotUpgradeAction;
import org.elasticsearch.xpack.core.ml.action.DeleteJobAction;
import org.elasticsearch.xpack.core.ml.action.PutJobAction;
import org.elasticsearch.xpack.core.ml.action.RevertModelSnapshotAction;
import org.elasticsearch.xpack.core.ml.action.UpdateJobAction;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedJobValidator;
import org.elasticsearch.xpack.core.ml.job.config.AnalysisConfig;
import org.elasticsearch.xpack.core.ml.job.config.AnalysisLimits;
import org.elasticsearch.xpack.core.ml.job.config.Blocked;
import org.elasticsearch.xpack.core.ml.job.config.CategorizationAnalyzerConfig;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.core.ml.job.config.JobState;
import org.elasticsearch.xpack.core.ml.job.config.JobUpdate;
import org.elasticsearch.xpack.core.ml.job.config.MlFilter;
import org.elasticsearch.xpack.core.ml.job.messages.Messages;
import org.elasticsearch.xpack.core.ml.job.persistence.ElasticsearchMappings;
import org.elasticsearch.xpack.core.ml.job.process.autodetect.state.ModelSizeStats;
import org.elasticsearch.xpack.core.ml.job.process.autodetect.state.ModelSnapshot;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.ml.MachineLearning;
import org.elasticsearch.xpack.ml.job.categorization.CategorizationAnalyzer;
import org.elasticsearch.xpack.ml.job.persistence.JobConfigProvider;
import org.elasticsearch.xpack.ml.job.persistence.JobDataDeleter;
import org.elasticsearch.xpack.ml.job.persistence.JobResultsPersister;
import org.elasticsearch.xpack.ml.job.persistence.JobResultsProvider;
import org.elasticsearch.xpack.ml.job.process.autodetect.UpdateParams;
import org.elasticsearch.xpack.ml.notifications.AnomalyDetectionAuditor;
import org.elasticsearch.xpack.ml.utils.VoidChainTaskExecutor;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.elasticsearch.core.Strings.format;

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

    private static final Version MIN_NODE_VERSION_FOR_STANDARD_CATEGORIZATION_ANALYZER = Version.V_7_14_0;

    private static final Logger logger = LogManager.getLogger(JobManager.class);

    private final JobResultsProvider jobResultsProvider;
    private final JobResultsPersister jobResultsPersister;
    private final ClusterService clusterService;
    private final AnomalyDetectionAuditor auditor;
    private final Client client;
    private final ThreadPool threadPool;
    private final UpdateJobProcessNotifier updateJobProcessNotifier;
    private final JobConfigProvider jobConfigProvider;
    private final NamedXContentRegistry xContentRegistry;
    private final IndexNameExpressionResolver indexNameExpressionResolver;
    private final Supplier<ByteSizeValue> maxModelMemoryLimitSupplier;

    /**
     * Create a JobManager
     */
    public JobManager(
        JobResultsProvider jobResultsProvider,
        JobResultsPersister jobResultsPersister,
        ClusterService clusterService,
        AnomalyDetectionAuditor auditor,
        ThreadPool threadPool,
        Client client,
        UpdateJobProcessNotifier updateJobProcessNotifier,
        NamedXContentRegistry xContentRegistry,
        IndexNameExpressionResolver indexNameExpressionResolver,
        Supplier<ByteSizeValue> maxModelMemoryLimitSupplier
    ) {
        this.jobResultsProvider = Objects.requireNonNull(jobResultsProvider);
        this.jobResultsPersister = Objects.requireNonNull(jobResultsPersister);
        this.clusterService = Objects.requireNonNull(clusterService);
        this.auditor = Objects.requireNonNull(auditor);
        this.client = Objects.requireNonNull(client);
        this.threadPool = Objects.requireNonNull(threadPool);
        this.updateJobProcessNotifier = updateJobProcessNotifier;
        this.jobConfigProvider = new JobConfigProvider(client, xContentRegistry);
        this.xContentRegistry = xContentRegistry;
        this.indexNameExpressionResolver = Objects.requireNonNull(indexNameExpressionResolver);
        this.maxModelMemoryLimitSupplier = Objects.requireNonNull(maxModelMemoryLimitSupplier);
    }

    public void jobExists(String jobId, @Nullable TaskId parentTaskId, ActionListener<Boolean> listener) {
        jobConfigProvider.jobExists(jobId, true, parentTaskId, listener);
    }

    /**
     * Gets the job that matches the given {@code jobId}.
     *
     * @param jobId the jobId
     * @param jobListener the Job listener. If no job matches {@code jobId}
     *                    a ResourceNotFoundException is returned
     */
    public void getJob(String jobId, ActionListener<Job> jobListener) {
        jobConfigProvider.getJob(
            jobId,
            null,
            ActionListener.wrap(
                r -> jobListener.onResponse(r.build()), // TODO JIndex we shouldn't be building the job here
                jobListener::onFailure
            )
        );
    }

    /**
     * Get the jobs as builder objects that match the given {@code expression}.
     * Note that when the {@code jobId} is {@link Metadata#ALL} all jobs are returned.
     *
     * @param expression   the jobId or an expression matching jobIds
     * @param allowNoMatch if {@code false}, an error is thrown when no job matches the {@code jobId}
     * @param parentTaskId The parent task ID if available
     * @param jobsListener The jobs listener
     */
    public void expandJobBuilders(
        String expression,
        boolean allowNoMatch,
        @Nullable TaskId parentTaskId,
        ActionListener<List<Job.Builder>> jobsListener
    ) {
        jobConfigProvider.expandJobs(expression, allowNoMatch, false, parentTaskId, jobsListener);
    }

    /**
     * Get the jobs that match the given {@code expression}.
     * Note that when the {@code jobId} is {@link Metadata#ALL} all jobs are returned.
     *
     * @param expression   the jobId or an expression matching jobIds
     * @param allowNoMatch if {@code false}, an error is thrown when no job matches the {@code jobId}
     * @param jobsListener The jobs listener
     */
    public void expandJobs(String expression, boolean allowNoMatch, ActionListener<QueryPage<Job>> jobsListener) {
        expandJobBuilders(
            expression,
            allowNoMatch,
            null,
            ActionListener.wrap(
                jobBuilders -> jobsListener.onResponse(
                    new QueryPage<>(
                        jobBuilders.stream().map(Job.Builder::build).collect(Collectors.toList()),
                        jobBuilders.size(),
                        Job.RESULTS_FIELD
                    )
                ),
                jobsListener::onFailure
            )
        );
    }

    /**
     * Validate the char filter/tokenizer/token filter names used in the categorization analyzer config (if any).
     * If the user has not provided a categorization analyzer then set the standard one if categorization is
     * being used at all and all the nodes in the cluster are running a version that will understand it.  This
     * method must only be called when a job is first created - since it applies a default if it were to be
     * called after that it could change the meaning of a job that has already run.  The validation in this
     * method has to be done server-side; it cannot be done in a client as that won't have loaded the appropriate
     * analysis modules/plugins. (The overall structure can be validated at parse time, but the exact names need
     * to be checked separately, as plugins that provide the functionality can be installed/uninstalled.)
     */
    static void validateCategorizationAnalyzerOrSetDefault(
        Job.Builder jobBuilder,
        AnalysisRegistry analysisRegistry,
        Version minNodeVersion
    ) throws IOException {
        AnalysisConfig analysisConfig = jobBuilder.getAnalysisConfig();
        CategorizationAnalyzerConfig categorizationAnalyzerConfig = analysisConfig.getCategorizationAnalyzerConfig();
        if (categorizationAnalyzerConfig != null) {
            CategorizationAnalyzer.verifyConfigBuilder(
                new CategorizationAnalyzerConfig.Builder(categorizationAnalyzerConfig),
                analysisRegistry
            );
        } else if (analysisConfig.getCategorizationFieldName() != null
            && minNodeVersion.onOrAfter(MIN_NODE_VERSION_FOR_STANDARD_CATEGORIZATION_ANALYZER)) {
                // Any supplied categorization filters are transferred into the new categorization analyzer.
                // The user supplied categorization filters will already have been validated when the put job
                // request was built, so we know they're valid.
                AnalysisConfig.Builder analysisConfigBuilder = new AnalysisConfig.Builder(analysisConfig).setCategorizationAnalyzerConfig(
                    CategorizationAnalyzerConfig.buildStandardCategorizationAnalyzer(analysisConfig.getCategorizationFilters())
                ).setCategorizationFilters(null);
                jobBuilder.setAnalysisConfig(analysisConfigBuilder);
            }
    }

    /**
     * Stores the anomaly job configuration
     */
    public void putJob(
        PutJobAction.Request request,
        AnalysisRegistry analysisRegistry,
        ClusterState state,
        ActionListener<PutJobAction.Response> actionListener
    ) throws IOException {

        Version minNodeVersion = state.getNodes().getMinNodeVersion();

        Job.Builder jobBuilder = request.getJobBuilder();
        jobBuilder.validateAnalysisLimitsAndSetDefaults(maxModelMemoryLimitSupplier.get());
        jobBuilder.validateModelSnapshotRetentionSettingsAndSetDefaults();
        validateCategorizationAnalyzerOrSetDefault(jobBuilder, analysisRegistry, minNodeVersion);

        Job job = jobBuilder.build(new Date());

        ActionListener<Boolean> putJobListener = new ActionListener<>() {
            @Override
            public void onResponse(Boolean mappingsUpdated) {

                jobConfigProvider.putJob(job, ActionListener.wrap(response -> {
                    auditor.info(job.getId(), Messages.getMessage(Messages.JOB_AUDIT_CREATED));
                    actionListener.onResponse(new PutJobAction.Response(job));
                }, actionListener::onFailure));
            }

            @Override
            public void onFailure(Exception e) {
                if (ExceptionsHelper.unwrapCause(e) instanceof IllegalArgumentException) {
                    // the underlying error differs depending on which way around the clashing fields are seen
                    Matcher matcher = Pattern.compile("can't merge a non object mapping \\[(.*)\\] with an object mapping")
                        .matcher(e.getMessage());
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
            indicesCreated -> ElasticsearchMappings.addDocMappingIfMissing(
                MlConfigIndex.indexName(),
                MlConfigIndex::mapping,
                client,
                state,
                request.masterNodeTimeout(),
                putJobListener
            ),
            putJobListener::onFailure
        );

        ActionListener<List<String>> checkForLeftOverDocs = ActionListener.wrap(matchedIds -> {
            if (matchedIds.isEmpty()) {
                if (job.getDatafeedConfig().isPresent()) {
                    try {
                        DatafeedJobValidator.validate(job.getDatafeedConfig().get(), job, xContentRegistry);
                    } catch (Exception e) {
                        actionListener.onFailure(e);
                        return;
                    }
                }
                jobResultsProvider.createJobResultIndex(job, state, addDocMappingsListener);
            } else {
                // A job has the same Id as one of the group names
                // error with the first in the list
                actionListener.onFailure(
                    new ResourceAlreadyExistsException(Messages.getMessage(Messages.JOB_AND_GROUP_NAMES_MUST_BE_UNIQUE, matchedIds.get(0)))
                );
            }
        }, actionListener::onFailure);

        ActionListener<Boolean> checkNoJobsWithGroupId = ActionListener.wrap(groupExists -> {
            if (groupExists) {
                actionListener.onFailure(
                    new ResourceAlreadyExistsException(Messages.getMessage(Messages.JOB_AND_GROUP_NAMES_MUST_BE_UNIQUE, job.getId()))
                );
                return;
            }
            if (job.getGroups().isEmpty()) {
                checkForLeftOverDocs.onResponse(Collections.emptyList());
            } else {
                jobConfigProvider.jobIdMatches(job.getGroups(), checkForLeftOverDocs);
            }
        }, actionListener::onFailure);

        ActionListener<Boolean> checkNoGroupWithTheJobId = ActionListener.wrap(
            ok -> jobConfigProvider.groupExists(job.getId(), checkNoJobsWithGroupId),
            actionListener::onFailure
        );

        jobConfigProvider.jobExists(job.getId(), false, null, ActionListener.wrap(jobExists -> {
            if (jobExists) {
                actionListener.onFailure(ExceptionsHelper.jobAlreadyExists(job.getId()));
            } else {
                jobResultsProvider.checkForLeftOverDocuments(job, checkNoGroupWithTheJobId);
            }
        }, actionListener::onFailure));
    }

    public void updateJob(UpdateJobAction.Request request, ActionListener<PutJobAction.Response> actionListener) {

        Runnable doUpdate = () -> jobConfigProvider.updateJobWithValidation(
            request.getJobId(),
            request.getJobUpdate(),
            maxModelMemoryLimitSupplier.get(),
            this::validate,
            ActionListener.wrap(updatedJob -> postJobUpdate(request, updatedJob, actionListener), actionListener::onFailure)
        );

        // Obviously if we're updating a job it's impossible that the config index has no mappings at
        // all, but if we rewrite the job config we may add new fields that require the latest mappings
        Runnable checkMappingsAreUpToDate = () -> ElasticsearchMappings.addDocMappingIfMissing(
            MlConfigIndex.indexName(),
            MlConfigIndex::mapping,
            client,
            clusterService.state(),
            request.masterNodeTimeout(),
            ActionListener.wrap(bool -> doUpdate.run(), actionListener::onFailure)
        );

        if (request.getJobUpdate().getGroups() != null && request.getJobUpdate().getGroups().isEmpty() == false) {

            // check the new groups are not job Ids
            jobConfigProvider.jobIdMatches(request.getJobUpdate().getGroups(), ActionListener.wrap(matchingIds -> {
                if (matchingIds.isEmpty()) {
                    checkMappingsAreUpToDate.run();
                } else {
                    actionListener.onFailure(
                        new ResourceAlreadyExistsException(
                            Messages.getMessage(Messages.JOB_AND_GROUP_NAMES_MUST_BE_UNIQUE, matchingIds.get(0))
                        )
                    );
                }
            }, actionListener::onFailure));
        } else {
            checkMappingsAreUpToDate.run();
        }
    }

    public void deleteJob(DeleteJobAction.Request request, ClusterState state, ActionListener<AcknowledgedResponse> listener) {
        deleteJob(request, client, state, listener);
    }

    public void deleteJob(
        DeleteJobAction.Request request,
        Client clientToUse,
        ClusterState state,
        ActionListener<AcknowledgedResponse> listener
    ) {
        final String jobId = request.getJobId();

        // Step 5. When the job has been removed from the config index, return a response
        // -------
        CheckedConsumer<Boolean, Exception> configResponseHandler = jobDeleted -> {
            if (jobDeleted) {
                logger.info("Job [" + jobId + "] deleted");
                auditor.info(jobId, Messages.getMessage(Messages.JOB_AUDIT_DELETED));
                listener.onResponse(AcknowledgedResponse.TRUE);
            } else {
                listener.onResponse(AcknowledgedResponse.FALSE);
            }
        };

        // Step 4. When the physical storage has been deleted, delete the job config document
        // -------
        // Don't report an error if the document has already been deleted
        CheckedConsumer<Boolean, Exception> removeFromCalendarsHandler = response -> jobConfigProvider.deleteJob(
            jobId,
            false,
            ActionListener.wrap(deleteResponse -> configResponseHandler.accept(Boolean.TRUE), listener::onFailure)
        );

        // Step 3. Remove the job from any calendars
        CheckedConsumer<Boolean, Exception> deleteJobStateHandler = response -> jobResultsProvider.removeJobFromCalendars(
            jobId,
            ActionListener.wrap(removeFromCalendarsHandler, listener::onFailure)
        );

        // Step 2. Delete the physical storage
        ActionListener<CancelJobModelSnapshotUpgradeAction.Response> cancelUpgradesListener = ActionListener.wrap(
            r -> new JobDataDeleter(clientToUse, jobId, request.getDeleteUserAnnotations()).deleteJobDocuments(
                jobConfigProvider,
                indexNameExpressionResolver,
                state,
                deleteJobStateHandler,
                listener::onFailure
            ),
            listener::onFailure
        );

        // Step 1. Cancel any model snapshot upgrades that might be in progress
        clientToUse.execute(
            CancelJobModelSnapshotUpgradeAction.INSTANCE,
            new CancelJobModelSnapshotUpgradeAction.Request(jobId, "_all"),
            cancelUpgradesListener
        );
    }

    private void postJobUpdate(UpdateJobAction.Request request, Job updatedJob, ActionListener<PutJobAction.Response> actionListener) {
        // Autodetect must be updated if the fields that the C++ uses are changed
        JobUpdate jobUpdate = request.getJobUpdate();
        if (jobUpdate.isAutodetectProcessUpdate()) {
            if (isJobOpen(clusterService.state(), request.getJobId())) {
                updateJobProcessNotifier.submitJobUpdate(UpdateParams.fromJobUpdate(jobUpdate), ActionListener.wrap(isUpdated -> {
                    if (isUpdated) {
                        auditJobUpdatedIfNotInternal(request);
                    } else {
                        logger.error("[{}] Updating autodetect failed for job update [{}]", jobUpdate.getJobId(), jobUpdate);
                    }
                },
                    e -> logger.error(
                        () -> format(
                            "[%s] Updating autodetect failed with an exception, job update [%s] ",
                            jobUpdate.getJobId(),
                            jobUpdate
                        ),
                        e
                    )
                ));
            }
        } else {
            logger.debug("[{}] No process update required for job update: {}", jobUpdate::getJobId, jobUpdate::toString);
            auditJobUpdatedIfNotInternal(request);
        }

        actionListener.onResponse(new PutJobAction.Response(updatedJob));
    }

    private void validate(Job job, JobUpdate jobUpdate, ActionListener<Void> handler) {
        VoidChainTaskExecutor voidChainTaskExecutor = new VoidChainTaskExecutor(
            client.threadPool().executor(MachineLearning.UTILITY_THREAD_POOL_NAME),
            true
        );
        validateModelSnapshotIdUpdate(job, jobUpdate.getModelSnapshotId(), voidChainTaskExecutor);
        validateAnalysisLimitsUpdate(job, jobUpdate.getAnalysisLimits(), voidChainTaskExecutor);
        voidChainTaskExecutor.execute(ActionListener.wrap(aVoids -> handler.onResponse(null), handler::onFailure));
    }

    private void validateModelSnapshotIdUpdate(Job job, String modelSnapshotId, VoidChainTaskExecutor voidChainTaskExecutor) {
        if (modelSnapshotId != null && ModelSnapshot.isTheEmptySnapshot(modelSnapshotId) == false) {
            voidChainTaskExecutor.add(listener -> jobResultsProvider.getModelSnapshot(job.getId(), modelSnapshotId, newModelSnapshot -> {
                if (newModelSnapshot == null) {
                    String message = Messages.getMessage(Messages.REST_NO_SUCH_MODEL_SNAPSHOT, modelSnapshotId, job.getId());
                    listener.onFailure(new ResourceNotFoundException(message));
                    return;
                }
                jobResultsProvider.getModelSnapshot(job.getId(), job.getModelSnapshotId(), oldModelSnapshot -> {
                    if (oldModelSnapshot != null && newModelSnapshot.result.getTimestamp().before(oldModelSnapshot.result.getTimestamp())) {
                        String message = "Job ["
                            + job.getId()
                            + "] has a more recent model snapshot ["
                            + oldModelSnapshot.result.getSnapshotId()
                            + "]";
                        listener.onFailure(new IllegalArgumentException(message));
                    }
                    listener.onResponse(null);
                }, listener::onFailure);
            }, listener::onFailure));
        }
    }

    private void validateAnalysisLimitsUpdate(Job job, AnalysisLimits newLimits, VoidChainTaskExecutor voidChainTaskExecutor) {
        if (newLimits == null || newLimits.getModelMemoryLimit() == null) {
            return;
        }
        Long newModelMemoryLimit = newLimits.getModelMemoryLimit();
        voidChainTaskExecutor.add(listener -> {
            if (isJobOpen(clusterService.state(), job.getId())) {
                listener.onFailure(
                    ExceptionsHelper.badRequestException(
                        "Cannot update " + Job.ANALYSIS_LIMITS.getPreferredName() + " while the job is open"
                    )
                );
                return;
            }
            jobResultsProvider.modelSizeStats(job.getId(), modelSizeStats -> {
                if (modelSizeStats != null) {
                    ByteSizeValue modelSize = ByteSizeValue.ofBytes(modelSizeStats.getModelBytes());
                    if (newModelMemoryLimit < modelSize.getMb()) {
                        listener.onFailure(
                            ExceptionsHelper.badRequestException(
                                Messages.getMessage(
                                    Messages.JOB_CONFIG_UPDATE_ANALYSIS_LIMITS_MODEL_MEMORY_LIMIT_CANNOT_BE_DECREASED,
                                    ByteSizeValue.ofMb(modelSize.getMb()),
                                    ByteSizeValue.ofMb(newModelMemoryLimit)
                                )
                            )
                        );
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

    private static boolean isJobOpen(ClusterState clusterState, String jobId) {
        PersistentTasksCustomMetadata persistentTasks = clusterState.metadata().custom(PersistentTasksCustomMetadata.TYPE);
        JobState jobState = MlTasks.getJobState(jobId, persistentTasks);
        return jobState == JobState.OPENED;
    }

    private static Set<String> openJobIds(ClusterState clusterState) {
        PersistentTasksCustomMetadata persistentTasks = clusterState.metadata().custom(PersistentTasksCustomMetadata.TYPE);
        return MlTasks.openJobIds(persistentTasks);
    }

    public void notifyFilterChanged(
        MlFilter filter,
        Set<String> addedItems,
        Set<String> removedItems,
        ActionListener<Boolean> updatedListener
    ) {
        if (addedItems.isEmpty() && removedItems.isEmpty()) {
            updatedListener.onResponse(Boolean.TRUE);
            return;
        }

        jobConfigProvider.findJobsWithCustomRules(
            ActionListener.wrap(jobBuilders -> threadPool.executor(MachineLearning.UTILITY_THREAD_POOL_NAME).execute(() -> {
                for (Job job : jobBuilders) {
                    Set<String> jobFilters = job.getAnalysisConfig().extractReferencedFilters();
                    ClusterState clusterState = clusterService.state();
                    if (jobFilters.contains(filter.getId())) {
                        if (isJobOpen(clusterState, job.getId())) {
                            updateJobProcessNotifier.submitJobUpdate(
                                UpdateParams.filterUpdate(job.getId(), filter),
                                ActionListener.wrap(
                                    isUpdated -> auditFilterChanges(job.getId(), filter.getId(), addedItems, removedItems),
                                    e -> {}
                                )
                            );
                        } else {
                            auditFilterChanges(job.getId(), filter.getId(), addedItems, removedItems);
                        }
                    }
                }

                updatedListener.onResponse(Boolean.TRUE);
            }), updatedListener::onFailure)
        );
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

        boolean appliesToAllJobs = calendarJobIds.stream().anyMatch(Metadata.ALL::equals);
        if (appliesToAllJobs) {
            submitJobEventUpdate(openJobIds, updateListener);
            return;
        }

        // calendarJobIds may be a group or job
        jobConfigProvider.expandGroupIds(
            calendarJobIds,
            ActionListener.wrap(expandedIds -> threadPool.executor(MachineLearning.UTILITY_THREAD_POOL_NAME).execute(() -> {
                // Merge the expanded group members with the request Ids.
                // Ids that aren't jobs will be filtered by isJobOpen()
                expandedIds.addAll(calendarJobIds);

                openJobIds.retainAll(expandedIds);
                submitJobEventUpdate(openJobIds, updateListener);
            }), updateListener::onFailure)
        );
    }

    private void submitJobEventUpdate(Collection<String> jobIds, ActionListener<Boolean> updateListener) {
        for (String jobId : jobIds) {
            updateJobProcessNotifier.submitJobUpdate(
                UpdateParams.scheduledEventsUpdate(jobId),
                ActionListener.wrap(
                    isUpdated -> auditor.info(jobId, Messages.getMessage(Messages.JOB_AUDIT_CALENDARS_UPDATED_ON_PROCESS)),
                    e -> logger.error("[" + jobId + "] failed submitting process update on calendar change", e)
                )
            );
        }

        updateListener.onResponse(Boolean.TRUE);
    }

    public void revertSnapshot(
        RevertModelSnapshotAction.Request request,
        ActionListener<RevertModelSnapshotAction.Response> actionListener,
        ModelSnapshot modelSnapshot
    ) {

        final ModelSizeStats modelSizeStats = modelSnapshot.getModelSizeStats();

        // Step 3. After the model size stats is persisted, also persist the snapshot's quantiles and respond
        // -------
        CheckedConsumer<IndexResponse, Exception> modelSizeStatsResponseHandler = response -> {
            // In case we are reverting to the empty snapshot the quantiles will be null
            if (modelSnapshot.getQuantiles() == null) {
                actionListener.onResponse(new RevertModelSnapshotAction.Response(modelSnapshot));
                return;
            }
            jobResultsPersister.persistQuantiles(
                modelSnapshot.getQuantiles(),
                WriteRequest.RefreshPolicy.IMMEDIATE,
                ActionListener.wrap(quantilesResponse -> {
                    // The quantiles can be large, and totally dominate the output -
                    // it's clearer to remove them as they are not necessary for the revert op
                    ModelSnapshot snapshotWithoutQuantiles = new ModelSnapshot.Builder(modelSnapshot).setQuantiles(null).build();
                    actionListener.onResponse(new RevertModelSnapshotAction.Response(snapshotWithoutQuantiles));
                }, actionListener::onFailure)
            );
        };

        // Step 2. When the model_snapshot_id is updated on the job, persist the snapshot's model size stats with a touched log time
        // so that a search for the latest model size stats returns the reverted one.
        // -------
        CheckedConsumer<Boolean, Exception> updateHandler = response -> {
            if (response) {
                ModelSizeStats revertedModelSizeStats = new ModelSizeStats.Builder(modelSizeStats).setLogTime(new Date()).build();
                jobResultsPersister.persistModelSizeStatsWithoutRetries(
                    revertedModelSizeStats,
                    WriteRequest.RefreshPolicy.IMMEDIATE,
                    ActionListener.wrap(modelSizeStatsResponseHandler, actionListener::onFailure)
                );
            }
        };

        // Step 1. update the job
        // -------
        JobUpdate update = new JobUpdate.Builder(request.getJobId()).setModelSnapshotId(modelSnapshot.getSnapshotId()).build();

        jobConfigProvider.updateJob(request.getJobId(), update, maxModelMemoryLimitSupplier.get(), ActionListener.wrap(job -> {
            auditor.info(request.getJobId(), Messages.getMessage(Messages.JOB_AUDIT_REVERTED, modelSnapshot.getDescription()));
            updateHandler.accept(Boolean.TRUE);
        }, actionListener::onFailure));
    }

    public void updateJobBlockReason(String jobId, Blocked blocked, ActionListener<PutJobAction.Response> listener) {
        jobConfigProvider.updateJobBlockReason(jobId, blocked, listener);
    }
}
