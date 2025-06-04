/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.action;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.exception.ElasticsearchStatusException;
import org.elasticsearch.exception.ResourceAlreadyExistsException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.TransportSearchAction;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.ParentTaskAssigningClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.logging.HeaderWarning;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.license.License;
import org.elasticsearch.license.LicenseUtils;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.persistent.AllocatedPersistentTask;
import org.elasticsearch.persistent.PersistentTaskParams;
import org.elasticsearch.persistent.PersistentTaskState;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.persistent.PersistentTasksService;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.XPackField;
import org.elasticsearch.xpack.core.common.validation.SourceDestValidator;
import org.elasticsearch.xpack.core.ml.MachineLearningField;
import org.elasticsearch.xpack.core.ml.MlConfigIndex;
import org.elasticsearch.xpack.core.ml.MlConfigVersion;
import org.elasticsearch.xpack.core.ml.MlStatsIndex;
import org.elasticsearch.xpack.core.ml.MlTasks;
import org.elasticsearch.xpack.core.ml.action.ExplainDataFrameAnalyticsAction;
import org.elasticsearch.xpack.core.ml.action.GetDataFrameAnalyticsStatsAction;
import org.elasticsearch.xpack.core.ml.action.NodeAcknowledgedResponse;
import org.elasticsearch.xpack.core.ml.action.StartDataFrameAnalyticsAction;
import org.elasticsearch.xpack.core.ml.action.StartDataFrameAnalyticsAction.TaskParams;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsConfig;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsState;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsTaskState;
import org.elasticsearch.xpack.core.ml.dataframe.analyses.RequiredField;
import org.elasticsearch.xpack.core.ml.inference.persistence.InferenceIndexConstants;
import org.elasticsearch.xpack.core.ml.job.messages.Messages;
import org.elasticsearch.xpack.core.ml.job.persistence.AnomalyDetectorsIndex;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.core.ml.utils.MlIndexAndAlias;
import org.elasticsearch.xpack.core.ml.utils.PhaseProgress;
import org.elasticsearch.xpack.ml.MachineLearning;
import org.elasticsearch.xpack.ml.dataframe.DataFrameAnalyticsManager;
import org.elasticsearch.xpack.ml.dataframe.DataFrameAnalyticsTask;
import org.elasticsearch.xpack.ml.dataframe.MappingsMerger;
import org.elasticsearch.xpack.ml.dataframe.SourceDestValidations;
import org.elasticsearch.xpack.ml.dataframe.extractor.DataFrameDataExtractorFactory;
import org.elasticsearch.xpack.ml.dataframe.extractor.ExtractedFieldsDetectorFactory;
import org.elasticsearch.xpack.ml.dataframe.persistence.DataFrameAnalyticsConfigProvider;
import org.elasticsearch.xpack.ml.dataframe.stats.StatsHolder;
import org.elasticsearch.xpack.ml.extractor.ExtractedFields;
import org.elasticsearch.xpack.ml.job.JobNodeSelector;
import org.elasticsearch.xpack.ml.notifications.DataFrameAnalyticsAuditor;
import org.elasticsearch.xpack.ml.process.MlMemoryTracker;
import org.elasticsearch.xpack.ml.task.AbstractJobPersistentTasksExecutor;

import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static org.elasticsearch.core.Strings.format;
import static org.elasticsearch.xpack.core.ClientHelper.ML_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.executeAsyncWithOrigin;

/**
 * Starts the persistent task for running data frame analytics.
 */
public class TransportStartDataFrameAnalyticsAction extends TransportMasterNodeAction<
    StartDataFrameAnalyticsAction.Request,
    NodeAcknowledgedResponse> {

    private static final Logger logger = LogManager.getLogger(TransportStartDataFrameAnalyticsAction.class);
    private static final String PRIMARY_SHARDS_INACTIVE = "not all primary shards are active";

    private final XPackLicenseState licenseState;
    private final Client client;
    private final PersistentTasksService persistentTasksService;
    private final DataFrameAnalyticsConfigProvider configProvider;
    private final MlMemoryTracker memoryTracker;
    private final DataFrameAnalyticsAuditor auditor;
    private final SourceDestValidator sourceDestValidator;

    @Inject
    public TransportStartDataFrameAnalyticsAction(
        TransportService transportService,
        Client client,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        XPackLicenseState licenseState,
        IndexNameExpressionResolver indexNameExpressionResolver,
        PersistentTasksService persistentTasksService,
        DataFrameAnalyticsConfigProvider configProvider,
        MlMemoryTracker memoryTracker,
        DataFrameAnalyticsAuditor auditor
    ) {
        super(
            StartDataFrameAnalyticsAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            StartDataFrameAnalyticsAction.Request::new,
            NodeAcknowledgedResponse::new,
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );
        this.licenseState = licenseState;
        this.client = client;
        this.persistentTasksService = persistentTasksService;
        this.configProvider = configProvider;
        this.memoryTracker = memoryTracker;
        this.auditor = Objects.requireNonNull(auditor);

        this.sourceDestValidator = new SourceDestValidator(
            indexNameExpressionResolver,
            transportService.getRemoteClusterService(),
            null,
            null,
            clusterService.getNodeName(),
            License.OperationMode.PLATINUM.description()
        );
    }

    @Override
    protected ClusterBlockException checkBlock(StartDataFrameAnalyticsAction.Request request, ClusterState state) {
        // We only delegate here to PersistentTasksService, but if there is a metadata writeblock,
        // then delegating to PersistentTasksService doesn't make a whole lot of sense,
        // because PersistentTasksService will then fail.
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }

    @Override
    protected void masterOperation(
        Task task,
        StartDataFrameAnalyticsAction.Request request,
        ClusterState state,
        ActionListener<NodeAcknowledgedResponse> listener
    ) {
        logger.debug(() -> "[" + request.getId() + "] received start request");
        if (MachineLearningField.ML_API_FEATURE.check(licenseState) == false) {
            listener.onFailure(LicenseUtils.newComplianceException(XPackField.MACHINE_LEARNING));
            return;
        }

        // Wait for analytics to be started
        ActionListener<PersistentTasksCustomMetadata.PersistentTask<TaskParams>> waitForAnalyticsToStart = new ActionListener<
            PersistentTasksCustomMetadata.PersistentTask<TaskParams>>() {
            @Override
            public void onResponse(PersistentTasksCustomMetadata.PersistentTask<TaskParams> task) {
                waitForAnalyticsStarted(task, request.getTimeout(), listener);
            }

            @Override
            public void onFailure(Exception e) {
                if (ExceptionsHelper.unwrapCause(e) instanceof ResourceAlreadyExistsException) {
                    e = new ElasticsearchStatusException(
                        "Cannot start data frame analytics [{}] because it has already been started",
                        RestStatus.CONFLICT,
                        e,
                        request.getId()
                    );
                }
                listener.onFailure(e);
            }
        };

        // Start persistent task
        ActionListener<StartContext> memoryUsageHandledListener = ActionListener.wrap(startContext -> {
            TaskParams taskParams = new TaskParams(
                request.getId(),
                startContext.config.getVersion(),
                startContext.config.isAllowLazyStart()
            );
            persistentTasksService.sendStartRequest(
                MlTasks.dataFrameAnalyticsTaskId(request.getId()),
                MlTasks.DATA_FRAME_ANALYTICS_TASK_NAME,
                taskParams,
                request.masterNodeTimeout(),
                waitForAnalyticsToStart
            );
        }, listener::onFailure);

        // Perform memory usage estimation for this config
        ActionListener<StartContext> startContextListener = ActionListener.wrap(
            startContext -> estimateMemoryUsageAndUpdateMemoryTracker(startContext, memoryUsageHandledListener),
            listener::onFailure
        );

        // Get start context
        getStartContext(request.getId(), task, startContextListener, request.masterNodeTimeout());
    }

    private void estimateMemoryUsageAndUpdateMemoryTracker(StartContext startContext, ActionListener<StartContext> listener) {
        final String jobId = startContext.config.getId();

        // Tell the job tracker to refresh the memory requirement for this job and all other jobs that have persistent tasks
        ActionListener<ExplainDataFrameAnalyticsAction.Response> explainListener = ActionListener.wrap(explainResponse -> {
            ByteSizeValue expectedMemoryWithoutDisk = explainResponse.getMemoryEstimation().getExpectedMemoryWithoutDisk();
            auditor.info(jobId, Messages.getMessage(Messages.DATA_FRAME_ANALYTICS_AUDIT_ESTIMATED_MEMORY_USAGE, expectedMemoryWithoutDisk));
            // Validate that model memory limit is sufficient to run the analysis
            // We will only warn the caller if the configured limit is too low.
            if (startContext.config.getModelMemoryLimit().compareTo(expectedMemoryWithoutDisk) < 0) {
                String warning = Messages.getMessage(
                    Messages.DATA_FRAME_ANALYTICS_AUDIT_ESTIMATED_MEMORY_USAGE_HIGHER_THAN_CONFIGURED,
                    startContext.config.getModelMemoryLimit(),
                    expectedMemoryWithoutDisk
                );
                auditor.warning(jobId, warning);
                logger.warn("[{}] {}", jobId, warning);
                HeaderWarning.addWarning(warning);
            }
            // Refresh memory requirement for jobs
            memoryTracker.addDataFrameAnalyticsJobMemoryAndRefreshAllOthers(
                jobId,
                startContext.config.getModelMemoryLimit().getBytes(),
                ActionListener.wrap(aVoid -> listener.onResponse(startContext), listener::onFailure)
            );
        }, listener::onFailure);

        ExplainDataFrameAnalyticsAction.Request explainRequest = new ExplainDataFrameAnalyticsAction.Request(startContext.config);
        ClientHelper.executeAsyncWithOrigin(
            client,
            ClientHelper.ML_ORIGIN,
            ExplainDataFrameAnalyticsAction.INSTANCE,
            explainRequest,
            explainListener
        );

    }

    private void getStartContext(String id, Task task, ActionListener<StartContext> finalListener, TimeValue masterTimeout) {

        ParentTaskAssigningClient parentTaskClient = new ParentTaskAssigningClient(client, task.getParentTaskId());

        SubscribableListener

            // Step 1. Get the config
            .<DataFrameAnalyticsConfig>newForked(l -> configProvider.get(id, l))

            // Step 2. Get stats to recover progress
            .<StartContext>andThen((l, config) -> getProgress(config, l.map(progress -> new StartContext(config, progress))))

            // Step 3. Validate source and dest
            .<StartContext>andThen((l, startContext) -> {
                // Validate the query parses
                startContext.config.getSource().getParsedQuery();

                // Validate source/dest are valid
                sourceDestValidator.validate(
                    clusterService.state(),
                    startContext.config.getSource().getIndex(),
                    startContext.config.getDest().getIndex(),
                    null,
                    SourceDestValidations.ALL_VALIDATIONS,
                    l.map(ignored -> startContext)
                );
            })

            // Step 4. Check data extraction is possible
            .<StartContext>andThen(
                (l, startContext) -> new ExtractedFieldsDetectorFactory(parentTaskClient).createFromSource(
                    startContext.config,
                    l.map(extractedFieldsDetector -> {
                        startContext.extractedFields = extractedFieldsDetector.detect().v1();
                        return startContext;
                    })
                )
            )

            // Step 5. Validate dest index is empty if task is starting for first time
            .<StartContext>andThen((l, startContext) -> {
                switch (startContext.startingState) {
                    case FIRST_TIME -> checkDestIndexIsEmptyIfExists(parentTaskClient, startContext, l);
                    case RESUMING_REINDEXING, RESUMING_ANALYZING, RESUMING_INFERENCE -> l.onResponse(startContext);
                    case FINISHED -> {
                        logger.info("[{}] Job has already finished", startContext.config.getId());
                        l.onFailure(ExceptionsHelper.badRequestException("Cannot start because the job has already finished"));
                    }
                    default -> l.onFailure(ExceptionsHelper.serverError("Unexpected starting state {}", startContext.startingState));
                }
            })

            // Step 6. Validate mappings can be merged
            .<StartContext>andThen(
                (l, startContext) -> MappingsMerger.mergeMappings(
                    parentTaskClient,
                    masterTimeout,
                    startContext.config.getHeaders(),
                    startContext.config.getSource(),
                    l.map(ignored -> startContext)
                )
            )

            // Step 7. Validate that there are analyzable data in the source index
            .<StartContext>andThen((l, startContext) -> validateSourceIndexHasAnalyzableData(startContext, l))

            // Step 8. Respond
            .addListener(finalListener);
    }

    private void validateSourceIndexHasAnalyzableData(StartContext startContext, ActionListener<StartContext> listener) {
        ActionListener<Void> validateAtLeastOneAnalyzedFieldListener = ActionListener.wrap(
            aVoid -> validateSourceIndexRowsCount(startContext, listener),
            listener::onFailure
        );

        validateSourceIndexHasAtLeastOneAnalyzedField(startContext, validateAtLeastOneAnalyzedFieldListener);
    }

    private static void validateSourceIndexHasAtLeastOneAnalyzedField(StartContext startContext, ActionListener<Void> listener) {
        Set<String> requiredFields = startContext.config.getAnalysis()
            .getRequiredFields()
            .stream()
            .map(RequiredField::getName)
            .collect(Collectors.toSet());

        // We assume here that required fields are not features
        long nonRequiredFieldsCount = startContext.extractedFields.getAllFields()
            .stream()
            .filter(extractedField -> requiredFields.contains(extractedField.getName()) == false)
            .count();
        if (nonRequiredFieldsCount == 0) {
            StringBuilder msgBuilder = new StringBuilder("at least one field must be included in the analysis");
            if (requiredFields.isEmpty() == false) {
                msgBuilder.append(" (excluding fields ").append(requiredFields).append(")");
            }
            listener.onFailure(ExceptionsHelper.badRequestException(msgBuilder.toString()));
        } else {
            listener.onResponse(null);
        }
    }

    private void validateSourceIndexRowsCount(StartContext startContext, ActionListener<StartContext> listener) {
        DataFrameDataExtractorFactory extractorFactory = DataFrameDataExtractorFactory.createForSourceIndices(
            client,
            "validate_source_index_has_rows-" + startContext.config.getId(),
            startContext.config,
            startContext.extractedFields
        );
        extractorFactory.newExtractor(false).collectDataSummaryAsync(ActionListener.wrap(dataSummary -> {
            if (dataSummary.rows == 0) {
                listener.onFailure(
                    ExceptionsHelper.badRequestException(
                        "Unable to start {} as no documents in the source indices [{}] contained all the fields "
                            + "selected for analysis. If you are relying on automatic field selection then there are "
                            + "currently mapped fields that do not exist in any indexed documents, and you will have "
                            + "to switch to explicit field selection and include only fields that exist in indexed "
                            + "documents.",
                        startContext.config.getId(),
                        Strings.arrayToCommaDelimitedString(startContext.config.getSource().getIndex())
                    )
                );
            } else if (Math.floor(startContext.config.getAnalysis().getTrainingPercent() * dataSummary.rows) >= Math.pow(2, 32)) {
                listener.onFailure(
                    ExceptionsHelper.badRequestException(
                        "Unable to start because too many documents "
                            + "(more than 2^32) are included in the analysis. Consider downsampling."
                    )
                );
            } else {
                listener.onResponse(startContext);
            }
        }, listener::onFailure));
    }

    private void getProgress(DataFrameAnalyticsConfig config, ActionListener<List<PhaseProgress>> listener) {
        GetDataFrameAnalyticsStatsAction.Request getStatsRequest = new GetDataFrameAnalyticsStatsAction.Request(config.getId());
        executeAsyncWithOrigin(
            client,
            ML_ORIGIN,
            GetDataFrameAnalyticsStatsAction.INSTANCE,
            getStatsRequest,
            ActionListener.wrap(statsResponse -> {
                List<GetDataFrameAnalyticsStatsAction.Response.Stats> stats = statsResponse.getResponse().results();
                if (stats.isEmpty()) {
                    // The job has been deleted in between
                    listener.onFailure(ExceptionsHelper.missingDataFrameAnalytics(config.getId()));
                } else {
                    listener.onResponse(stats.get(0).getProgress());
                }
            }, listener::onFailure)
        );
    }

    private static void checkDestIndexIsEmptyIfExists(
        ParentTaskAssigningClient parentTaskClient,
        StartContext startContext,
        ActionListener<StartContext> listener
    ) {
        String destIndex = startContext.config.getDest().getIndex();
        SearchRequest destEmptySearch = new SearchRequest(destIndex);
        destEmptySearch.source().size(0);
        destEmptySearch.allowPartialSearchResults(false);
        ClientHelper.executeWithHeadersAsync(
            startContext.config.getHeaders(),
            ClientHelper.ML_ORIGIN,
            parentTaskClient,
            TransportSearchAction.TYPE,
            destEmptySearch,
            ActionListener.wrap(searchResponse -> {
                if (searchResponse.getHits().getTotalHits().value() > 0) {
                    listener.onFailure(ExceptionsHelper.badRequestException("dest index [{}] must be empty", destIndex));
                } else {
                    listener.onResponse(startContext);
                }
            }, e -> {
                if (ExceptionsHelper.unwrapCause(e) instanceof IndexNotFoundException) {
                    listener.onResponse(startContext);
                } else {
                    listener.onFailure(e);
                }
            })
        );
    }

    private void waitForAnalyticsStarted(
        PersistentTasksCustomMetadata.PersistentTask<TaskParams> task,
        TimeValue timeout,
        ActionListener<NodeAcknowledgedResponse> listener
    ) {
        AnalyticsPredicate predicate = new AnalyticsPredicate();
        persistentTasksService.waitForPersistentTaskCondition(
            task.getId(),
            predicate,
            timeout,

            new PersistentTasksService.WaitForPersistentTaskListener<PersistentTaskParams>() {

                @Override
                public void onResponse(PersistentTasksCustomMetadata.PersistentTask<PersistentTaskParams> persistentTask) {
                    if (predicate.exception != null) {
                        // We want to return to the caller without leaving an unassigned persistent task, to match
                        // what would have happened if the error had been detected in the "fast fail" validation
                        cancelAnalyticsStart(task, predicate.exception, listener);
                    } else {
                        auditor.info(task.getParams().getId(), Messages.DATA_FRAME_ANALYTICS_AUDIT_STARTED);
                        listener.onResponse(new NodeAcknowledgedResponse(true, predicate.node));
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    listener.onFailure(e);
                }

                @Override
                public void onTimeout(TimeValue timeout) {
                    logger.error(
                        () -> format(
                            "[%s] timed out when starting task after [%s]. Assignment explanation [%s]",
                            task.getParams().getId(),
                            timeout,
                            predicate.assignmentExplanation
                        )
                    );
                    if (predicate.assignmentExplanation != null) {
                        cancelAnalyticsStart(
                            task,
                            new ElasticsearchStatusException(
                                "Could not start data frame analytics task, timed out after [{}] waiting for task assignment. "
                                    + "Assignment explanation [{}]",
                                RestStatus.TOO_MANY_REQUESTS,
                                timeout,
                                predicate.assignmentExplanation
                            ),
                            listener
                        );
                    } else {
                        listener.onFailure(
                            new ElasticsearchStatusException(
                                "Starting data frame analytics [{}] timed out after [{}]",
                                RestStatus.REQUEST_TIMEOUT,
                                task.getParams().getId(),
                                timeout
                            )
                        );
                    }
                }
            }
        );
    }

    private static class StartContext {
        private final DataFrameAnalyticsConfig config;
        private final DataFrameAnalyticsTask.StartingState startingState;
        private volatile ExtractedFields extractedFields;

        private StartContext(DataFrameAnalyticsConfig config, List<PhaseProgress> progressOnStart) {
            this.config = config;
            this.startingState = DataFrameAnalyticsTask.determineStartingState(config.getId(), progressOnStart);
        }
    }

    /**
     * Important: the methods of this class must NOT throw exceptions.  If they did then the callers
     * of endpoints waiting for a condition tested by this predicate would never get a response.
     */
    private static class AnalyticsPredicate implements Predicate<PersistentTasksCustomMetadata.PersistentTask<?>> {

        private volatile Exception exception;
        private volatile String node = "";
        private volatile String assignmentExplanation;

        @Override
        public boolean test(PersistentTasksCustomMetadata.PersistentTask<?> persistentTask) {
            if (persistentTask == null) {
                return false;
            }

            PersistentTasksCustomMetadata.Assignment assignment = persistentTask.getAssignment();

            // This means we are awaiting a new node to be spun up, ok to return back to the user to await node creation
            if (assignment != null && assignment.equals(JobNodeSelector.AWAITING_LAZY_ASSIGNMENT)) {
                return true;
            }
            String reason = "__unknown__";

            if (assignment != null
                && assignment.equals(PersistentTasksCustomMetadata.INITIAL_ASSIGNMENT) == false
                && assignment.isAssigned() == false) {
                assignmentExplanation = assignment.getExplanation();
                // Assignment failed due to primary shard check.
                // This is hopefully intermittent and we should allow another assignment attempt.
                if (assignmentExplanation.contains(PRIMARY_SHARDS_INACTIVE)) {
                    return false;
                }
                exception = new ElasticsearchStatusException(
                    "Could not start data frame analytics task, allocation explanation [{}]",
                    RestStatus.TOO_MANY_REQUESTS,
                    assignment.getExplanation()
                );
                return true;
            }
            DataFrameAnalyticsTaskState taskState = (DataFrameAnalyticsTaskState) persistentTask.getState();
            reason = taskState != null ? taskState.getReason() : reason;
            DataFrameAnalyticsState analyticsState = taskState == null ? DataFrameAnalyticsState.STOPPED : taskState.getState();
            switch (analyticsState) {
                case STARTED:
                    node = persistentTask.getExecutorNode();
                    return true;
                case STOPPING:
                    exception = ExceptionsHelper.conflictStatusException("the task has been stopped while waiting to be started");
                    return true;
                // The STARTING case here is expected to be incredibly short-lived, just occurring during the
                // time period when a job has successfully been assigned to a node but the request to update
                // its task state is still in-flight. (The long-lived STARTING case when a lazy node needs to
                // be added to the cluster to accommodate the job was dealt with higher up this method when the
                // magic AWAITING_LAZY_ASSIGNMENT assignment was checked for.)
                case STARTING:
                case STOPPED:
                    return false;
                case FAILED:
                default:
                    exception = ExceptionsHelper.serverError(
                        "Unexpected task state [{}] {}while waiting to be started",
                        analyticsState,
                        reason == null ? "" : "with reason [" + reason + "] "
                    );
                    return true;
            }
        }
    }

    private void cancelAnalyticsStart(
        PersistentTasksCustomMetadata.PersistentTask<TaskParams> persistentTask,
        Exception exception,
        ActionListener<NodeAcknowledgedResponse> listener
    ) {
        persistentTasksService.sendRemoveRequest(
            persistentTask.getId(),
            MachineLearning.HARD_CODED_MACHINE_LEARNING_MASTER_NODE_TIMEOUT,
            new ActionListener<>() {
                @Override
                public void onResponse(PersistentTasksCustomMetadata.PersistentTask<?> task) {
                    // We succeeded in cancelling the persistent task, but the
                    // problem that caused us to cancel it is the overall result
                    listener.onFailure(exception);
                }

                @Override
                public void onFailure(Exception e) {
                    logger.error(
                        () -> format(
                            "[%s] Failed to cancel persistent task that could not be assigned due to [%s]",
                            persistentTask.getParams().getId(),
                            exception.getMessage()
                        ),
                        e
                    );
                    listener.onFailure(exception);
                }
            }
        );
    }

    public static class TaskExecutor extends AbstractJobPersistentTasksExecutor<TaskParams> {

        private final Client client;
        private final DataFrameAnalyticsManager manager;
        private final DataFrameAnalyticsAuditor auditor;
        private final XPackLicenseState licenseState;

        private volatile ClusterState clusterState;

        public TaskExecutor(
            Settings settings,
            Client client,
            ClusterService clusterService,
            DataFrameAnalyticsManager manager,
            DataFrameAnalyticsAuditor auditor,
            MlMemoryTracker memoryTracker,
            IndexNameExpressionResolver resolver,
            XPackLicenseState licenseState
        ) {
            super(
                MlTasks.DATA_FRAME_ANALYTICS_TASK_NAME,
                MachineLearning.UTILITY_THREAD_POOL_NAME,
                settings,
                clusterService,
                memoryTracker,
                resolver
            );
            this.client = Objects.requireNonNull(client);
            this.manager = Objects.requireNonNull(manager);
            this.auditor = Objects.requireNonNull(auditor);
            this.licenseState = licenseState;
            clusterService.addListener(event -> clusterState = event.state());
        }

        @Override
        protected AllocatedPersistentTask createTask(
            long id,
            String type,
            String action,
            TaskId parentTaskId,
            PersistentTasksCustomMetadata.PersistentTask<TaskParams> persistentTask,
            Map<String, String> headers
        ) {
            return new DataFrameAnalyticsTask(
                id,
                type,
                action,
                parentTaskId,
                headers,
                client,
                manager,
                auditor,
                persistentTask.getParams(),
                licenseState
            );
        }

        @Override
        public PersistentTasksCustomMetadata.Assignment getAssignment(
            TaskParams params,
            Collection<DiscoveryNode> candidateNodes,
            @SuppressWarnings("HiddenField") ClusterState clusterState
        ) {
            boolean isMemoryTrackerRecentlyRefreshed = memoryTracker.isRecentlyRefreshed();
            Optional<PersistentTasksCustomMetadata.Assignment> optionalAssignment = getPotentialAssignment(
                params,
                clusterState,
                isMemoryTrackerRecentlyRefreshed
            );
            // NOTE: this will return here if isMemoryTrackerRecentlyRefreshed is false, we don't allow assignment with stale memory
            if (optionalAssignment.isPresent()) {
                return optionalAssignment.get();
            }
            JobNodeSelector jobNodeSelector = new JobNodeSelector(
                clusterState,
                candidateNodes,
                params.getId(),
                MlTasks.DATA_FRAME_ANALYTICS_TASK_NAME,
                memoryTracker,
                params.isAllowLazyStart() ? Integer.MAX_VALUE : maxLazyMLNodes,
                node -> nodeFilter(node, params)
            );
            // Pass an effectively infinite value for max concurrent opening jobs, because data frame analytics jobs do
            // not have an "opening" state so would never be rejected for causing too many jobs in the "opening" state
            PersistentTasksCustomMetadata.Assignment assignment = jobNodeSelector.selectNode(
                maxOpenJobs,
                Integer.MAX_VALUE,
                maxMachineMemoryPercent,
                maxNodeMemory,
                useAutoMemoryPercentage
            );
            auditRequireMemoryIfNecessary(params.getId(), auditor, assignment, jobNodeSelector, isMemoryTrackerRecentlyRefreshed);
            return assignment;
        }

        @Override
        protected void nodeOperation(AllocatedPersistentTask task, TaskParams params, PersistentTaskState state) {
            DataFrameAnalyticsTask dfaTask = (DataFrameAnalyticsTask) task;
            DataFrameAnalyticsTaskState analyticsTaskState = (DataFrameAnalyticsTaskState) state;
            DataFrameAnalyticsState analyticsState = analyticsTaskState == null
                ? DataFrameAnalyticsState.STOPPED
                : analyticsTaskState.getState();
            logger.info("[{}] Starting data frame analytics from state [{}]", params.getId(), analyticsState);

            // If we are "stopping" there is nothing to do and we should stop
            if (DataFrameAnalyticsState.STOPPING.equals(analyticsState)) {
                logger.info("[{}] data frame analytics got reassigned while stopping. Marking as completed", params.getId());
                task.markAsCompleted();
                return;
            }
            // If we are "failed" then we should leave the task as is; for recovery it must be force stopped.
            if (DataFrameAnalyticsState.FAILED.equals(analyticsState)) {
                return;
            }

            // Execute task
            ActionListener<GetDataFrameAnalyticsStatsAction.Response> statsListener = ActionListener.wrap(statsResponse -> {
                GetDataFrameAnalyticsStatsAction.Response.Stats stats = statsResponse.getResponse().results().get(0);
                dfaTask.setStatsHolder(
                    new StatsHolder(stats.getProgress(), stats.getMemoryUsage(), stats.getAnalysisStats(), stats.getDataCounts())
                );
                executeTask(dfaTask);
            }, dfaTask::setFailed);

            // Get stats to initialize in memory stats tracking
            ActionListener<Boolean> indexCheckListener = ActionListener.wrap(
                ok -> executeAsyncWithOrigin(
                    client,
                    ML_ORIGIN,
                    GetDataFrameAnalyticsStatsAction.INSTANCE,
                    new GetDataFrameAnalyticsStatsAction.Request(params.getId()),
                    statsListener
                ),
                error -> {
                    Throwable cause = ExceptionsHelper.unwrapCause(error);
                    logger.error(
                        () -> format(
                            "[%s] failed to create internal index [%s]",
                            params.getId(),
                            InferenceIndexConstants.LATEST_INDEX_NAME
                        ),
                        cause
                    );
                    dfaTask.setFailed(error);
                }
            );

            // Create the system index explicitly. Although the master node would create it automatically on first use,
            // in a mixed version cluster where the master node is on an older version than this node relying on auto-creation
            // might use outdated mappings.
            MlIndexAndAlias.createSystemIndexIfNecessary(
                client,
                clusterState,
                MachineLearning.getInferenceIndexSystemIndexDescriptor(),
                MlTasks.PERSISTENT_TASK_MASTER_NODE_TIMEOUT,
                indexCheckListener
            );
        }

        private void executeTask(DataFrameAnalyticsTask task) {
            DataFrameAnalyticsTaskState startedState = new DataFrameAnalyticsTaskState(
                DataFrameAnalyticsState.STARTED,
                task.getAllocationId(),
                null,
                Instant.now()
            );
            task.updatePersistentTaskState(
                startedState,
                ActionListener.wrap(
                    response -> manager.execute(task, clusterState, MlTasks.PERSISTENT_TASK_MASTER_NODE_TIMEOUT),
                    task::markAsFailed
                )
            );
        }

        public static String nodeFilter(DiscoveryNode node, TaskParams params) {
            String id = params.getId();

            if (MlConfigVersion.fromNode(node).before(TaskParams.VERSION_INTRODUCED)) {
                return "Not opening job ["
                    + id
                    + "] on node ["
                    + JobNodeSelector.nodeNameAndVersion(node)
                    + "], because the data frame analytics requires a node with ML config version ["
                    + TaskParams.VERSION_INTRODUCED
                    + "] or higher";
            }

            return null;
        }

        @Override
        protected String[] indicesOfInterest(TaskParams params) {
            return new String[] { MlConfigIndex.indexName(), MlStatsIndex.indexPattern(), AnomalyDetectorsIndex.jobStateIndexPattern() };
        }

        @Override
        protected String getJobId(TaskParams params) {
            return params.getId();
        }

    }
}
