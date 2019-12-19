/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.action;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.license.LicenseUtils;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.persistent.AllocatedPersistentTask;
import org.elasticsearch.persistent.PersistentTaskParams;
import org.elasticsearch.persistent.PersistentTaskState;
import org.elasticsearch.persistent.PersistentTasksCustomMetaData;
import org.elasticsearch.persistent.PersistentTasksExecutor;
import org.elasticsearch.persistent.PersistentTasksService;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.XPackField;
import org.elasticsearch.xpack.core.ml.MlMetadata;
import org.elasticsearch.xpack.core.ml.MlTasks;
import org.elasticsearch.xpack.core.ml.action.ExplainDataFrameAnalyticsAction;
import org.elasticsearch.xpack.core.ml.action.GetDataFrameAnalyticsStatsAction;
import org.elasticsearch.xpack.core.ml.action.PutDataFrameAnalyticsAction;
import org.elasticsearch.xpack.core.ml.action.StartDataFrameAnalyticsAction;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsConfig;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsState;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsTaskState;
import org.elasticsearch.xpack.core.ml.job.messages.Messages;
import org.elasticsearch.xpack.core.ml.job.persistence.AnomalyDetectorsIndex;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.core.ml.utils.PhaseProgress;
import org.elasticsearch.xpack.ml.MachineLearning;
import org.elasticsearch.xpack.ml.dataframe.DataFrameAnalyticsManager;
import org.elasticsearch.xpack.ml.dataframe.DataFrameAnalyticsTask;
import org.elasticsearch.xpack.ml.dataframe.MappingsMerger;
import org.elasticsearch.xpack.ml.dataframe.SourceDestValidator;
import org.elasticsearch.xpack.ml.dataframe.extractor.DataFrameDataExtractorFactory;
import org.elasticsearch.xpack.ml.dataframe.extractor.ExtractedFieldsDetectorFactory;
import org.elasticsearch.xpack.ml.dataframe.persistence.DataFrameAnalyticsConfigProvider;
import org.elasticsearch.xpack.ml.extractor.ExtractedFields;
import org.elasticsearch.xpack.ml.job.JobNodeSelector;
import org.elasticsearch.xpack.ml.notifications.DataFrameAnalyticsAuditor;
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

/**
 * Starts the persistent task for running data frame analytics.
 *
 * TODO Add to the upgrade mode action
 */
public class TransportStartDataFrameAnalyticsAction
    extends TransportMasterNodeAction<StartDataFrameAnalyticsAction.Request, AcknowledgedResponse> {

    private static final Logger logger = LogManager.getLogger(TransportStartDataFrameAnalyticsAction.class);

    private final XPackLicenseState licenseState;
    private final Client client;
    private final PersistentTasksService persistentTasksService;
    private final DataFrameAnalyticsConfigProvider configProvider;
    private final MlMemoryTracker memoryTracker;
    private final DataFrameAnalyticsAuditor auditor;

    @Inject
    public TransportStartDataFrameAnalyticsAction(TransportService transportService, Client client, ClusterService clusterService,
                                                  ThreadPool threadPool, ActionFilters actionFilters, XPackLicenseState licenseState,
                                                  IndexNameExpressionResolver indexNameExpressionResolver,
                                                  PersistentTasksService persistentTasksService,
                                                  DataFrameAnalyticsConfigProvider configProvider, MlMemoryTracker memoryTracker,
                                                  DataFrameAnalyticsAuditor auditor) {
        super(StartDataFrameAnalyticsAction.NAME, transportService, clusterService, threadPool, actionFilters,
                StartDataFrameAnalyticsAction.Request::new, indexNameExpressionResolver);
        this.licenseState = licenseState;
        this.client = client;
        this.persistentTasksService = persistentTasksService;
        this.configProvider = configProvider;
        this.memoryTracker = memoryTracker;
        this.auditor = Objects.requireNonNull(auditor);
    }

    @Override
    protected String executor() {
        // This api doesn't do heavy or blocking operations (just delegates PersistentTasksService),
        // so we can do this on the network thread
        return ThreadPool.Names.SAME;
    }

    @Override
    protected AcknowledgedResponse read(StreamInput in) throws IOException {
        return new AcknowledgedResponse(in);
    }

    @Override
    protected ClusterBlockException checkBlock(StartDataFrameAnalyticsAction.Request request, ClusterState state) {
        // We only delegate here to PersistentTasksService, but if there is a metadata writeblock,
        // then delegating to PersistentTasksService doesn't make a whole lot of sense,
        // because PersistentTasksService will then fail.
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }

    @Override
    protected void masterOperation(Task task, StartDataFrameAnalyticsAction.Request request, ClusterState state,
                                   ActionListener<AcknowledgedResponse> listener) {
        if (licenseState.isMachineLearningAllowed() == false) {
            listener.onFailure(LicenseUtils.newComplianceException(XPackField.MACHINE_LEARNING));
            return;
        }

        // Wait for analytics to be started
        ActionListener<PersistentTasksCustomMetaData.PersistentTask<StartDataFrameAnalyticsAction.TaskParams>> waitForAnalyticsToStart =
            new ActionListener<PersistentTasksCustomMetaData.PersistentTask<StartDataFrameAnalyticsAction.TaskParams>>() {
                @Override
                public void onResponse(PersistentTasksCustomMetaData.PersistentTask<StartDataFrameAnalyticsAction.TaskParams> task) {
                    waitForAnalyticsStarted(task, request.getTimeout(), listener);
                }

                @Override
                public void onFailure(Exception e) {
                    if (ExceptionsHelper.unwrapCause(e) instanceof ResourceAlreadyExistsException) {
                        e = new ElasticsearchStatusException("Cannot start data frame analytics [" + request.getId() +
                            "] because it has already been started", RestStatus.CONFLICT, e);
                    }
                    listener.onFailure(e);
                }
            };

        // Start persistent task
        ActionListener<StartContext> memoryUsageHandledListener = ActionListener.wrap(
            startContext -> {
                StartDataFrameAnalyticsAction.TaskParams taskParams = new StartDataFrameAnalyticsAction.TaskParams(
                    request.getId(), startContext.config.getVersion(), startContext.progressOnStart,
                    startContext.config.isAllowLazyStart());
                persistentTasksService.sendStartRequest(MlTasks.dataFrameAnalyticsTaskId(request.getId()),
                    MlTasks.DATA_FRAME_ANALYTICS_TASK_NAME, taskParams, waitForAnalyticsToStart);
            },
            listener::onFailure
        );

        // Perform memory usage estimation for this config
        ActionListener<StartContext> startContextListener = ActionListener.wrap(
            startContext -> {
                estimateMemoryUsageAndUpdateMemoryTracker(startContext, memoryUsageHandledListener);
            },
            listener::onFailure
        );

        // Get start context
        getStartContext(request.getId(), startContextListener);
    }

    private void estimateMemoryUsageAndUpdateMemoryTracker(StartContext startContext, ActionListener<StartContext> listener) {
        final String jobId = startContext.config.getId();

        // Tell the job tracker to refresh the memory requirement for this job and all other jobs that have persistent tasks
        ActionListener<ExplainDataFrameAnalyticsAction.Response> explainListener = ActionListener.wrap(
            explainResponse -> {
                ByteSizeValue expectedMemoryWithoutDisk = explainResponse.getMemoryEstimation().getExpectedMemoryWithoutDisk();
                auditor.info(jobId,
                    Messages.getMessage(Messages.DATA_FRAME_ANALYTICS_AUDIT_ESTIMATED_MEMORY_USAGE, expectedMemoryWithoutDisk));
                // Validate that model memory limit is sufficient to run the analysis
                if (startContext.config.getModelMemoryLimit()
                    .compareTo(expectedMemoryWithoutDisk) < 0) {
                    ElasticsearchStatusException e =
                        ExceptionsHelper.badRequestException(
                            "Cannot start because the configured model memory limit [{}] is lower than the expected memory usage [{}]",
                            startContext.config.getModelMemoryLimit(), expectedMemoryWithoutDisk);
                    listener.onFailure(e);
                    return;
                }
                // Refresh memory requirement for jobs
                memoryTracker.addDataFrameAnalyticsJobMemoryAndRefreshAllOthers(
                    jobId, startContext.config.getModelMemoryLimit().getBytes(), ActionListener.wrap(
                        aVoid -> listener.onResponse(startContext), listener::onFailure));
            },
            listener::onFailure
        );

        PutDataFrameAnalyticsAction.Request explainRequest = new PutDataFrameAnalyticsAction.Request(startContext.config);
        ClientHelper.executeAsyncWithOrigin(
            client,
            ClientHelper.ML_ORIGIN,
            ExplainDataFrameAnalyticsAction.INSTANCE,
            explainRequest,
            explainListener);

    }

    private void getStartContext(String id, ActionListener<StartContext> finalListener) {

        // Step 6. Validate that there are analyzable data in the source index
        ActionListener<StartContext> validateMappingsMergeListener = ActionListener.wrap(
            startContext -> validateSourceIndexHasRows(startContext, finalListener),
            finalListener::onFailure
        );

        // Step 5. Validate mappings can be merged
        ActionListener<StartContext> toValidateMappingsListener = ActionListener.wrap(
            startContext -> MappingsMerger.mergeMappings(client, startContext.config.getHeaders(),
                startContext.config.getSource(), ActionListener.wrap(
                mappings -> validateMappingsMergeListener.onResponse(startContext), finalListener::onFailure)),
            finalListener::onFailure
        );

        // Step 4. Validate dest index is empty if task is starting for first time
        ActionListener<StartContext> toValidateDestEmptyListener = ActionListener.wrap(
            startContext -> {
                switch (startContext.startingState) {
                    case FIRST_TIME:
                        checkDestIndexIsEmptyIfExists(startContext, toValidateMappingsListener);
                        break;
                    case RESUMING_REINDEXING:
                    case RESUMING_ANALYZING:
                        toValidateMappingsListener.onResponse(startContext);
                        break;
                    case FINISHED:
                        logger.info("[{}] Job has already finished", startContext.config.getId());
                        finalListener.onFailure(ExceptionsHelper.badRequestException(
                            "Cannot start because the job has already finished"));
                        break;
                    default:
                        finalListener.onFailure(ExceptionsHelper.serverError("Unexpected starting state " + startContext.startingState));
                        break;
                }
            },
            finalListener::onFailure
        );

        // Step 3. Validate source and dest; check data extraction is possible
        ActionListener<StartContext> startContextListener = ActionListener.wrap(
            startContext -> {
                // Validate the query parses
                startContext.config.getSource().getParsedQuery();

                // Validate source/dest are valid
                new SourceDestValidator(clusterService.state(), indexNameExpressionResolver).check(startContext.config);

                // Validate extraction is possible
                boolean isTaskRestarting = startContext.startingState != DataFrameAnalyticsTask.StartingState.FIRST_TIME;
                new ExtractedFieldsDetectorFactory(client).createFromSource(startContext.config, isTaskRestarting, ActionListener.wrap(
                    extractedFieldsDetector -> {
                        startContext.extractedFields = extractedFieldsDetector.detect().v1();
                        toValidateDestEmptyListener.onResponse(startContext);
                    },
                    finalListener::onFailure));
            },
            finalListener::onFailure
        );

        // Step 2. Get stats to recover progress
        ActionListener<DataFrameAnalyticsConfig> getConfigListener = ActionListener.wrap(
            config -> getProgress(config, ActionListener.wrap(
                progress -> startContextListener.onResponse(new StartContext(config, progress)), finalListener::onFailure)),
            finalListener::onFailure
        );

        // Step 1. Get the config
        configProvider.get(id, getConfigListener);
    }

    private void validateSourceIndexHasRows(StartContext startContext, ActionListener<StartContext> listener) {
        DataFrameDataExtractorFactory extractorFactory = DataFrameDataExtractorFactory.createForSourceIndices(client,
            "validate_source_index_has_rows-" + startContext.config.getId(),
            startContext.config,
            startContext.extractedFields);
        extractorFactory.newExtractor(false)
            .collectDataSummaryAsync(ActionListener.wrap(
                dataSummary -> {
                    if (dataSummary.rows == 0) {
                        listener.onFailure(ExceptionsHelper.badRequestException(
                            "Unable to start {} as no documents in the source indices [{}] contained all the fields "
                                + "selected for analysis. If you are relying on automatic field selection then there are "
                                + "currently mapped fields that do not exist in any indexed documents, and you will have "
                                + "to switch to explicit field selection and include only fields that exist in indexed "
                                + "documents.",
                            startContext.config.getId(),
                            Strings.arrayToCommaDelimitedString(startContext.config.getSource().getIndex())
                        ));
                    } else {
                        listener.onResponse(startContext);
                    }
                },
                listener::onFailure
            ));
    }

    private void getProgress(DataFrameAnalyticsConfig config, ActionListener<List<PhaseProgress>> listener) {
        GetDataFrameAnalyticsStatsAction.Request getStatsRequest = new GetDataFrameAnalyticsStatsAction.Request(config.getId());
        executeAsyncWithOrigin(client, ML_ORIGIN, GetDataFrameAnalyticsStatsAction.INSTANCE, getStatsRequest, ActionListener.wrap(
            statsResponse -> {
                List<GetDataFrameAnalyticsStatsAction.Response.Stats> stats = statsResponse.getResponse().results();
                if (stats.isEmpty()) {
                    // The job has been deleted in between
                    listener.onFailure(ExceptionsHelper.missingDataFrameAnalytics(config.getId()));
                } else {
                    listener.onResponse(stats.get(0).getProgress());
                }
            },
            listener::onFailure
        ));
    }

    private void checkDestIndexIsEmptyIfExists(StartContext startContext, ActionListener<StartContext> listener) {
        String destIndex = startContext.config.getDest().getIndex();
        SearchRequest destEmptySearch = new SearchRequest(destIndex);
        destEmptySearch.source().size(0);
        destEmptySearch.allowPartialSearchResults(false);
        ClientHelper.executeWithHeadersAsync(startContext.config.getHeaders(), ClientHelper.ML_ORIGIN, client, SearchAction.INSTANCE,
            destEmptySearch, ActionListener.wrap(
                searchResponse -> {
                    if (searchResponse.getHits().getTotalHits().value > 0) {
                        listener.onFailure(ExceptionsHelper.badRequestException("dest index [{}] must be empty", destIndex));
                    } else {
                        listener.onResponse(startContext);
                    }
                },
                e -> {
                    if (ExceptionsHelper.unwrapCause(e) instanceof IndexNotFoundException) {
                        listener.onResponse(startContext);
                    } else {
                        listener.onFailure(e);
                    }
                }
            )
        );
    }

    private void waitForAnalyticsStarted(PersistentTasksCustomMetaData.PersistentTask<StartDataFrameAnalyticsAction.TaskParams> task,
                                         TimeValue timeout, ActionListener<AcknowledgedResponse> listener) {
        AnalyticsPredicate predicate = new AnalyticsPredicate();
        persistentTasksService.waitForPersistentTaskCondition(task.getId(), predicate, timeout,

            new PersistentTasksService.WaitForPersistentTaskListener<PersistentTaskParams>() {

                @Override
                public void onResponse(PersistentTasksCustomMetaData.PersistentTask<PersistentTaskParams> persistentTask) {
                    if (predicate.exception != null) {
                        // We want to return to the caller without leaving an unassigned persistent task, to match
                        // what would have happened if the error had been detected in the "fast fail" validation
                        cancelAnalyticsStart(task, predicate.exception, listener);
                    } else {
                        auditor.info(task.getParams().getId(), Messages.DATA_FRAME_ANALYTICS_AUDIT_STARTED);
                        listener.onResponse(new AcknowledgedResponse(true));
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    listener.onFailure(e);
                }

                @Override
                public void onTimeout(TimeValue timeout) {
                    listener.onFailure(new ElasticsearchException(
                        "Starting data frame analytics [" + task.getParams().getId() + "] timed out after [" + timeout + "]"));
                }
        });
    }

    private static class StartContext {
        private final DataFrameAnalyticsConfig config;
        private final List<PhaseProgress> progressOnStart;
        private final DataFrameAnalyticsTask.StartingState startingState;
        private volatile ExtractedFields extractedFields;

        private StartContext(DataFrameAnalyticsConfig config, List<PhaseProgress> progressOnStart) {
            this.config = config;
            this.progressOnStart = progressOnStart;
            this.startingState = DataFrameAnalyticsTask.determineStartingState(config.getId(), progressOnStart);
        }
    }

    /**
     * Important: the methods of this class must NOT throw exceptions.  If they did then the callers
     * of endpoints waiting for a condition tested by this predicate would never get a response.
     */
    private static class AnalyticsPredicate implements Predicate<PersistentTasksCustomMetaData.PersistentTask<?>> {

        private volatile Exception exception;

        @Override
        public boolean test(PersistentTasksCustomMetaData.PersistentTask<?> persistentTask) {
            if (persistentTask == null) {
                return false;
            }

            PersistentTasksCustomMetaData.Assignment assignment = persistentTask.getAssignment();

            // This means we are awaiting a new node to be spun up, ok to return back to the user to await node creation
            if (assignment != null && assignment.equals(JobNodeSelector.AWAITING_LAZY_ASSIGNMENT)) {
                return true;
            }

            if (assignment != null && assignment.equals(PersistentTasksCustomMetaData.INITIAL_ASSIGNMENT) == false &&
                assignment.isAssigned() == false) {
                // Assignment has failed despite passing our "fast fail" validation
                exception = new ElasticsearchStatusException("Could not start data frame analytics task, allocation explanation [" +
                    assignment.getExplanation() + "]", RestStatus.TOO_MANY_REQUESTS);
                return true;
            }
            DataFrameAnalyticsTaskState taskState = (DataFrameAnalyticsTaskState) persistentTask.getState();
            DataFrameAnalyticsState analyticsState = taskState == null ? DataFrameAnalyticsState.STOPPED : taskState.getState();
            switch (analyticsState) {
                case STARTED:
                case REINDEXING:
                case ANALYZING:
                    return true;
                case STOPPING:
                    exception = ExceptionsHelper.conflictStatusException("the task has been stopped while waiting to be started");
                    return true;
                // The STARTING case here is expected to be incredibly short-lived, just occurring during the
                // time period when a job has successfully been assigned to a node but the request to update
                // its task state is still in-flight.  (The long-lived STARTING case when a lazy node needs to
                // be added to the cluster to accommodate the job was dealt with higher up this method when the
                // magic AWAITING_LAZY_ASSIGNMENT assignment was checked for.)
                case STARTING:
                case STOPPED:
                    return false;
                case FAILED:
                default:
                    exception = ExceptionsHelper.serverError("Unexpected task state [" + analyticsState + "] while waiting to be started");
                    return true;
            }
        }
    }

    private void cancelAnalyticsStart(
        PersistentTasksCustomMetaData.PersistentTask<StartDataFrameAnalyticsAction.TaskParams> persistentTask, Exception exception,
        ActionListener<AcknowledgedResponse> listener) {
        persistentTasksService.sendRemoveRequest(persistentTask.getId(),
            new ActionListener<PersistentTasksCustomMetaData.PersistentTask<?>>() {
                @Override
                public void onResponse(PersistentTasksCustomMetaData.PersistentTask<?> task) {
                    // We succeeded in cancelling the persistent task, but the
                    // problem that caused us to cancel it is the overall result
                    listener.onFailure(exception);
                }

                @Override
                public void onFailure(Exception e) {
                    logger.error("[" + persistentTask.getParams().getId() + "] Failed to cancel persistent task that could " +
                        "not be assigned due to [" + exception.getMessage() + "]", e);
                    listener.onFailure(exception);
                }
            }
        );
    }

    static List<String> verifyIndicesPrimaryShardsAreActive(ClusterState clusterState, String... indexNames) {
        IndexNameExpressionResolver resolver = new IndexNameExpressionResolver();
        String[] concreteIndices = resolver.concreteIndexNames(clusterState, IndicesOptions.lenientExpandOpen(), indexNames);
        List<String> unavailableIndices = new ArrayList<>(concreteIndices.length);
        for (String index : concreteIndices) {
            IndexRoutingTable routingTable = clusterState.getRoutingTable().index(index);
            if (routingTable == null || routingTable.allPrimaryShardsActive() == false) {
                unavailableIndices.add(index);
            }
        }
        return unavailableIndices;
    }

    public static class TaskExecutor extends PersistentTasksExecutor<StartDataFrameAnalyticsAction.TaskParams> {

        private final Client client;
        private final ClusterService clusterService;
        private final DataFrameAnalyticsManager manager;
        private final DataFrameAnalyticsAuditor auditor;
        private final MlMemoryTracker memoryTracker;

        private volatile int maxMachineMemoryPercent;
        private volatile int maxLazyMLNodes;
        private volatile int maxOpenJobs;
        private volatile ClusterState clusterState;

        public TaskExecutor(Settings settings, Client client, ClusterService clusterService, DataFrameAnalyticsManager manager,
                            DataFrameAnalyticsAuditor auditor, MlMemoryTracker memoryTracker) {
            super(MlTasks.DATA_FRAME_ANALYTICS_TASK_NAME, MachineLearning.UTILITY_THREAD_POOL_NAME);
            this.client = Objects.requireNonNull(client);
            this.clusterService = Objects.requireNonNull(clusterService);
            this.manager = Objects.requireNonNull(manager);
            this.auditor = Objects.requireNonNull(auditor);
            this.memoryTracker = Objects.requireNonNull(memoryTracker);
            this.maxMachineMemoryPercent = MachineLearning.MAX_MACHINE_MEMORY_PERCENT.get(settings);
            this.maxLazyMLNodes = MachineLearning.MAX_LAZY_ML_NODES.get(settings);
            this.maxOpenJobs = MAX_OPEN_JOBS_PER_NODE.get(settings);
            clusterService.getClusterSettings()
                .addSettingsUpdateConsumer(MachineLearning.MAX_MACHINE_MEMORY_PERCENT, this::setMaxMachineMemoryPercent);
            clusterService.getClusterSettings().addSettingsUpdateConsumer(MachineLearning.MAX_LAZY_ML_NODES, this::setMaxLazyMLNodes);
            clusterService.getClusterSettings().addSettingsUpdateConsumer(MAX_OPEN_JOBS_PER_NODE, this::setMaxOpenJobs);
            clusterService.addListener(event -> clusterState = event.state());
        }

        @Override
        protected AllocatedPersistentTask createTask(
            long id, String type, String action, TaskId parentTaskId,
            PersistentTasksCustomMetaData.PersistentTask<StartDataFrameAnalyticsAction.TaskParams> persistentTask,
            Map<String, String> headers) {
            return new DataFrameAnalyticsTask(
                id, type, action, parentTaskId, headers, client, clusterService, manager, auditor, persistentTask.getParams());
        }

        @Override
        public PersistentTasksCustomMetaData.Assignment getAssignment(StartDataFrameAnalyticsAction.TaskParams params,
                                                                      ClusterState clusterState) {

            // If we are waiting for an upgrade to complete, we should not assign to a node
            if (MlMetadata.getMlMetadata(clusterState).isUpgradeMode()) {
                return AWAITING_UPGRADE;
            }

            String id = params.getId();

            List<String> unavailableIndices = verifyIndicesPrimaryShardsAreActive(clusterState, AnomalyDetectorsIndex.configIndexName());
            if (unavailableIndices.size() != 0) {
                String reason = "Not opening data frame analytics job [" + id +
                    "], because not all primary shards are active for the following indices [" + String.join(",", unavailableIndices) + "]";
                logger.debug(reason);
                return new PersistentTasksCustomMetaData.Assignment(null, reason);
            }

            boolean isMemoryTrackerRecentlyRefreshed = memoryTracker.isRecentlyRefreshed();
            if (isMemoryTrackerRecentlyRefreshed == false) {
                boolean scheduledRefresh = memoryTracker.asyncRefresh();
                if (scheduledRefresh) {
                    String reason = "Not opening data frame analytics job [" + id +
                        "] because job memory requirements are stale - refresh requested";
                    logger.debug(reason);
                    return new PersistentTasksCustomMetaData.Assignment(null, reason);
                }
            }

            JobNodeSelector jobNodeSelector = new JobNodeSelector(clusterState, id, MlTasks.DATA_FRAME_ANALYTICS_TASK_NAME, memoryTracker,
                params.isAllowLazyStart() ? Integer.MAX_VALUE : maxLazyMLNodes, node -> nodeFilter(node, id));
            // Pass an effectively infinite value for max concurrent opening jobs, because data frame analytics jobs do
            // not have an "opening" state so would never be rejected for causing too many jobs in the "opening" state
            return jobNodeSelector.selectNode(
                maxOpenJobs, Integer.MAX_VALUE, maxMachineMemoryPercent, isMemoryTrackerRecentlyRefreshed);
        }

        @Override
        protected void nodeOperation(AllocatedPersistentTask task, StartDataFrameAnalyticsAction.TaskParams params,
                                     PersistentTaskState state) {
            logger.info("[{}] Starting data frame analytics", params.getId());
            DataFrameAnalyticsTaskState analyticsTaskState = (DataFrameAnalyticsTaskState) state;

            // If we are "stopping" there is nothing to do
            // If we are "failed" then we should leave the task as is; for recovery it must be force stopped.
            if (analyticsTaskState != null && analyticsTaskState.getState().isAnyOf(
                    DataFrameAnalyticsState.STOPPING, DataFrameAnalyticsState.FAILED)) {
                return;
            }

            if (analyticsTaskState == null) {
                DataFrameAnalyticsTaskState startedState = new DataFrameAnalyticsTaskState(DataFrameAnalyticsState.STARTED,
                    task.getAllocationId(), null);
                task.updatePersistentTaskState(startedState, ActionListener.wrap(
                    response -> manager.execute((DataFrameAnalyticsTask) task, DataFrameAnalyticsState.STARTED, clusterState),
                    task::markAsFailed));
            } else {
                manager.execute((DataFrameAnalyticsTask) task, analyticsTaskState.getState(), clusterState);
            }
        }

        public static String nodeFilter(DiscoveryNode node, String id) {

            if (node.getVersion().before(StartDataFrameAnalyticsAction.TaskParams.VERSION_INTRODUCED)) {
                return "Not opening job [" + id + "] on node [" + JobNodeSelector.nodeNameAndVersion(node)
                    + "], because the data frame analytics requires a node of version ["
                    + StartDataFrameAnalyticsAction.TaskParams.VERSION_INTRODUCED + "] or higher";
            }

            return null;
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


}
