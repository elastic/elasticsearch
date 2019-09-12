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
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.license.LicenseUtils;
import org.elasticsearch.license.RemoteClusterLicenseChecker;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.persistent.AllocatedPersistentTask;
import org.elasticsearch.persistent.PersistentTaskState;
import org.elasticsearch.persistent.PersistentTasksCustomMetaData;
import org.elasticsearch.persistent.PersistentTasksExecutor;
import org.elasticsearch.persistent.PersistentTasksService;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.RemoteClusterService;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.XPackField;
import org.elasticsearch.xpack.core.ml.MlTasks;
import org.elasticsearch.xpack.core.ml.action.StartDatafeedAction;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedConfig;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedJobValidator;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedState;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedTimingStats;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.core.ml.job.config.JobState;
import org.elasticsearch.xpack.core.ml.job.messages.Messages;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.ml.MachineLearning;
import org.elasticsearch.xpack.ml.MlConfigMigrationEligibilityCheck;
import org.elasticsearch.xpack.ml.datafeed.DatafeedManager;
import org.elasticsearch.xpack.ml.datafeed.DatafeedNodeSelector;
import org.elasticsearch.xpack.ml.datafeed.DatafeedTimingStatsReporter;
import org.elasticsearch.xpack.ml.datafeed.extractor.DataExtractorFactory;
import org.elasticsearch.xpack.ml.datafeed.persistence.DatafeedConfigProvider;
import org.elasticsearch.xpack.ml.job.persistence.JobConfigProvider;
import org.elasticsearch.xpack.ml.notifications.AnomalyDetectionAuditor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Predicate;

/* This class extends from TransportMasterNodeAction for cluster state observing purposes.
 The stop datafeed api also redirect the elected master node.
 The master node will wait for the datafeed to be started by checking the persistent task's status and then return.
 To ensure that a subsequent stop datafeed call will see that same task status (and sanity validation doesn't fail)
 both start and stop datafeed apis redirect to the elected master node.
 In case of instability persistent tasks checks may fail and that is ok, in that case all bets are off.
 The start datafeed api is a low through put api, so the fact that we redirect to elected master node shouldn't be an issue.
 */
public class TransportStartDatafeedAction extends TransportMasterNodeAction<StartDatafeedAction.Request, AcknowledgedResponse> {

    private static final Logger logger = LogManager.getLogger(TransportStartDatafeedAction.class);

    private final Client client;
    private final XPackLicenseState licenseState;
    private final PersistentTasksService persistentTasksService;
    private final JobConfigProvider jobConfigProvider;
    private final DatafeedConfigProvider datafeedConfigProvider;
    private final AnomalyDetectionAuditor auditor;
    private final MlConfigMigrationEligibilityCheck migrationEligibilityCheck;
    private final NamedXContentRegistry xContentRegistry;
    private final boolean remoteClusterSearchSupported;

    @Inject
    public TransportStartDatafeedAction(Settings settings, TransportService transportService, ThreadPool threadPool,
                                        ClusterService clusterService, XPackLicenseState licenseState,
                                        PersistentTasksService persistentTasksService,
                                        ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver,
                                        Client client, JobConfigProvider jobConfigProvider, DatafeedConfigProvider datafeedConfigProvider,
                                        AnomalyDetectionAuditor auditor, NamedXContentRegistry xContentRegistry) {
        super(StartDatafeedAction.NAME, transportService, clusterService, threadPool, actionFilters, StartDatafeedAction.Request::new,
            indexNameExpressionResolver);
        this.licenseState = licenseState;
        this.persistentTasksService = persistentTasksService;
        this.client = client;
        this.jobConfigProvider = jobConfigProvider;
        this.datafeedConfigProvider = datafeedConfigProvider;
        this.auditor = auditor;
        this.migrationEligibilityCheck = new MlConfigMigrationEligibilityCheck(settings, clusterService);
        this.xContentRegistry = xContentRegistry;
        this.remoteClusterSearchSupported = RemoteClusterService.ENABLE_REMOTE_CLUSTERS.get(settings);
    }

    static void validate(Job job,
                         DatafeedConfig datafeedConfig,
                         PersistentTasksCustomMetaData tasks,
                         NamedXContentRegistry xContentRegistry) {
        DatafeedJobValidator.validate(datafeedConfig, job, xContentRegistry);
        DatafeedConfig.validateAggregations(datafeedConfig.getParsedAggregations(xContentRegistry));
        JobState jobState = MlTasks.getJobState(datafeedConfig.getJobId(), tasks);
        if (jobState.isAnyOf(JobState.OPENING, JobState.OPENED) == false) {
            throw ExceptionsHelper.conflictStatusException("cannot start datafeed [" + datafeedConfig.getId() +
                    "] because job [" + job.getId() + "] is " + jobState);
        }
    }

    //Get the deprecation warnings from the parsed query and aggs to audit
    static void auditDeprecations(DatafeedConfig datafeed, Job job, AnomalyDetectionAuditor auditor,
                                  NamedXContentRegistry xContentRegistry) {
        List<String> deprecationWarnings = new ArrayList<>();
        deprecationWarnings.addAll(datafeed.getAggDeprecations(xContentRegistry));
        deprecationWarnings.addAll(datafeed.getQueryDeprecations(xContentRegistry));
        if (deprecationWarnings.isEmpty() == false) {
            String msg = "datafeed [" + datafeed.getId() +"] configuration has deprecations. [" +
                Strings.collectionToDelimitedString(deprecationWarnings, ", ") + "]";
            auditor.warning(job.getId(), msg);
        }

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
    protected void masterOperation(Task task, StartDatafeedAction.Request request, ClusterState state,
                                   ActionListener<AcknowledgedResponse> listener) {
        StartDatafeedAction.DatafeedParams params = request.getParams();
        if (licenseState.isMachineLearningAllowed() == false) {
            listener.onFailure(LicenseUtils.newComplianceException(XPackField.MACHINE_LEARNING));
            return;
        }

        if (migrationEligibilityCheck.datafeedIsEligibleForMigration(request.getParams().getDatafeedId(), state)) {
            listener.onFailure(ExceptionsHelper.configHasNotBeenMigrated("start datafeed", request.getParams().getDatafeedId()));
            return;
        }

        AtomicReference<DatafeedConfig> datafeedConfigHolder = new AtomicReference<>();
        PersistentTasksCustomMetaData tasks = state.getMetaData().custom(PersistentTasksCustomMetaData.TYPE);

        ActionListener<PersistentTasksCustomMetaData.PersistentTask<StartDatafeedAction.DatafeedParams>> waitForTaskListener =
                new ActionListener<PersistentTasksCustomMetaData.PersistentTask<StartDatafeedAction.DatafeedParams>>() {
                    @Override
                    public void onResponse(PersistentTasksCustomMetaData.PersistentTask<StartDatafeedAction.DatafeedParams>
                                                   persistentTask) {
                        waitForDatafeedStarted(persistentTask.getId(), params, listener);
                    }

                    @Override
                    public void onFailure(Exception e) {
                        if (e instanceof ResourceAlreadyExistsException) {
                            logger.debug("datafeed already started", e);
                            e = new ElasticsearchStatusException("cannot start datafeed [" + params.getDatafeedId() +
                                    "] because it has already been started", RestStatus.CONFLICT);
                        }
                        listener.onFailure(e);
                    }
                };

        // Verify data extractor factory can be created, then start persistent task
        Consumer<Job> createDataExtractor = job -> {
                if (RemoteClusterLicenseChecker.containsRemoteIndex(params.getDatafeedIndices())) {
                    final RemoteClusterLicenseChecker remoteClusterLicenseChecker =
                            new RemoteClusterLicenseChecker(client, XPackLicenseState::isMachineLearningAllowedForOperationMode);
                    remoteClusterLicenseChecker.checkRemoteClusterLicenses(
                            RemoteClusterLicenseChecker.remoteClusterAliases(
                                    transportService.getRemoteClusterService().getRegisteredRemoteClusterNames(),
                                    params.getDatafeedIndices()),
                            ActionListener.wrap(
                                    response -> {
                                        if (response.isSuccess() == false) {
                                            listener.onFailure(createUnlicensedError(params.getDatafeedId(), response));
                                        } else if (remoteClusterSearchSupported == false) {
                                            listener.onFailure(
                                                ExceptionsHelper.badRequestException(Messages.getMessage(
                                                    Messages.DATAFEED_NEEDS_REMOTE_CLUSTER_SEARCH,
                                                    datafeedConfigHolder.get().getId(),
                                                    RemoteClusterLicenseChecker.remoteIndices(datafeedConfigHolder.get().getIndices()),
                                                    clusterService.getNodeName())));
                                        } else {
                                            createDataExtractor(job, datafeedConfigHolder.get(), params, waitForTaskListener);
                                        }
                                    },
                                    e -> listener.onFailure(
                                            createUnknownLicenseError(
                                                    params.getDatafeedId(),
                                                    RemoteClusterLicenseChecker.remoteIndices(params.getDatafeedIndices()), e))
                            )
                    );
                } else {
                    createDataExtractor(job, datafeedConfigHolder.get(), params, waitForTaskListener);
                }
            };

        ActionListener<Job.Builder> jobListener = ActionListener.wrap(
                jobBuilder -> {
                    try {
                        Job job = jobBuilder.build();
                        validate(job, datafeedConfigHolder.get(), tasks, xContentRegistry);
                        auditDeprecations(datafeedConfigHolder.get(), job, auditor, xContentRegistry);
                        createDataExtractor.accept(job);
                    } catch (Exception e) {
                        listener.onFailure(e);
                    }
                },
                listener::onFailure
        );

        ActionListener<DatafeedConfig.Builder> datafeedListener = ActionListener.wrap(
                datafeedBuilder -> {
                    try {
                        DatafeedConfig datafeedConfig = datafeedBuilder.build();
                        params.setDatafeedIndices(datafeedConfig.getIndices());
                        params.setJobId(datafeedConfig.getJobId());
                        datafeedConfigHolder.set(datafeedConfig);
                        jobConfigProvider.getJob(datafeedConfig.getJobId(), jobListener);
                    } catch (Exception e) {
                        listener.onFailure(e);
                    }
                },
                listener::onFailure
        );

        datafeedConfigProvider.getDatafeedConfig(params.getDatafeedId(), datafeedListener);
    }

    /** Creates {@link DataExtractorFactory} solely for the purpose of validation i.e. verifying that it can be created. */
    private void createDataExtractor(Job job, DatafeedConfig datafeed, StartDatafeedAction.DatafeedParams params,
                                     ActionListener<PersistentTasksCustomMetaData.PersistentTask<StartDatafeedAction.DatafeedParams>>
                                             listener) {
        DataExtractorFactory.create(
            client,
            datafeed,
            job,
            xContentRegistry,
            // Fake DatafeedTimingStatsReporter that does not have access to results index
            new DatafeedTimingStatsReporter(new DatafeedTimingStats(job.getId()), (ts, refreshPolicy) -> {}),
            ActionListener.wrap(
                unused ->
                    persistentTasksService.sendStartRequest(
                        MlTasks.datafeedTaskId(params.getDatafeedId()), MlTasks.DATAFEED_TASK_NAME, params, listener),
                listener::onFailure));
    }

    @Override
    protected ClusterBlockException checkBlock(StartDatafeedAction.Request request, ClusterState state) {
        // We only delegate here to PersistentTasksService, but if there is a metadata writeblock,
        // then delagating to PersistentTasksService doesn't make a whole lot of sense,
        // because PersistentTasksService will then fail.
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }

    private void waitForDatafeedStarted(String taskId, StartDatafeedAction.DatafeedParams params,
                                        ActionListener<AcknowledgedResponse> listener) {
        DatafeedPredicate predicate = new DatafeedPredicate();
        persistentTasksService.waitForPersistentTaskCondition(taskId, predicate, params.getTimeout(),
                new PersistentTasksService.WaitForPersistentTaskListener<StartDatafeedAction.DatafeedParams>() {
                    @Override
                    public void onResponse(PersistentTasksCustomMetaData.PersistentTask<StartDatafeedAction.DatafeedParams>
                                                   persistentTask) {
                        if (predicate.exception != null) {
                            // We want to return to the caller without leaving an unassigned persistent task, to match
                            // what would have happened if the error had been detected in the "fast fail" validation
                            cancelDatafeedStart(persistentTask, predicate.exception, listener);
                        } else {
                            listener.onResponse(new AcknowledgedResponse(true));
                        }
                    }

                    @Override
                    public void onFailure(Exception e) {
                        listener.onFailure(e);
                    }

                    @Override
                    public void onTimeout(TimeValue timeout) {
                        listener.onFailure(new ElasticsearchException("Starting datafeed ["
                                + params.getDatafeedId() + "] timed out after [" + timeout + "]"));
                    }
                });
    }

    private void cancelDatafeedStart(PersistentTasksCustomMetaData.PersistentTask<StartDatafeedAction.DatafeedParams> persistentTask,
                                     Exception exception, ActionListener<AcknowledgedResponse> listener) {
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
                        logger.error("[" + persistentTask.getParams().getDatafeedId() + "] Failed to cancel persistent task that could " +
                                "not be assigned due to [" + exception.getMessage() + "]", e);
                        listener.onFailure(exception);
                    }
                }
        );
    }

    private ElasticsearchStatusException createUnlicensedError(
            final String datafeedId, final RemoteClusterLicenseChecker.LicenseCheck licenseCheck) {
        final String message = String.format(
                Locale.ROOT,
                "cannot start datafeed [%s] as it is configured to use indices on remote cluster [%s] that is not licensed for ml; %s",
                datafeedId,
                licenseCheck.remoteClusterLicenseInfo().clusterAlias(),
                RemoteClusterLicenseChecker.buildErrorMessage(
                        "ml",
                        licenseCheck.remoteClusterLicenseInfo(),
                        RemoteClusterLicenseChecker::isLicensePlatinumOrTrial));
        return new ElasticsearchStatusException(message, RestStatus.BAD_REQUEST);
    }

    private ElasticsearchStatusException createUnknownLicenseError(
            final String datafeedId, final List<String> remoteIndices, final Exception cause) {
        final int numberOfRemoteClusters = RemoteClusterLicenseChecker.remoteClusterAliases(
                transportService.getRemoteClusterService().getRegisteredRemoteClusterNames(), remoteIndices).size();
        assert numberOfRemoteClusters > 0;
        final String remoteClusterQualifier = numberOfRemoteClusters == 1 ? "a remote cluster" : "remote clusters";
        final String licenseTypeQualifier = numberOfRemoteClusters == 1 ? "" : "s";
        final String message = String.format(
                Locale.ROOT,
                "cannot start datafeed [%s] as it uses indices on %s %s but the license type%s could not be verified",
                datafeedId,
                remoteClusterQualifier,
                remoteIndices,
                licenseTypeQualifier);

        return new ElasticsearchStatusException(message, RestStatus.BAD_REQUEST, cause);
    }

    public static class StartDatafeedPersistentTasksExecutor extends PersistentTasksExecutor<StartDatafeedAction.DatafeedParams> {
        private final DatafeedManager datafeedManager;
        private final IndexNameExpressionResolver resolver;

        public StartDatafeedPersistentTasksExecutor(DatafeedManager datafeedManager) {
            super(MlTasks.DATAFEED_TASK_NAME, MachineLearning.UTILITY_THREAD_POOL_NAME);
            this.datafeedManager = datafeedManager;
            this.resolver = new IndexNameExpressionResolver();
        }

        @Override
        public PersistentTasksCustomMetaData.Assignment getAssignment(StartDatafeedAction.DatafeedParams params,
                                                                      ClusterState clusterState) {
            return new DatafeedNodeSelector(clusterState, resolver, params.getDatafeedId(), params.getJobId(),
                    params.getDatafeedIndices()).selectNode();
        }

        @Override
        public void validate(StartDatafeedAction.DatafeedParams params, ClusterState clusterState) {
            new DatafeedNodeSelector(clusterState, resolver, params.getDatafeedId(), params.getJobId(), params.getDatafeedIndices())
                    .checkDatafeedTaskCanBeCreated();
        }

        @Override
        protected void nodeOperation(final AllocatedPersistentTask allocatedPersistentTask,
                                     final StartDatafeedAction.DatafeedParams params,
                                     final PersistentTaskState state) {
            DatafeedTask datafeedTask = (DatafeedTask) allocatedPersistentTask;
            datafeedTask.datafeedManager = datafeedManager;
            datafeedManager.run(datafeedTask,
                    (error) -> {
                        if (error != null) {
                            datafeedTask.markAsFailed(error);
                        } else {
                            datafeedTask.markAsCompleted();
                        }
                    });
        }

        @Override
        protected AllocatedPersistentTask createTask(
                long id, String type, String action, TaskId parentTaskId,
                PersistentTasksCustomMetaData.PersistentTask<StartDatafeedAction.DatafeedParams> persistentTask,
                Map<String, String> headers) {
            return new DatafeedTask(id, type, action, parentTaskId, persistentTask.getParams(), headers);
        }
    }

    public static class DatafeedTask extends AllocatedPersistentTask implements StartDatafeedAction.DatafeedTaskMatcher {

        private final String datafeedId;
        private final long startTime;
        private final Long endTime;
        /* only pck protected for testing */
        volatile DatafeedManager datafeedManager;

        DatafeedTask(long id, String type, String action, TaskId parentTaskId, StartDatafeedAction.DatafeedParams params,
                     Map<String, String> headers) {
            super(id, type, action, "datafeed-" + params.getDatafeedId(), parentTaskId, headers);
            this.datafeedId = params.getDatafeedId();
            this.startTime = params.getStartTime();
            this.endTime = params.getEndTime();
        }

        public String getDatafeedId() {
            return datafeedId;
        }

        public long getDatafeedStartTime() {
            return startTime;
        }

        @Nullable
        public Long getEndTime() {
            return endTime;
        }

        public boolean isLookbackOnly() {
            return endTime != null;
        }

        @Override
        protected void onCancelled() {
            // If the persistent task framework wants us to stop then we should do so immediately and
            // we should wait for an existing datafeed import to realize we want it to stop.
            // Note that this only applied when task cancel is invoked and stop datafeed api doesn't use this.
            // Also stop datafeed api will obey the timeout.
            stop(getReasonCancelled(), TimeValue.ZERO);
        }

        public void stop(String reason, TimeValue timeout) {
            if (datafeedManager != null) {
                datafeedManager.stopDatafeed(this, reason, timeout);
            }
        }

        public void isolate() {
            if (datafeedManager != null) {
                datafeedManager.isolateDatafeed(getAllocationId());
            }
        }
    }

    /**
     * Important: the methods of this class must NOT throw exceptions.  If they did then the callers
     * of endpoints waiting for a condition tested by this predicate would never get a response.
     */
    private class DatafeedPredicate implements Predicate<PersistentTasksCustomMetaData.PersistentTask<?>> {

        private volatile Exception exception;

        @Override
        public boolean test(PersistentTasksCustomMetaData.PersistentTask<?> persistentTask) {
            if (persistentTask == null) {
                return false;
            }
            PersistentTasksCustomMetaData.Assignment assignment = persistentTask.getAssignment();
            if (assignment != null && assignment.equals(PersistentTasksCustomMetaData.INITIAL_ASSIGNMENT) == false &&
                    assignment.isAssigned() == false) {
                // Assignment has failed despite passing our "fast fail" validation
                exception = new ElasticsearchStatusException("Could not start datafeed, allocation explanation [" +
                        assignment.getExplanation() + "]", RestStatus.TOO_MANY_REQUESTS);
                return true;
            }
            DatafeedState datafeedState = (DatafeedState) persistentTask.getState();
            return datafeedState == DatafeedState.STARTED;
        }
    }
}
