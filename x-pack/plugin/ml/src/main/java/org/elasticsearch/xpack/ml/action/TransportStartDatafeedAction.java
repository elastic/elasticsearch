/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.action;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.ContextPreservingActionListener;
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
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.license.LicenseUtils;
import org.elasticsearch.license.RemoteClusterLicenseChecker;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.persistent.AllocatedPersistentTask;
import org.elasticsearch.persistent.PersistentTaskState;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.persistent.PersistentTasksExecutor;
import org.elasticsearch.persistent.PersistentTasksService;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.RemoteClusterService;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xpack.core.XPackField;
import org.elasticsearch.xpack.core.ml.MachineLearningField;
import org.elasticsearch.xpack.core.ml.MlTasks;
import org.elasticsearch.xpack.core.ml.action.GetDatafeedRunningStateAction;
import org.elasticsearch.xpack.core.ml.action.NodeAcknowledgedResponse;
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
import org.elasticsearch.xpack.ml.datafeed.DatafeedNodeSelector;
import org.elasticsearch.xpack.ml.datafeed.DatafeedRunner;
import org.elasticsearch.xpack.ml.datafeed.DatafeedTimingStatsReporter;
import org.elasticsearch.xpack.ml.datafeed.extractor.DataExtractorFactory;
import org.elasticsearch.xpack.ml.datafeed.persistence.DatafeedConfigProvider;
import org.elasticsearch.xpack.ml.job.persistence.JobConfigProvider;
import org.elasticsearch.xpack.ml.notifications.AnomalyDetectionAuditor;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.core.common.validation.SourceDestValidator.REMOTE_CLUSTERS_TRANSPORT_TOO_OLD;

/* This class extends from TransportMasterNodeAction for cluster state observing purposes.
 The stop datafeed api also redirect the elected master node.
 The master node will wait for the datafeed to be started by checking the persistent task's status and then return.
 To ensure that a subsequent stop datafeed call will see that same task status (and sanity validation doesn't fail)
 both start and stop datafeed apis redirect to the elected master node.
 In case of instability persistent tasks checks may fail and that is ok, in that case all bets are off.
 The start datafeed api is a low through put api, so the fact that we redirect to elected master node shouldn't be an issue.
 */
public class TransportStartDatafeedAction extends TransportMasterNodeAction<StartDatafeedAction.Request, NodeAcknowledgedResponse> {

    private static final Logger logger = LogManager.getLogger(TransportStartDatafeedAction.class);

    private final Client client;
    private final XPackLicenseState licenseState;
    private final PersistentTasksService persistentTasksService;
    private final JobConfigProvider jobConfigProvider;
    private final DatafeedConfigProvider datafeedConfigProvider;
    private final AnomalyDetectionAuditor auditor;
    private final NamedXContentRegistry xContentRegistry;
    private final boolean remoteClusterClient;

    @Inject
    public TransportStartDatafeedAction(
        Settings settings,
        TransportService transportService,
        ThreadPool threadPool,
        ClusterService clusterService,
        XPackLicenseState licenseState,
        PersistentTasksService persistentTasksService,
        ActionFilters actionFilters,
        Client client,
        JobConfigProvider jobConfigProvider,
        DatafeedConfigProvider datafeedConfigProvider,
        AnomalyDetectionAuditor auditor,
        NamedXContentRegistry xContentRegistry
    ) {
        super(
            StartDatafeedAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            StartDatafeedAction.Request::new,
            NodeAcknowledgedResponse::new,
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );
        this.licenseState = licenseState;
        this.persistentTasksService = persistentTasksService;
        this.client = client;
        this.jobConfigProvider = jobConfigProvider;
        this.datafeedConfigProvider = datafeedConfigProvider;
        this.auditor = auditor;
        this.xContentRegistry = xContentRegistry;
        this.remoteClusterClient = DiscoveryNode.isRemoteClusterClient(settings);
    }

    static void validate(
        Job job,
        DatafeedConfig datafeedConfig,
        PersistentTasksCustomMetadata tasks,
        NamedXContentRegistry xContentRegistry
    ) {
        DatafeedJobValidator.validate(datafeedConfig, job, xContentRegistry);
        DatafeedConfig.validateAggregations(datafeedConfig.getParsedAggregations(xContentRegistry));
        JobState jobState = MlTasks.getJobState(datafeedConfig.getJobId(), tasks);
        if (jobState.isAnyOf(JobState.OPENING, JobState.OPENED) == false) {
            throw ExceptionsHelper.conflictStatusException(
                "cannot start datafeed [" + datafeedConfig.getId() + "] because job [" + job.getId() + "] is " + jobState
            );
        }
    }

    // Get the deprecation warnings from the parsed query and aggs to audit
    static void auditDeprecations(
        DatafeedConfig datafeed,
        Job job,
        AnomalyDetectionAuditor auditor,
        NamedXContentRegistry xContentRegistry
    ) {
        List<String> deprecationWarnings = new ArrayList<>();
        deprecationWarnings.addAll(datafeed.getAggDeprecations(xContentRegistry));
        deprecationWarnings.addAll(datafeed.getQueryDeprecations(xContentRegistry));
        if (deprecationWarnings.isEmpty() == false) {
            String msg = "datafeed ["
                + datafeed.getId()
                + "] configuration has deprecations. ["
                + Strings.collectionToDelimitedString(deprecationWarnings, ", ")
                + "]";
            auditor.warning(job.getId(), msg);
        }

    }

    @Override
    protected void masterOperation(
        Task task,
        StartDatafeedAction.Request request,
        ClusterState state,
        ActionListener<NodeAcknowledgedResponse> listener
    ) {
        StartDatafeedAction.DatafeedParams params = request.getParams();
        if (MachineLearningField.ML_API_FEATURE.check(licenseState) == false) {
            listener.onFailure(LicenseUtils.newComplianceException(XPackField.MACHINE_LEARNING));
            return;
        }

        // The datafeed will run its with the configured headers,
        // preserve the response headers.
        var responseHeaderPreservingListener = ContextPreservingActionListener.wrapPreservingContext(
            listener,
            threadPool.getThreadContext()
        );

        AtomicReference<DatafeedConfig> datafeedConfigHolder = new AtomicReference<>();
        PersistentTasksCustomMetadata tasks = state.getMetadata().getProject().custom(PersistentTasksCustomMetadata.TYPE);

        ActionListener<PersistentTasksCustomMetadata.PersistentTask<StartDatafeedAction.DatafeedParams>> waitForTaskListener =
            new ActionListener<>() {
                @Override
                public void onResponse(PersistentTasksCustomMetadata.PersistentTask<StartDatafeedAction.DatafeedParams> persistentTask) {
                    waitForDatafeedStarted(persistentTask.getId(), params, responseHeaderPreservingListener);
                }

                @Override
                public void onFailure(Exception e) {
                    if (ExceptionsHelper.unwrapCause(e) instanceof ResourceAlreadyExistsException) {
                        logger.debug("datafeed already started", e);
                        e = new ElasticsearchStatusException(
                            "cannot start datafeed [" + params.getDatafeedId() + "] because it has already been started",
                            RestStatus.CONFLICT
                        );
                    }
                    responseHeaderPreservingListener.onFailure(e);
                }
            };

        // Verify data extractor factory can be created, then start persistent task
        Consumer<Job> createDataExtractor = job -> {
            final List<String> remoteIndices = RemoteClusterLicenseChecker.remoteIndices(params.getDatafeedIndices());
            if (remoteIndices.isEmpty() == false) {
                final RemoteClusterLicenseChecker remoteClusterLicenseChecker = new RemoteClusterLicenseChecker(
                    client,
                    MachineLearningField.ML_API_FEATURE
                );
                remoteClusterLicenseChecker.checkRemoteClusterLicenses(
                    RemoteClusterLicenseChecker.remoteClusterAliases(
                        transportService.getRemoteClusterService().getRegisteredRemoteClusterNames(),
                        params.getDatafeedIndices()
                    ),
                    ActionListener.wrap(response -> {
                        if (response.isSuccess() == false) {
                            responseHeaderPreservingListener.onFailure(createUnlicensedError(params.getDatafeedId(), response));
                        } else if (remoteClusterClient == false) {
                            responseHeaderPreservingListener.onFailure(
                                ExceptionsHelper.badRequestException(
                                    Messages.getMessage(
                                        Messages.DATAFEED_NEEDS_REMOTE_CLUSTER_SEARCH,
                                        datafeedConfigHolder.get().getId(),
                                        RemoteClusterLicenseChecker.remoteIndices(datafeedConfigHolder.get().getIndices()),
                                        clusterService.getNodeName()
                                    )
                                )
                            );
                        } else {
                            final RemoteClusterService remoteClusterService = transportService.getRemoteClusterService();
                            List<String> remoteAliases = RemoteClusterLicenseChecker.remoteClusterAliases(
                                remoteClusterService.getRegisteredRemoteClusterNames(),
                                remoteIndices
                            );
                            checkRemoteConfigVersions(
                                datafeedConfigHolder.get(),
                                remoteAliases,
                                (cn) -> remoteClusterService.getConnection(cn).getTransportVersion()
                            );
                            createDataExtractor(task, job, datafeedConfigHolder.get(), params, waitForTaskListener);
                        }
                    },
                        e -> responseHeaderPreservingListener.onFailure(
                            createUnknownLicenseError(
                                params.getDatafeedId(),
                                RemoteClusterLicenseChecker.remoteIndices(params.getDatafeedIndices()),
                                e
                            )
                        )
                    )
                );
            } else {
                createDataExtractor(task, job, datafeedConfigHolder.get(), params, waitForTaskListener);
            }
        };

        ActionListener<Job.Builder> jobListener = ActionListener.wrap(jobBuilder -> {
            Job job = jobBuilder.build();
            validate(job, datafeedConfigHolder.get(), tasks, xContentRegistry);
            auditDeprecations(datafeedConfigHolder.get(), job, auditor, xContentRegistry);
            createDataExtractor.accept(job);
        }, responseHeaderPreservingListener::onFailure);

        ActionListener<DatafeedConfig.Builder> datafeedListener = ActionListener.wrap(datafeedBuilder -> {
            DatafeedConfig datafeedConfig = datafeedBuilder.build();
            params.setDatafeedIndices(datafeedConfig.getIndices());
            params.setJobId(datafeedConfig.getJobId());
            params.setIndicesOptions(datafeedConfig.getIndicesOptions());
            datafeedConfigHolder.set(datafeedConfig);

            jobConfigProvider.getJob(datafeedConfig.getJobId(), null, jobListener);
        }, responseHeaderPreservingListener::onFailure);

        datafeedConfigProvider.getDatafeedConfig(params.getDatafeedId(), null, datafeedListener);
    }

    static void checkRemoteConfigVersions(
        DatafeedConfig config,
        List<String> remoteClusters,
        Function<String, TransportVersion> transportVersionSupplier
    ) {
        Optional<Tuple<TransportVersion, String>> minVersionAndReason = config.minRequiredTransportVersion();
        if (minVersionAndReason.isPresent() == false) {
            return;
        }
        final String reason = minVersionAndReason.get().v2();
        final TransportVersion minVersion = minVersionAndReason.get().v1();

        List<String> clustersTooOld = remoteClusters.stream()
            .filter(cn -> transportVersionSupplier.apply(cn).before(minVersion))
            .collect(Collectors.toList());
        if (clustersTooOld.isEmpty()) {
            return;
        }

        throw ExceptionsHelper.badRequestException(
            Messages.getMessage(
                REMOTE_CLUSTERS_TRANSPORT_TOO_OLD,
                minVersion.toReleaseVersion(),
                reason,
                Strings.collectionToCommaDelimitedString(clustersTooOld)
            )
        );
    }

    /** Creates {@link DataExtractorFactory} solely for the purpose of validation i.e. verifying that it can be created. */
    private void createDataExtractor(
        Task task,
        Job job,
        DatafeedConfig datafeed,
        StartDatafeedAction.DatafeedParams params,
        ActionListener<PersistentTasksCustomMetadata.PersistentTask<StartDatafeedAction.DatafeedParams>> listener
    ) {
        DataExtractorFactory.create(
            new ParentTaskAssigningClient(client, clusterService.localNode(), task),
            datafeed,
            job,
            xContentRegistry,
            // Fake DatafeedTimingStatsReporter that does not have access to results index
            new DatafeedTimingStatsReporter(new DatafeedTimingStats(job.getId()), (ts, refreshPolicy, listener1) -> {}),
            ActionListener.wrap(
                unused -> persistentTasksService.sendStartRequest(
                    MlTasks.datafeedTaskId(params.getDatafeedId()),
                    MlTasks.DATAFEED_TASK_NAME,
                    params,
                    MachineLearning.HARD_CODED_MACHINE_LEARNING_MASTER_NODE_TIMEOUT,
                    listener
                ),
                listener::onFailure
            )
        );
    }

    @Override
    protected ClusterBlockException checkBlock(StartDatafeedAction.Request request, ClusterState state) {
        // We only delegate here to PersistentTasksService, but if there is a metadata writeblock,
        // then delagating to PersistentTasksService doesn't make a whole lot of sense,
        // because PersistentTasksService will then fail.
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }

    private void waitForDatafeedStarted(
        String taskId,
        StartDatafeedAction.DatafeedParams params,
        ActionListener<NodeAcknowledgedResponse> listener
    ) {
        DatafeedPredicate predicate = new DatafeedPredicate();
        persistentTasksService.waitForPersistentTaskCondition(
            taskId,
            predicate,
            params.getTimeout(),
            new PersistentTasksService.WaitForPersistentTaskListener<StartDatafeedAction.DatafeedParams>() {
                @Override
                public void onResponse(PersistentTasksCustomMetadata.PersistentTask<StartDatafeedAction.DatafeedParams> persistentTask) {
                    if (predicate.exception != null) {
                        // We want to return to the caller without leaving an unassigned persistent task, to match
                        // what would have happened if the error had been detected in the "fast fail" validation
                        cancelDatafeedStart(persistentTask, predicate.exception, listener);
                    } else {
                        listener.onResponse(new NodeAcknowledgedResponse(true, predicate.node));
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    listener.onFailure(e);
                }

                @Override
                public void onTimeout(TimeValue timeout) {
                    listener.onFailure(
                        new ElasticsearchStatusException(
                            "Starting datafeed [{}] timed out after [{}]",
                            RestStatus.REQUEST_TIMEOUT,
                            params.getDatafeedId(),
                            timeout
                        )
                    );
                }
            }
        );
    }

    private void cancelDatafeedStart(
        PersistentTasksCustomMetadata.PersistentTask<StartDatafeedAction.DatafeedParams> persistentTask,
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
                        "["
                            + persistentTask.getParams().getDatafeedId()
                            + "] Failed to cancel persistent task that could "
                            + "not be assigned due to ["
                            + exception.getMessage()
                            + "]",
                        e
                    );
                    listener.onFailure(exception);
                }
            }
        );
    }

    private static ElasticsearchStatusException createUnlicensedError(
        final String datafeedId,
        final RemoteClusterLicenseChecker.LicenseCheck licenseCheck
    ) {
        final String message = String.format(
            Locale.ROOT,
            "cannot start datafeed [%s] as it is configured to use indices on remote cluster [%s] that is not licensed for ml; %s",
            datafeedId,
            licenseCheck.remoteClusterLicenseInfo().clusterAlias(),
            RemoteClusterLicenseChecker.buildErrorMessage(MachineLearningField.ML_API_FEATURE, licenseCheck.remoteClusterLicenseInfo())
        );
        return new ElasticsearchStatusException(message, RestStatus.BAD_REQUEST);
    }

    private ElasticsearchStatusException createUnknownLicenseError(
        final String datafeedId,
        final List<String> remoteIndices,
        final Exception cause
    ) {
        final int numberOfRemoteClusters = RemoteClusterLicenseChecker.remoteClusterAliases(
            transportService.getRemoteClusterService().getRegisteredRemoteClusterNames(),
            remoteIndices
        ).size();
        assert numberOfRemoteClusters > 0;
        final String remoteClusterQualifier = numberOfRemoteClusters == 1 ? "a remote cluster" : "remote clusters";
        final String licenseTypeQualifier = numberOfRemoteClusters == 1 ? "" : "s";
        final String message = String.format(
            Locale.ROOT,
            "cannot start datafeed [%s] as it uses indices on %s %s but the license type%s could not be verified",
            datafeedId,
            remoteClusterQualifier,
            remoteIndices,
            licenseTypeQualifier
        );

        return new ElasticsearchStatusException(message, RestStatus.BAD_REQUEST, cause);
    }

    public static class StartDatafeedPersistentTasksExecutor extends PersistentTasksExecutor<StartDatafeedAction.DatafeedParams> {
        private final DatafeedRunner datafeedRunner;
        private final IndexNameExpressionResolver resolver;

        public StartDatafeedPersistentTasksExecutor(
            DatafeedRunner datafeedRunner,
            IndexNameExpressionResolver resolver,
            ThreadPool threadPool
        ) {
            super(MlTasks.DATAFEED_TASK_NAME, threadPool.executor(MachineLearning.UTILITY_THREAD_POOL_NAME));
            this.datafeedRunner = datafeedRunner;
            this.resolver = resolver;
        }

        @Override
        public PersistentTasksCustomMetadata.Assignment getAssignment(
            StartDatafeedAction.DatafeedParams params,
            Collection<DiscoveryNode> candidateNodes,
            ClusterState clusterState
        ) {
            return new DatafeedNodeSelector(
                clusterState,
                resolver,
                params.getDatafeedId(),
                params.getJobId(),
                params.getDatafeedIndices(),
                params.getIndicesOptions()
            ).selectNode(candidateNodes);
        }

        @Override
        public void validate(StartDatafeedAction.DatafeedParams params, ClusterState clusterState) {
            new DatafeedNodeSelector(
                clusterState,
                resolver,
                params.getDatafeedId(),
                params.getJobId(),
                params.getDatafeedIndices(),
                params.getIndicesOptions()
            ).checkDatafeedTaskCanBeCreated();
        }

        @Override
        protected void nodeOperation(
            final AllocatedPersistentTask allocatedPersistentTask,
            final StartDatafeedAction.DatafeedParams params,
            final PersistentTaskState state
        ) {
            DatafeedTask datafeedTask = (DatafeedTask) allocatedPersistentTask;
            DatafeedState datafeedState = (DatafeedState) state;

            // If we are stopping, stopped or isolated we should not start the runner. Due to
            // races in the way messages pass between nodes via cluster state or direct action calls
            // we need to detect stopped/stopping by both considering the persistent task state in
            // cluster state and also whether an explicit request to stop has been received on this
            // node.
            if (DatafeedState.STOPPING.equals(datafeedState)) {
                logger.info("[{}] datafeed got reassigned while stopping. Marking as completed", params.getDatafeedId());
                datafeedTask.completeOrFailIfRequired(null);
                return;
            }
            switch (datafeedTask.setDatafeedRunner(datafeedRunner)) {
                case NEITHER -> datafeedRunner.run(datafeedTask, datafeedTask::completeOrFailIfRequired);
                case ISOLATED -> logger.info("[{}] datafeed isolated immediately after reassignment.", params.getDatafeedId());
                case STOPPED -> {
                    logger.info("[{}] datafeed stopped immediately after reassignment. Marking as completed", params.getDatafeedId());
                    datafeedTask.completeOrFailIfRequired(null);
                }
            }
        }

        @Override
        protected AllocatedPersistentTask createTask(
            long id,
            String type,
            String action,
            TaskId parentTaskId,
            PersistentTasksCustomMetadata.PersistentTask<StartDatafeedAction.DatafeedParams> persistentTask,
            Map<String, String> headers
        ) {
            return new DatafeedTask(id, type, action, parentTaskId, persistentTask.getParams(), headers);
        }
    }

    public static class DatafeedTask extends AllocatedPersistentTask implements StartDatafeedAction.DatafeedTaskMatcher {

        public enum StoppedOrIsolated {
            NEITHER,
            ISOLATED,
            STOPPED
        }

        private final String datafeedId;
        private final long startTime;
        private final Long endTime;
        /**
         * This must always be set within a synchronized block that also checks
         * the value of the {@code stoppedOrIsolatedBeforeRunning} flag.
         */
        private DatafeedRunner datafeedRunner;
        private StoppedOrIsolated stoppedOrIsolated = StoppedOrIsolated.NEITHER;

        DatafeedTask(
            long id,
            String type,
            String action,
            TaskId parentTaskId,
            StartDatafeedAction.DatafeedParams params,
            Map<String, String> headers
        ) {
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

        /**
         * Set the datafeed runner <em>if</em> the task has not already been told to stop or isolate.
         * @return A {@link StoppedOrIsolated} object that indicates whether the
         *         datafeed task had previously been told to stop or isolate.  {@code datafeedRunner}
         *         will only be set to the supplied value if the return value of this method is
         *         {@link StoppedOrIsolated#NEITHER}.
         */
        StoppedOrIsolated setDatafeedRunner(DatafeedRunner datafeedRunner) {
            return executeIfNotStoppedOrIsolated(() -> this.datafeedRunner = Objects.requireNonNull(datafeedRunner));
        }

        /**
         * Run a command <em>if</em> the task has not already been told to stop or isolate.
         * @param runnable The command to run.
         * @return A {@link StoppedOrIsolated} object that indicates whether the datafeed task
         *         had previously been told to stop or isolate. {@code runnable} will only be
         *         run if the return value of this method is {@link StoppedOrIsolated#NEITHER}.
         */
        public synchronized StoppedOrIsolated executeIfNotStoppedOrIsolated(Runnable runnable) {
            if (stoppedOrIsolated == StoppedOrIsolated.NEITHER) {
                runnable.run();
            }
            return stoppedOrIsolated;
        }

        @Override
        protected void onCancelled() {
            // If the persistent task framework wants us to stop then we should do so immediately and
            // we should wait for an existing datafeed import to realize we want it to stop.
            // Note that this only applied when task cancel is invoked and stop datafeed api doesn't use this.
            // Also stop datafeed api will obey the timeout.
            stop(getReasonCancelled(), TimeValue.ZERO);
        }

        @Override
        public boolean shouldCancelChildrenOnCancellation() {
            // onCancelled implements graceful shutdown of children
            return false;
        }

        public void stop(String reason, TimeValue timeout) {
            synchronized (this) {
                stoppedOrIsolated = StoppedOrIsolated.STOPPED;
                if (datafeedRunner == null) {
                    return;
                }
            }
            datafeedRunner.stopDatafeed(this, reason, timeout);
        }

        public synchronized StoppedOrIsolated getStoppedOrIsolated() {
            return stoppedOrIsolated;
        }

        public void isolate() {
            synchronized (this) {
                // Stopped takes precedence over isolated for what we report externally,
                // as stopped needs to cause the persistent task to be marked as completed
                // (regardless of whether it was isolated) whereas isolated but not stopped
                // mustn't do this.
                if (stoppedOrIsolated == StoppedOrIsolated.NEITHER) {
                    stoppedOrIsolated = StoppedOrIsolated.ISOLATED;
                }
                if (datafeedRunner == null) {
                    return;
                }
            }
            datafeedRunner.isolateDatafeed(this);
        }

        void completeOrFailIfRequired(Exception error) {
            // A task can only be completed or failed once - trying multiple times just causes log spam
            if (isCompleted()) {
                return;
            }
            if (error != null) {
                markAsFailed(error);
            } else {
                markAsCompleted();
            }
        }

        public GetDatafeedRunningStateAction.Response.RunningState getRunningState() {
            synchronized (this) {
                if (datafeedRunner == null) {
                    // In this case we don't know for sure if lookback has completed. It may be that the
                    // datafeed has just moved nodes, but with so little delay that there's no lookback to
                    // do on the new node. However, there _might_ be some catching up required, so it's
                    // reasonable to say real-time running hasn't started yet. The state will quickly
                    // change once the datafeed runner gets going and determines where the datafeed is up
                    // to.
                    return new GetDatafeedRunningStateAction.Response.RunningState(endTime == null, false, null);
                }
            }
            return new GetDatafeedRunningStateAction.Response.RunningState(
                endTime == null,
                datafeedRunner.finishedLookBack(this),
                datafeedRunner.getSearchInterval(this)
            );
        }
    }

    /**
     * Important: the methods of this class must NOT throw exceptions.  If they did then the callers
     * of endpoints waiting for a condition tested by this predicate would never get a response.
     */
    private static class DatafeedPredicate implements Predicate<PersistentTasksCustomMetadata.PersistentTask<?>> {

        private volatile Exception exception;
        private volatile String node = "";

        @Override
        public boolean test(PersistentTasksCustomMetadata.PersistentTask<?> persistentTask) {
            if (persistentTask == null) {
                return false;
            }
            PersistentTasksCustomMetadata.Assignment assignment = persistentTask.getAssignment();
            if (assignment != null) {
                // This means we are awaiting the datafeed's job to be assigned to a node
                if (assignment.equals(DatafeedNodeSelector.AWAITING_JOB_ASSIGNMENT)) {
                    return true;
                }
                // This means the node the job got assigned to was shut down in between starting the job and the datafeed - not an error
                if (assignment.equals(DatafeedNodeSelector.AWAITING_JOB_RELOCATION)) {
                    return true;
                }
                if (assignment.equals(PersistentTasksCustomMetadata.INITIAL_ASSIGNMENT) == false && assignment.isAssigned() == false) {
                    // Assignment has failed despite passing our "fast fail" validation
                    exception = new ElasticsearchStatusException(
                        "Could not start datafeed, allocation explanation [" + assignment.getExplanation() + "]",
                        RestStatus.TOO_MANY_REQUESTS
                    );
                    return true;
                }
            }
            DatafeedState datafeedState = (DatafeedState) persistentTask.getState();
            if (datafeedState == DatafeedState.STARTED) {
                node = persistentTask.getExecutorNode();
                return true;
            }
            return false;
        }
    }
}
