/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.action;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.license.LicenseUtils;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.XpackField;
import org.elasticsearch.xpack.ml.MLMetadataField;
import org.elasticsearch.xpack.ml.MachineLearning;
import org.elasticsearch.xpack.ml.MlMetadata;
import org.elasticsearch.xpack.ml.datafeed.DatafeedConfig;
import org.elasticsearch.xpack.ml.datafeed.DatafeedJobValidator;
import org.elasticsearch.xpack.ml.datafeed.DatafeedManager;
import org.elasticsearch.xpack.ml.datafeed.DatafeedNodeSelector;
import org.elasticsearch.xpack.ml.datafeed.DatafeedState;
import org.elasticsearch.xpack.ml.datafeed.extractor.DataExtractorFactory;
import org.elasticsearch.xpack.ml.job.config.Job;
import org.elasticsearch.xpack.ml.job.config.JobState;
import org.elasticsearch.xpack.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.persistent.AllocatedPersistentTask;
import org.elasticsearch.xpack.persistent.PersistentTasksCustomMetaData;
import org.elasticsearch.xpack.persistent.PersistentTasksExecutor;
import org.elasticsearch.xpack.persistent.PersistentTasksService;

import java.util.function.Predicate;

/* This class extends from TransportMasterNodeAction for cluster state observing purposes.
 The stop datafeed api also redirect the elected master node.
 The master node will wait for the datafeed to be started by checking the persistent task's status and then return.
 To ensure that a subsequent stop datafeed call will see that same task status (and sanity validation doesn't fail)
 both start and stop datafeed apis redirect to the elected master node.
 In case of instability persistent tasks checks may fail and that is ok, in that case all bets are off.
 The start datafeed api is a low through put api, so the fact that we redirect to elected master node shouldn't be an issue.
 */
public class TransportStartDatafeedAction extends TransportMasterNodeAction<StartDatafeedAction.Request, StartDatafeedAction.Response> {

    private final Client client;
    private final XPackLicenseState licenseState;
    private final PersistentTasksService persistentTasksService;

    @Inject
    public TransportStartDatafeedAction(Settings settings, TransportService transportService, ThreadPool threadPool,
                                        ClusterService clusterService, XPackLicenseState licenseState,
                                        PersistentTasksService persistentTasksService,
                                        ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver,
                                        Client client) {
        super(settings, StartDatafeedAction.NAME, transportService, clusterService, threadPool, actionFilters, indexNameExpressionResolver,
                StartDatafeedAction.Request::new);
        this.licenseState = licenseState;
        this.persistentTasksService = persistentTasksService;
        this.client = client;
    }

    static void validate(String datafeedId, MlMetadata mlMetadata, PersistentTasksCustomMetaData tasks) {
        DatafeedConfig datafeed = (mlMetadata == null) ? null : mlMetadata.getDatafeed(datafeedId);
        if (datafeed == null) {
            throw ExceptionsHelper.missingDatafeedException(datafeedId);
        }
        Job job = mlMetadata.getJobs().get(datafeed.getJobId());
        if (job == null) {
            throw ExceptionsHelper.missingJobException(datafeed.getJobId());
        }
        DatafeedJobValidator.validate(datafeed, job);
        JobState jobState = MlMetadata.getJobState(datafeed.getJobId(), tasks);
        if (jobState.isAnyOf(JobState.OPENING, JobState.OPENED) == false) {
            throw ExceptionsHelper.conflictStatusException("cannot start datafeed [" + datafeedId + "] because job [" + job.getId() +
                    "] is " + jobState);
        }
    }

    @Override
    protected String executor() {
        // This api doesn't do heavy or blocking operations (just delegates PersistentTasksService),
        // so we can do this on the network thread
        return ThreadPool.Names.SAME;
    }

    @Override
    protected StartDatafeedAction.Response newResponse() {
        return new StartDatafeedAction.Response();
    }

    @Override
    protected void masterOperation(StartDatafeedAction.Request request, ClusterState state,
                                   ActionListener<StartDatafeedAction.Response> listener) {
        StartDatafeedAction.DatafeedParams params = request.getParams();
        if (licenseState.isMachineLearningAllowed()) {
            ActionListener<PersistentTasksCustomMetaData.PersistentTask<StartDatafeedAction.DatafeedParams>> finalListener =
                    new ActionListener<PersistentTasksCustomMetaData.PersistentTask<StartDatafeedAction.DatafeedParams>>() {
                @Override
                public void onResponse(PersistentTasksCustomMetaData.PersistentTask<StartDatafeedAction.DatafeedParams> persistentTask) {
                    waitForDatafeedStarted(persistentTask.getId(), params, listener);
                }

                @Override
                public void onFailure(Exception e) {
                    if (e instanceof ResourceAlreadyExistsException) {
                        logger.debug(e);
                        e = new ElasticsearchStatusException("cannot start datafeed [" + params.getDatafeedId() +
                                "] because it has already been started", RestStatus.CONFLICT);
                    }
                    listener.onFailure(e);
                }
            };

            // Verify data extractor factory can be created, then start persistent task
            MlMetadata mlMetadata = state.metaData().custom(MLMetadataField.TYPE);
            PersistentTasksCustomMetaData tasks = state.getMetaData().custom(PersistentTasksCustomMetaData.TYPE);
            validate(params.getDatafeedId(), mlMetadata, tasks);
            DatafeedConfig datafeed = mlMetadata.getDatafeed(params.getDatafeedId());
            Job job = mlMetadata.getJobs().get(datafeed.getJobId());
            DataExtractorFactory.create(client, datafeed, job, ActionListener.wrap(
                    dataExtractorFactory ->
                            persistentTasksService.startPersistentTask(MLMetadataField.datafeedTaskId(params.getDatafeedId()),
                            StartDatafeedAction.TASK_NAME, params, finalListener)
                    , listener::onFailure));
        } else {
            listener.onFailure(LicenseUtils.newComplianceException(XpackField.MACHINE_LEARNING));
        }
    }

    @Override
    protected ClusterBlockException checkBlock(StartDatafeedAction.Request request, ClusterState state) {
        // We only delegate here to PersistentTasksService, but if there is a metadata writeblock,
        // then delagating to PersistentTasksService doesn't make a whole lot of sense,
        // because PersistentTasksService will then fail.
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }

    void waitForDatafeedStarted(String taskId, StartDatafeedAction.DatafeedParams params,
                                ActionListener<StartDatafeedAction.Response> listener) {
        Predicate<PersistentTasksCustomMetaData.PersistentTask<?>> predicate = persistentTask -> {
            if (persistentTask == null) {
                return false;
            }
            DatafeedState datafeedState = (DatafeedState) persistentTask.getStatus();
            return datafeedState == DatafeedState.STARTED;
        };
        persistentTasksService.waitForPersistentTaskStatus(taskId, predicate, params.getTimeout(),
                new PersistentTasksService.WaitForPersistentTaskStatusListener<StartDatafeedAction.DatafeedParams>() {
            @Override
            public void onResponse(PersistentTasksCustomMetaData.PersistentTask<StartDatafeedAction.DatafeedParams> task) {
                listener.onResponse(new StartDatafeedAction.Response(true));
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

    public static class StartDatafeedPersistentTasksExecutor extends PersistentTasksExecutor<StartDatafeedAction.DatafeedParams> {
        private final DatafeedManager datafeedManager;
        private final IndexNameExpressionResolver resolver;

        public StartDatafeedPersistentTasksExecutor(Settings settings, DatafeedManager datafeedManager) {
            super(settings, StartDatafeedAction.TASK_NAME, MachineLearning.UTILITY_THREAD_POOL_NAME);
            this.datafeedManager = datafeedManager;
            this.resolver = new IndexNameExpressionResolver(settings);
        }

        @Override
        public PersistentTasksCustomMetaData.Assignment getAssignment(StartDatafeedAction.DatafeedParams params,
                                                                      ClusterState clusterState) {
            return new DatafeedNodeSelector(clusterState, resolver, params.getDatafeedId()).selectNode();
        }

        @Override
        public void validate(StartDatafeedAction.DatafeedParams params, ClusterState clusterState) {
            MlMetadata mlMetadata = clusterState.metaData().custom(MLMetadataField.TYPE);
            PersistentTasksCustomMetaData tasks = clusterState.getMetaData().custom(PersistentTasksCustomMetaData.TYPE);
            TransportStartDatafeedAction.validate(params.getDatafeedId(), mlMetadata, tasks);
            new DatafeedNodeSelector(clusterState, resolver, params.getDatafeedId()).checkDatafeedTaskCanBeCreated();
        }

        @Override
        protected void nodeOperation(AllocatedPersistentTask allocatedPersistentTask, StartDatafeedAction.DatafeedParams params,
                                     Task.Status status) {
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
                PersistentTasksCustomMetaData.PersistentTask<StartDatafeedAction.DatafeedParams> persistentTask) {
            return new DatafeedTask(id, type, action, parentTaskId, persistentTask.getParams());
        }
    }

    public static class DatafeedTask extends AllocatedPersistentTask implements StartDatafeedAction.DatafeedTaskMatcher {

        private final String datafeedId;
        private final long startTime;
        private final Long endTime;
        /* only pck protected for testing */
        volatile DatafeedManager datafeedManager;

        DatafeedTask(long id, String type, String action, TaskId parentTaskId, StartDatafeedAction.DatafeedParams params) {
            super(id, type, action, "datafeed-" + params.getDatafeedId(), parentTaskId);
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
}
