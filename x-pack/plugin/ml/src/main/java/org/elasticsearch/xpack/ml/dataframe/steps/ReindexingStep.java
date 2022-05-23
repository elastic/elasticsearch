/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.dataframe.steps;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.node.tasks.cancel.CancelTasksRequest;
import org.elasticsearch.action.admin.cluster.node.tasks.cancel.CancelTasksResponse;
import org.elasticsearch.action.admin.cluster.node.tasks.get.GetTaskRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.get.GetIndexAction;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.action.admin.indices.get.GetIndexResponse;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.support.ContextPreservingActionListener;
import org.elasticsearch.client.internal.ParentTaskAssigningClient;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.mapper.SeqNoFieldMapper;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.BulkByScrollTask;
import org.elasticsearch.index.reindex.ReindexAction;
import org.elasticsearch.index.reindex.ReindexRequest;
import org.elasticsearch.script.Script;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.tasks.TaskResult;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsConfig;
import org.elasticsearch.xpack.core.ml.job.messages.Messages;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.ml.dataframe.DataFrameAnalyticsTask;
import org.elasticsearch.xpack.ml.dataframe.DestinationIndex;
import org.elasticsearch.xpack.ml.notifications.DataFrameAnalyticsAuditor;

import java.time.Clock;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;

import static org.elasticsearch.xpack.core.ClientHelper.ML_ORIGIN;

public class ReindexingStep extends AbstractDataFrameAnalyticsStep {

    private static final Logger LOGGER = LogManager.getLogger(ReindexingStep.class);

    private final ClusterService clusterService;
    @Nullable
    private volatile Long reindexingTaskId;
    private volatile boolean isReindexingFinished;

    public ReindexingStep(
        ClusterService clusterService,
        NodeClient client,
        DataFrameAnalyticsTask task,
        DataFrameAnalyticsAuditor auditor,
        DataFrameAnalyticsConfig config
    ) {
        super(client, task, auditor, config);
        this.clusterService = Objects.requireNonNull(clusterService);
    }

    @Override
    public Name name() {
        return Name.REINDEXING;
    }

    @Override
    protected void doExecute(ActionListener<StepResponse> listener) {
        task.getStatsHolder().getProgressTracker().updateReindexingProgress(1);

        final ParentTaskAssigningClient parentTaskClient = parentTaskClient();

        // Reindexing is complete
        ActionListener<BulkByScrollResponse> reindexCompletedListener = ActionListener.wrap(reindexResponse -> {

            // If the reindex task is canceled, this listener is called.
            // Consequently, we should not signal reindex completion.
            if (isTaskStopping()) {
                LOGGER.debug("[{}] task is stopping. Stopping reindexing before it is finished.", config.getId());
                listener.onResponse(new StepResponse(true));
                return;
            }

            synchronized (this) {
                reindexingTaskId = null;
            }

            Exception reindexError = getReindexError(config.getId(), reindexResponse);
            if (reindexError != null) {
                listener.onFailure(reindexError);
                return;
            }

            auditor.info(
                config.getId(),
                Messages.getMessage(
                    Messages.DATA_FRAME_ANALYTICS_AUDIT_FINISHED_REINDEXING,
                    config.getDest().getIndex(),
                    reindexResponse.getTook()
                )
            );

            isReindexingFinished = true;
            task.getStatsHolder().getProgressTracker().updateReindexingProgress(100);

            LOGGER.debug(
                "[{}] Reindex completed; created [{}]; retries [{}]",
                config.getId(),
                reindexResponse.getCreated(),
                reindexResponse.getBulkRetries()
            );

            listener.onResponse(new StepResponse(false));
        }, error -> {
            if (isTaskStopping() && isTaskCancelledException(error)) {
                LOGGER.debug(() -> "[" + config.getId() + "] Caught task cancelled exception while task is stopping", error);
                listener.onResponse(new StepResponse(true));
            } else {
                listener.onFailure(error);
            }
        });

        // Reindex
        ActionListener<CreateIndexResponse> copyIndexCreatedListener = ActionListener.wrap(createIndexResponse -> {
            ReindexRequest reindexRequest = new ReindexRequest();
            reindexRequest.setRefresh(true);
            reindexRequest.setSourceIndices(config.getSource().getIndex());
            reindexRequest.setSourceQuery(config.getSource().getParsedQuery());
            reindexRequest.getSearchRequest().allowPartialSearchResults(false);
            reindexRequest.getSearchRequest().source().fetchSource(config.getSource().getSourceFiltering());
            reindexRequest.getSearchRequest().source().sort(SeqNoFieldMapper.NAME, SortOrder.ASC);
            reindexRequest.setDestIndex(config.getDest().getIndex());

            // We explicitly set slices to 1 as we cannot parallelize in order to have the incremental id
            reindexRequest.setSlices(1);
            Map<String, Object> counterValueParam = new HashMap<>();
            counterValueParam.put("value", -1);
            reindexRequest.setScript(
                new Script(
                    Script.DEFAULT_SCRIPT_TYPE,
                    Script.DEFAULT_SCRIPT_LANG,
                    // We use indirection here because top level params are immutable.
                    // This is a work around at the moment but the plan is to make this a feature of reindex API.
                    "ctx._source." + DestinationIndex.INCREMENTAL_ID + " = ++params.counter.value",
                    Collections.singletonMap("counter", counterValueParam)
                )
            );

            reindexRequest.setParentTask(getParentTaskId());

            final ThreadContext threadContext = parentTaskClient.threadPool().getThreadContext();
            final Supplier<ThreadContext.StoredContext> supplier = threadContext.newRestorableContext(false);
            try (ThreadContext.StoredContext ignore = threadContext.stashWithOrigin(ML_ORIGIN)) {
                synchronized (this) {
                    if (isTaskStopping()) {
                        LOGGER.debug("[{}] task is stopping. Stopping reindexing before it is finished.", config.getId());
                        listener.onResponse(new StepResponse(true));
                        return;
                    }
                    LOGGER.info("[{}] Started reindexing", config.getId());
                    Task reindexTask = client.executeLocally(
                        ReindexAction.INSTANCE,
                        reindexRequest,
                        new ContextPreservingActionListener<>(supplier, reindexCompletedListener)
                    );
                    reindexingTaskId = reindexTask.getId();
                }
                auditor.info(
                    config.getId(),
                    Messages.getMessage(Messages.DATA_FRAME_ANALYTICS_AUDIT_STARTED_REINDEXING, config.getDest().getIndex())
                );
            }
        }, reindexCompletedListener::onFailure);

        // Create destination index if it does not exist
        ActionListener<GetIndexResponse> destIndexListener = ActionListener.wrap(indexResponse -> {
            auditor.info(
                config.getId(),
                Messages.getMessage(Messages.DATA_FRAME_ANALYTICS_AUDIT_REUSING_DEST_INDEX, indexResponse.indices()[0])
            );
            LOGGER.info("[{}] Using existing destination index [{}]", config.getId(), indexResponse.indices()[0]);
            DestinationIndex.updateMappingsToDestIndex(
                parentTaskClient,
                config,
                indexResponse,
                ActionListener.wrap(acknowledgedResponse -> copyIndexCreatedListener.onResponse(null), copyIndexCreatedListener::onFailure)
            );
        }, e -> {
            if (ExceptionsHelper.unwrapCause(e) instanceof IndexNotFoundException) {
                auditor.info(
                    config.getId(),
                    Messages.getMessage(Messages.DATA_FRAME_ANALYTICS_AUDIT_CREATING_DEST_INDEX, config.getDest().getIndex())
                );
                LOGGER.info("[{}] Creating destination index [{}]", config.getId(), config.getDest().getIndex());
                DestinationIndex.createDestinationIndex(parentTaskClient, Clock.systemUTC(), config, copyIndexCreatedListener);
            } else {
                copyIndexCreatedListener.onFailure(e);
            }
        });

        ClientHelper.executeWithHeadersAsync(
            config.getHeaders(),
            ML_ORIGIN,
            parentTaskClient,
            GetIndexAction.INSTANCE,
            new GetIndexRequest().indices(config.getDest().getIndex()),
            destIndexListener
        );
    }

    private static Exception getReindexError(String jobId, BulkByScrollResponse reindexResponse) {
        if (reindexResponse.getBulkFailures().isEmpty() == false) {
            LOGGER.error("[{}] reindexing encountered {} failures", jobId, reindexResponse.getBulkFailures().size());
            for (BulkItemResponse.Failure failure : reindexResponse.getBulkFailures()) {
                LOGGER.error("[{}] reindexing failure: {}", jobId, failure);
            }
            return ExceptionsHelper.serverError("reindexing encountered " + reindexResponse.getBulkFailures().size() + " failures");
        }
        if (reindexResponse.getReasonCancelled() != null) {
            LOGGER.error("[{}] reindex task got cancelled with reason [{}]", jobId, reindexResponse.getReasonCancelled());
            return ExceptionsHelper.serverError("reindex task got cancelled with reason [" + reindexResponse.getReasonCancelled() + "]");
        }
        if (reindexResponse.isTimedOut()) {
            LOGGER.error("[{}] reindex task timed out after [{}]", jobId, reindexResponse.getTook().getStringRep());
            return ExceptionsHelper.serverError("reindex task timed out after [" + reindexResponse.getTook().getStringRep() + "]");
        }
        return null;
    }

    private static boolean isTaskCancelledException(Exception error) {
        return ExceptionsHelper.unwrapCause(error) instanceof TaskCancelledException
            || ExceptionsHelper.unwrapCause(error.getCause()) instanceof TaskCancelledException;
    }

    @Override
    public void cancel(String reason, TimeValue timeout) {
        TaskId reindexTaskId = null;
        synchronized (this) {
            if (reindexingTaskId != null) {
                reindexTaskId = new TaskId(clusterService.localNode().getId(), reindexingTaskId);
            }
        }
        if (reindexTaskId == null) {
            return;
        }

        LOGGER.debug("[{}] Cancelling reindex task [{}]", config.getId(), reindexTaskId);

        CancelTasksRequest cancelReindex = new CancelTasksRequest();
        cancelReindex.setTargetTaskId(reindexTaskId);
        cancelReindex.setReason(reason);
        cancelReindex.setTimeout(timeout);

        // We need to cancel the reindexing task within context with ML origin as we started the task
        // from the same context
        CancelTasksResponse cancelReindexResponse = cancelTaskWithinMlOriginContext(cancelReindex);

        Throwable firstError = null;
        if (cancelReindexResponse.getNodeFailures().isEmpty() == false) {
            firstError = cancelReindexResponse.getNodeFailures().get(0).getRootCause();
        }
        if (cancelReindexResponse.getTaskFailures().isEmpty() == false) {
            firstError = cancelReindexResponse.getTaskFailures().get(0).getCause();
        }
        // There is a chance that the task is finished by the time we cancel it in which case we'll get
        // a ResourceNotFoundException which we can ignore.
        if (firstError != null && ExceptionsHelper.unwrapCause(firstError) instanceof ResourceNotFoundException == false) {
            throw ExceptionsHelper.serverError("[" + config.getId() + "] Error cancelling reindex task", firstError);
        } else {
            LOGGER.debug("[{}] Reindex task was successfully cancelled", config.getId());
        }
    }

    private CancelTasksResponse cancelTaskWithinMlOriginContext(CancelTasksRequest cancelTasksRequest) {
        final ThreadContext threadContext = client.threadPool().getThreadContext();
        try (ThreadContext.StoredContext ignore = threadContext.stashWithOrigin(ML_ORIGIN)) {
            return client.admin().cluster().cancelTasks(cancelTasksRequest).actionGet();
        }
    }

    @Override
    public void updateProgress(ActionListener<Void> listener) {
        getReindexTaskProgress(
            ActionListener.wrap(
                // We set reindexing progress at least to 1 for a running process to be able to
                // distinguish a job that is running for the first time against a job that is restarting.
                reindexTaskProgress -> {
                    task.getStatsHolder().getProgressTracker().updateReindexingProgress(Math.max(1, reindexTaskProgress));
                    listener.onResponse(null);
                },
                listener::onFailure
            )
        );
    }

    private void getReindexTaskProgress(ActionListener<Integer> listener) {
        TaskId reindexTaskId = getReindexTaskId();
        if (reindexTaskId == null) {
            listener.onResponse(isReindexingFinished ? 100 : 0);
            return;
        }

        GetTaskRequest getTaskRequest = new GetTaskRequest();
        getTaskRequest.setTaskId(reindexTaskId);
        client.admin().cluster().getTask(getTaskRequest, ActionListener.wrap(taskResponse -> {
            TaskResult taskResult = taskResponse.getTask();
            BulkByScrollTask.Status taskStatus = (BulkByScrollTask.Status) taskResult.getTask().status();
            int progress = (int) (taskStatus.getCreated() * 100.0 / taskStatus.getTotal());
            listener.onResponse(progress);
        }, error -> {
            if (ExceptionsHelper.unwrapCause(error) instanceof ResourceNotFoundException) {
                // The task is not present which means either it has not started yet or it finished.
                listener.onResponse(isReindexingFinished ? 100 : 0);
            } else {
                listener.onFailure(error);
            }
        }));
    }

    @Nullable
    private TaskId getReindexTaskId() {
        try {
            return new TaskId(clusterService.localNode().getId(), reindexingTaskId);
        } catch (NullPointerException e) {
            // This may happen if there is no reindexing task id set which means we either never started the task yet or we're finished
            return null;
        }
    }
}
