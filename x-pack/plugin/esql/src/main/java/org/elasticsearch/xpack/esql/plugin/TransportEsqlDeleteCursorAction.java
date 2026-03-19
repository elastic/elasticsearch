/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.NoShardAvailableActionException;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.compute.operator.exchange.ExchangeService;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.tasks.TaskManager;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.esql.action.EsqlCursor;
import org.elasticsearch.xpack.esql.action.EsqlDeleteCursorAction;
import org.elasticsearch.xpack.esql.action.EsqlDeleteCursorRequest;

import java.util.concurrent.atomic.AtomicBoolean;

public class TransportEsqlDeleteCursorAction extends HandledTransportAction<EsqlDeleteCursorRequest, AcknowledgedResponse> {

    private static final Logger logger = LogManager.getLogger(TransportEsqlDeleteCursorAction.class);

    static final int MAX_DELETE_RETRIES = 5;
    static final TimeValue DELETE_RETRY_DELAY = TimeValue.timeValueSeconds(1);
    static final TimeValue CANCEL_TASK_TIMEOUT = TimeValue.timeValueSeconds(30);
    static final TimeValue METADATA_RETRY_DELAY = TimeValue.timeValueMillis(200);
    static final TimeValue METADATA_WAIT_TIMEOUT = TimeValue.timeValueSeconds(5);

    private final EsqlCursorIndexService cursorIndexService;
    private final Client client;
    private final ThreadPool threadPool;
    private final ExchangeService exchangeService;
    private final ClusterService clusterService;
    private final TransportService transportService;
    private final TaskManager taskManager;

    @Inject
    public TransportEsqlDeleteCursorAction(
        TransportService transportService,
        ActionFilters actionFilters,
        EsqlCursorIndexService cursorIndexService,
        Client client,
        ExchangeService exchangeService,
        ClusterService clusterService
    ) {
        super(
            EsqlDeleteCursorAction.NAME,
            transportService,
            actionFilters,
            EsqlDeleteCursorRequest::new,
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );
        this.cursorIndexService = cursorIndexService;
        this.client = client;
        this.threadPool = transportService.getThreadPool();
        this.exchangeService = exchangeService;
        this.clusterService = clusterService;
        this.transportService = transportService;
        this.taskManager = transportService.getTaskManager();
    }

    @Override
    protected void doExecute(Task task, EsqlDeleteCursorRequest request, ActionListener<AcknowledgedResponse> listener) {
        try {
            EsqlCursor cursor = EsqlCursor.decode(request.cursor());
            String cursorId = cursor.cursorId();
            boolean waitForCompletion = request.waitForCompletion();
            logger.info("delete cursor [{}]: waitForCompletion=[{}]", cursorId, waitForCompletion);
            long deadlineMillis = threadPool.relativeTimeInMillis() + METADATA_WAIT_TIMEOUT.millis();
            if (waitForCompletion) {
                cancelThenDelete(cursorId, deadlineMillis, listener);
            } else {
                listener.onResponse(AcknowledgedResponse.TRUE);
                threadPool.generic().execute(() -> cancelThenDelete(cursorId, deadlineMillis, ActionListener.wrap(v -> {}, e -> {
                    logger.warn("background cursor cleanup failed for cursor [{}]", cursorId, e);
                })));
            }
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    /**
     * Reads the cursor metadata to find the background task ID, gracefully stops the compute
     * (waiting for it to finish), then deletes all cursor documents.
     * <p>
     * The background query may still be writing cursor data when the delete arrives (the cursor
     * index may not even exist yet). In that case getMetadata fails with a retryable error and
     * we retry the entire flow until the metadata becomes available or the deadline is reached.
     */
    private void cancelThenDelete(String cursorId, long deadlineMillis, ActionListener<AcknowledgedResponse> listener) {
        logger.info("cancelThenDelete cursor [{}]: fetching metadata", cursorId);
        cursorIndexService.getMetadata(cursorId, ActionListener.wrap(metadata -> {
            logger.info(
                "cancelThenDelete cursor [{}]: metadata fetched, totalPages=[{}], isInProgress=[{}], taskId=[{}]",
                cursorId,
                metadata.totalPages(),
                metadata.isInProgress(),
                metadata.taskId()
            );
            TaskId taskId = metadata.taskId();
            if (taskId != null && metadata.isInProgress()) {
                stopBackgroundTask(taskId, cursorId, ActionListener.wrap(v -> { deleteWithRetry(cursorId, 0, listener); }, e -> {
                    logger.debug("failed to stop background task [{}] for cursor [{}], proceeding with delete", taskId, cursorId, e);
                    deleteWithRetry(cursorId, 0, listener);
                }));
            } else {
                deleteWithRetry(cursorId, 0, listener);
            }
        }, e -> {
            // The metadata doc may not exist yet because the background StorePageOperator hasn't
            // written it, or the cursor index itself may not exist yet. Both cases surface as
            // ResourceNotFoundException or IndexNotFoundException/NoShardAvailableActionException.
            // Retry until the metadata appears or the deadline is reached, then proceed with delete.
            if (e instanceof ResourceNotFoundException || isRetryable(e)) {
                if (threadPool.relativeTimeInMillis() < deadlineMillis) {
                    logger.info("cancelThenDelete cursor [{}]: [{}], retrying", cursorId, e.getMessage());
                    threadPool.schedule(
                        () -> cancelThenDelete(cursorId, deadlineMillis, listener),
                        METADATA_RETRY_DELAY,
                        threadPool.generic()
                    );
                } else {
                    logger.info("cancelThenDelete cursor [{}]: deadline reached, proceeding with delete", cursorId);
                    deleteWithRetry(cursorId, 0, listener);
                }
            } else {
                listener.onFailure(e);
            }
        }));
    }

    /**
     * Gracefully stops the background pagination compute by finishing the exchange session early,
     * following the same pattern as {@code TransportEsqlAsyncStopAction}. If the task is on a
     * different node, the request is forwarded there. This avoids task cancellation which would
     * run the Driver teardown on the wrong thread pool.
     */
    private void stopBackgroundTask(TaskId taskId, String cursorId, ActionListener<Void> listener) {
        String taskNodeId = taskId.getNodeId();
        String localNodeId = clusterService.localNode().getId();

        if (localNodeId.equals(taskNodeId)) {
            stopBackgroundTaskLocally(taskId, cursorId, listener);
        } else {
            DiscoveryNode node = clusterService.state().nodes().get(taskNodeId);
            if (node == null) {
                listener.onResponse(null);
            } else {
                transportService.sendRequest(
                    node,
                    EsqlDeleteCursorAction.NAME,
                    new EsqlDeleteCursorRequest(new EsqlCursor(cursorId, 0).encode(), true),
                    new org.elasticsearch.transport.TransportResponseHandler.Empty() {
                        @Override
                        public java.util.concurrent.Executor executor() {
                            return EsExecutors.DIRECT_EXECUTOR_SERVICE;
                        }

                        @Override
                        public void handleResponse() {
                            listener.onResponse(null);
                        }

                        @Override
                        public void handleException(org.elasticsearch.transport.TransportException exp) {
                            listener.onFailure(exp);
                        }
                    }
                );
            }
        }
    }

    private void stopBackgroundTaskLocally(TaskId taskId, String cursorId, ActionListener<Void> listener) {
        AtomicBoolean responded = new AtomicBoolean(false);
        ActionListener<Void> onceListener = new ActionListener<>() {
            @Override
            public void onResponse(Void v) {
                if (responded.compareAndSet(false, true)) {
                    listener.onResponse(null);
                }
            }

            @Override
            public void onFailure(Exception e) {
                if (responded.compareAndSet(false, true)) {
                    listener.onFailure(e);
                }
            }
        };

        var timeoutHandle = threadPool.schedule(() -> {
            logger.warn(
                "timed out waiting for background task [{}] to stop for cursor [{}] after [{}], proceeding with delete",
                taskId,
                cursorId,
                CANCEL_TASK_TIMEOUT
            );
            onceListener.onResponse(null);
        }, CANCEL_TASK_TIMEOUT, threadPool.generic());

        Task task = taskManager.getTask(taskId.getId());
        if (task instanceof ComputeService.PaginationBackgroundTask bgTask) {
            exchangeService.finishSessionEarly(bgTask.sessionId(), ActionListener.running(() -> {
                bgTask.addCompletionListener(onceListener.map(v -> {
                    timeoutHandle.cancel();
                    return null;
                }));
            }));
        } else {
            timeoutHandle.cancel();
            onceListener.onResponse(null);
        }
    }

    private void deleteWithRetry(String cursorId, int attempt, ActionListener<AcknowledgedResponse> listener) {
        cursorIndexService.delete(cursorId, ActionListener.wrap(deleted -> listener.onResponse(AcknowledgedResponse.of(deleted)), e -> {
            if (isRetryable(e) && attempt < MAX_DELETE_RETRIES) {
                logger.debug("cursor index not yet available for delete of [{}], retrying (attempt [{}])", cursorId, attempt + 1);
                threadPool.schedule(() -> deleteWithRetry(cursorId, attempt + 1, listener), DELETE_RETRY_DELAY, threadPool.generic());
            } else {
                listener.onFailure(e);
            }
        }));
    }

    static boolean isRetryable(Exception e) {
        return e instanceof IndexNotFoundException || e instanceof NoShardAvailableActionException;
    }
}
