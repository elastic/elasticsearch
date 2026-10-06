/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.prometheus.rest;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.node.tasks.cancel.CancelTasksRequest;
import org.elasticsearch.client.internal.OriginSettingClient;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.http.HttpChannel;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.action.RestCancellableNodeClient;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.esql.action.EsqlQueryAction;
import org.elasticsearch.xpack.esql.action.EsqlQueryResponse;
import org.elasticsearch.xpack.esql.parser.ParsingException;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.elasticsearch.action.admin.cluster.node.tasks.get.TransportGetTaskAction.TASKS_ORIGIN;

/**
 * Runs the ES|QL request behind the Prometheus {@code query} and {@code query_range} endpoints so that it doesn't outlive its
 * HTTP request: the query is cancelled when the client disconnects, and when the Prometheus {@code timeout} elapses.
 * <p>
 * ES|QL has no query timeout of its own yet, so the timeout is enforced here by cancelling the query's task, which ES|QL already
 * honors. On timeout, the client receives a {@code 503} which maps to the Prometheus {@code timeout} error type.
 */
final class PromqlQueryExecutor {

    private static final Logger logger = LogManager.getLogger(PromqlQueryExecutor.class);

    static final String TIMEOUT_PARAM = "timeout";

    private PromqlQueryExecutor() {}

    /**
     * Resolves the timeout for a request, following Prometheus semantics: the {@code timeout} parameter defaults to, and is capped
     * by, {@code maxTimeout}.
     *
     * @param maxTimeout the configured maximum timeout; {@code -1} means there is no maximum
     * @return the timeout to apply, or {@code null} if the query must not time out
     */
    @Nullable
    static TimeValue resolveTimeout(RestRequest request, TimeValue maxTimeout) {
        String value = request.param(TIMEOUT_PARAM);
        TimeValue requested = value == null || value.isEmpty() ? null : parseTimeout(value);
        boolean hasMax = maxTimeout.duration() > 0;
        if (requested == null) {
            return hasMax ? maxTimeout : null;
        }
        return hasMax && requested.compareTo(maxTimeout) > 0 ? maxTimeout : requested;
    }

    /**
     * Parses the {@code timeout} parameter like other Prometheus duration parameters, see {@link PromqlQueryPlanBuilder#parseDuration}.
     */
    static TimeValue parseTimeout(String value) {
        Duration timeout;
        try {
            timeout = PromqlQueryPlanBuilder.parseDuration(value);
        } catch (ParsingException e) {
            throw new IllegalArgumentException(
                "invalid parameter \"" + TIMEOUT_PARAM + "\": cannot parse \"" + value + "\" to a valid duration",
                e
            );
        }
        if (timeout.isPositive() == false) {
            throw new IllegalArgumentException("invalid parameter \"" + TIMEOUT_PARAM + "\": must be positive, got \"" + value + "\"");
        }
        return TimeValue.timeValueMillis(timeout.toMillis());
    }

    /**
     * Executes {@code esqlRequest}, cancelling it if {@code httpChannel} closes or {@code timeout} elapses before it completes.
     *
     * @param timeout the timeout to apply, or {@code null} for none
     */
    static void execute(
        NodeClient client,
        HttpChannel httpChannel,
        PromqlQueryRequest esqlRequest,
        @Nullable TimeValue timeout,
        ActionListener<EsqlQueryResponse> listener
    ) {
        var cancellableClient = new RestCancellableNodeClient(client, httpChannel);
        if (timeout == null) {
            cancellableClient.execute(EsqlQueryAction.INSTANCE, esqlRequest, listener);
            return;
        }
        var timeoutListener = new TimeoutListener(listener);
        Task task;
        try {
            task = cancellableClient.executeAndReturnTask(EsqlQueryAction.INSTANCE, esqlRequest, timeoutListener);
        } catch (Exception e) {
            timeoutListener.onFailure(e);
            return;
        }
        TaskId taskId = new TaskId(client.getLocalNodeId(), task.getId());
        timeoutListener.scheduleTimeout(client.threadPool(), timeout, () -> cancelTask(client, taskId, timeout));
    }

    private static void cancelTask(NodeClient client, TaskId taskId, TimeValue timeout) {
        CancelTasksRequest request = new CancelTasksRequest().setTargetTaskId(taskId).setReason("timed out after [" + timeout + "]");
        // the user that issued the query may not be allowed to cancel tasks
        new OriginSettingClient(client, TASKS_ORIGIN).admin()
            .cluster()
            .cancelTasks(request, ActionListener.wrap(r -> {}, e -> logger.debug("failed to cancel timed out task [{}]", taskId, e)));
    }

    /**
     * Completes the delegate with whichever comes first: the query's result, or a timeout failure.
     */
    private static final class TimeoutListener implements ActionListener<EsqlQueryResponse> {
        private final ActionListener<EsqlQueryResponse> delegate;
        private final AtomicBoolean completed = new AtomicBoolean();
        private volatile Scheduler.ScheduledCancellable scheduledTimeout;

        TimeoutListener(ActionListener<EsqlQueryResponse> delegate) {
            this.delegate = delegate;
        }

        void scheduleTimeout(ThreadPool threadPool, TimeValue timeout, Runnable onTimeout) {
            scheduledTimeout = threadPool.schedule(() -> {
                if (completed.compareAndSet(false, true)) {
                    onTimeout.run();
                    delegate.onFailure(
                        new ElasticsearchStatusException("query timed out after [{}]", RestStatus.SERVICE_UNAVAILABLE, timeout)
                    );
                }
            }, timeout, threadPool.generic());
            if (completed.get()) {
                scheduledTimeout.cancel();
            }
        }

        @Override
        public void onResponse(EsqlQueryResponse response) {
            if (completed.compareAndSet(false, true)) {
                cancelScheduledTimeout();
                delegate.onResponse(response);
            }
        }

        @Override
        public void onFailure(Exception e) {
            if (completed.compareAndSet(false, true)) {
                cancelScheduledTimeout();
                delegate.onFailure(e);
            }
        }

        private void cancelScheduledTimeout() {
            Scheduler.ScheduledCancellable scheduled = scheduledTimeout;
            if (scheduled != null) {
                scheduled.cancel();
            }
        }
    }
}
