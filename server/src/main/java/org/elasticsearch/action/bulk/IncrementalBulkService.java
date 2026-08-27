/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.bulk;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexingPressure;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskAwareRequest;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.tasks.TaskManager;
import org.elasticsearch.telemetry.metric.LongHistogram;
import org.elasticsearch.telemetry.metric.MeterRegistry;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

import static java.util.Collections.emptySet;
import static org.elasticsearch.common.settings.Setting.boolSetting;

public class IncrementalBulkService {
    public static final String CHUNK_WAIT_TIME_HISTOGRAM_NAME = "es.rest.incremental_bulk.wait_for_next_chunk.duration.histogram";
    public static final String BULK_SESSION_TASK_TYPE = "bulk";
    public static final String BULK_SESSION_ACTION = "internal:bulk";

    public static final Setting<Boolean> INCREMENTAL_BULK = boolSetting(
        "rest.incremental_bulk",
        true,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * Overall wall-clock timeout for a single incremental bulk request, measured from the moment the
     * request session is created. A non-positive value (the default) disables the timeout, and no
     * {@link Handler#bulkSessionTask} is registered at all: without a timeout there is no bound on how
     * long a session may stay open, so a registration whose handler is abandoned could never be
     * reclaimed. When the timeout is configured and elapses, the task is cancelled and then
     * unregistered, which causes the next chunk to fail with a
     * {@link org.elasticsearch.tasks.TaskCancelledException}.
     */
    public static final Setting<TimeValue> REQUEST_TIMEOUT = Setting.timeSetting(
        "rest.incremental_bulk.request_timeout",
        TimeValue.MINUS_ONE,
        TimeValue.MINUS_ONE,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    private final Client client;
    private final AtomicBoolean enabledForTests = new AtomicBoolean(true);
    private final IndexingPressure indexingPressure;
    private final TaskManager taskManager;
    private final ThreadPool threadPool;
    @Nullable
    private volatile TimeValue requestTimeout;

    /* Capture in milliseconds because the APM histogram only has a range of 100,000 */
    private final LongHistogram chunkWaitTimeMillisHistogram;

    /**
     * Test-only constructor for callers that have no {@link ClusterSettings} to wire. Requests created by
     * this instance never time out; production code must use the {@link ClusterSettings}-aware constructor
     * so that {@link #REQUEST_TIMEOUT} is honoured.
     */
    public IncrementalBulkService(
        Client client,
        IndexingPressure indexingPressure,
        MeterRegistry meterRegistry,
        TaskManager taskManager,
        ThreadPool threadPool
    ) {
        this(client, indexingPressure, meterRegistry, taskManager, threadPool, null);
    }

    /**
     * @param clusterSettings used to watch {@link #REQUEST_TIMEOUT}; when {@code null} no overall request
     *                        timeout is enforced (see the test-only constructor above).
     */
    public IncrementalBulkService(
        Client client,
        IndexingPressure indexingPressure,
        MeterRegistry meterRegistry,
        TaskManager taskManager,
        ThreadPool threadPool,
        @Nullable ClusterSettings clusterSettings
    ) {
        this.client = client;
        this.indexingPressure = indexingPressure;
        this.taskManager = taskManager;
        this.threadPool = threadPool;
        this.chunkWaitTimeMillisHistogram = meterRegistry.registerLongHistogram(
            CHUNK_WAIT_TIME_HISTOGRAM_NAME,
            "Total time in millis spent waiting for next chunk of a bulk request",
            "ms"
        );
        if (clusterSettings != null) {
            clusterSettings.initializeAndWatch(REQUEST_TIMEOUT, value -> this.requestTimeout = value);
        }
    }

    public Handler newBulkRequest() {
        ensureEnabled();
        return newBulkRequest(null, null, null, emptySet());
    }

    public Handler newBulkRequest(
        @Nullable String waitForActiveShards,
        @Nullable TimeValue timeout,
        @Nullable String refresh,
        Set<String> paramsUsed
    ) {
        ensureEnabled();
        Handler handler = new Handler(
            client,
            indexingPressure,
            waitForActiveShards,
            timeout,
            refresh,
            chunkWaitTimeMillisHistogram,
            paramsUsed,
            taskManager,
            threadPool,
            requestTimeout
        );
        // Scheduled after construction (rather than in the Handler constructor) so the timeout callback
        // never captures a partially-initialized Handler.
        handler.scheduleTimeout();
        return handler;
    }

    private void ensureEnabled() {
        if (enabledForTests.get() == false) {
            throw new AssertionError("Unexpected incremental bulk request");
        }
    }

    // This method only exists to tests that the feature flag works. Remove once we no longer need the flag.
    public void setForTests(boolean value) {
        enabledForTests.set(value);
    }

    public static class Enabled implements Supplier<Boolean> {

        private final AtomicBoolean incrementalBulksEnabled = new AtomicBoolean(true);

        public Enabled() {}

        public Enabled(ClusterSettings clusterSettings) {
            incrementalBulksEnabled.set(clusterSettings.get(INCREMENTAL_BULK));
            clusterSettings.addSettingsUpdateConsumer(INCREMENTAL_BULK, incrementalBulksEnabled::set);
        }

        @Override
        public Boolean get() {
            return incrementalBulksEnabled.get();
        }
    }

    public static class Handler implements Releasable {

        public static final BulkRequest.IncrementalState EMPTY_STATE = new BulkRequest.IncrementalState(Collections.emptyMap(), true);

        private final Client client;
        private final ActiveShardCount waitForActiveShards;
        private final TimeValue timeout;
        private final Set<String> paramsUsed;
        private final String refresh;

        private final ArrayList<Releasable> releasables = new ArrayList<>(4);
        private final ArrayList<BulkResponse> responses = new ArrayList<>(2);
        private final IndexingPressure.Incremental incrementalOperation;
        // Ideally this should be in RestBulkAction, but it's harder to inject the metric registry there
        private final LongHistogram chunkWaitTimeMillisHistogram;
        private boolean closed = false;
        private boolean globalFailure = false;
        private boolean incrementalRequestSubmitted = false;
        private boolean bulkInProgress = false;
        private Exception bulkActionLevelFailure = null;
        private BulkRequest bulkRequest = null;
        private final TaskManager taskManager;
        private final ThreadPool threadPool;
        // Registered only when the request timeout is active; see REQUEST_TIMEOUT.
        @Nullable
        private final CancellableTask bulkSessionTask;
        @Nullable
        private final TimeValue requestTimeout;
        private final AtomicBoolean sessionTaskUnregistered = new AtomicBoolean();
        // Cancels the request after REQUEST_TIMEOUT elapses; null when no timeout is configured.
        private volatile Scheduler.Cancellable pendingTimeout;

        protected Handler(
            Client client,
            IndexingPressure indexingPressure,
            @Nullable String waitForActiveShards,
            @Nullable TimeValue timeout,
            @Nullable String refresh,
            LongHistogram chunkWaitTimeMillisHistogram,
            Set<String> paramsUsed,
            TaskManager taskManager,
            ThreadPool threadPool,
            @Nullable TimeValue requestTimeout
        ) {
            this.taskManager = taskManager;
            this.requestTimeout = requestTimeout;
            if (timeoutIsActive(requestTimeout)) {
                try (var ignored = threadPool.getThreadContext().newTraceContext()) {
                    bulkSessionTask = (CancellableTask) taskManager.register(
                        BULK_SESSION_TASK_TYPE,
                        BULK_SESSION_ACTION,
                        new TaskAwareRequest() {
                            @Override
                            public void setParentTask(TaskId taskId) {}

                            @Override
                            public void setRequestId(long requestId) {}

                            @Override
                            public TaskId getParentTask() {
                                return TaskId.EMPTY_TASK_ID;
                            }

                            @Override
                            public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
                                return new CancellableTask(id, type, action, getDescription(), parentTaskId, headers);
                            }
                        }
                    );
                }
            } else {
                bulkSessionTask = null;
            }

            this.client = client;
            this.threadPool = threadPool;
            this.waitForActiveShards = waitForActiveShards != null ? ActiveShardCount.parseString(waitForActiveShards) : null;
            this.timeout = timeout;
            this.refresh = refresh;
            this.paramsUsed = paramsUsed;
            this.incrementalOperation = indexingPressure.startIncrementalCoordinating(0, 0, false);
            this.chunkWaitTimeMillisHistogram = chunkWaitTimeMillisHistogram;
            createNewBulkRequest(EMPTY_STATE);
        }

        /**
         * Whether {@code requestTimeout} enables the overall request timeout. This is the single predicate
         * deciding both whether {@link #bulkSessionTask} is registered and whether the timeout is scheduled;
         * the two must never disagree, or a session would be registered with nothing to reclaim it.
         *
         * <p>Guards on millis(), not nanos(): {@link ThreadPool#schedule} floors the delay to whole
         * milliseconds, so a sub-millisecond timeout would otherwise arm a zero-delay task that cancels the
         * request immediately.
         */
        private static boolean timeoutIsActive(@Nullable TimeValue requestTimeout) {
            return requestTimeout != null && requestTimeout.millis() > 0;
        }

        /**
         * Schedules cancellation of this request once the request timeout elapses. A no-op when the timeout
         * is inactive, in which case no session task was registered either. Called once, immediately after
         * construction.
         */
        private void scheduleTimeout() {
            if (timeoutIsActive(requestTimeout)) {
                pendingTimeout = threadPool.schedule(this::onTimeout, requestTimeout, threadPool.generic());
            }
        }

        private void onTimeout() {
            cancelAndUnregisterSessionTask("request timed out after [" + requestTimeout + "]");
        }

        /**
         * Reclaims the session registration, at most once for the lifetime of this handler. Several paths can
         * end a session and more than one may run for the same session, but
         * {@link TaskManager#unregister} must not be called twice for one task.
         */
        private void unregisterSessionTask() {
            if (bulkSessionTask != null && sessionTaskUnregistered.compareAndSet(false, true)) {
                taskManager.unregister(bulkSessionTask);
            }
        }

        /**
         * Cancels the session and then reclaims it, for the paths that end a session before its response was
         * produced. The reclaim runs even if the cancellation throws, because this is the terminal reclaim
         * for a handler that may already have been abandoned: no further chunk will arrive, so neither
         * {@link #close()} nor {@code lastItems} will run again. A session already reclaimed by
         * {@code lastItems} completing is left alone.
         *
         * <p>The cancellation must precede the reclaim: cancelling a task that is already unregistered
         * silently does nothing while still reporting success.
         */
        private void cancelAndUnregisterSessionTask(String reason) {
            if (bulkSessionTask == null || sessionTaskUnregistered.get()) {
                return;
            }
            try {
                cancel(reason, () -> {});
            } finally {
                unregisterSessionTask();
            }
        }

        /** Cancels the pending timeout, if any. Idempotent; safe to call once the request has finished. */
        private void cancelTimeout() {
            Scheduler.Cancellable pending = pendingTimeout;
            if (pending != null) {
                pending.cancel();
            }
        }

        /**
         * Stashes the thread context before propagating the cancellation. {@code internal:admin/tasks/ban}
         * (and its matching unban) can never be granted to a user and must run as the system user.
         * {@link ThreadPool#schedule} preserves the REST caller's context into the timeout lambda, so
         * without the stash the security interceptor would deny the ban request. Mirrors
         * {@link org.elasticsearch.tasks.TaskCancellationService} sending {@code cancel_child}.
         */
        public void cancel(String reason, Runnable listener) {
            if (bulkSessionTask == null) {
                // No session task is registered unless the request timeout is active, so there is nothing to
                // cancel and the cancellation is vacuously complete.
                listener.run();
                return;
            }
            try (ThreadContext.StoredContext ignored = threadPool.getThreadContext().stashContext()) {
                threadPool.getThreadContext().markAsSystemContext();
                taskManager.cancelTaskAndDescendants(bulkSessionTask, reason, false, ActionListener.running(listener));
            }
        }

        public IndexingPressure.Incremental getIncrementalOperation() {
            return incrementalOperation;
        }

        public void updateWaitForChunkMetrics(long chunkWaitTimeInMillis) {
            chunkWaitTimeMillisHistogram.record(chunkWaitTimeInMillis);
        }

        public void addItems(List<DocWriteRequest<?>> items, Releasable releasable, Runnable nextItems) {
            assert closed == false;
            assert bulkInProgress == false;

            if (bulkActionLevelFailure != null) {
                shortCircuitDueToTopLevelFailure(items, releasable);
                nextItems.run();
            } else {
                assert bulkRequest != null;
                if (internalAddItems(items, releasable)) {
                    Optional<Releasable> maybeSplit = incrementalOperation.maybeSplit();
                    if (maybeSplit.isPresent()) {
                        Releasable coordinating = maybeSplit.get();
                        final boolean isFirstRequest = incrementalRequestSubmitted == false;
                        incrementalRequestSubmitted = true;
                        final ArrayList<Releasable> toRelease = new ArrayList<>(releasables);
                        releasables.clear();
                        bulkInProgress = true;
                        client.bulk(bulkRequest, ActionListener.runAfter(new ActionListener<>() {
                            @Override
                            public void onResponse(BulkResponse bulkResponse) {
                                handleBulkSuccess(bulkResponse);
                                createNewBulkRequest(
                                    new BulkRequest.IncrementalState(bulkResponse.getIncrementalState().shardLevelFailures(), true)
                                );
                            }

                            @Override
                            public void onFailure(Exception e) {
                                handleBulkFailure(isFirstRequest, e);
                            }
                        }, () -> {
                            bulkInProgress = false;
                            toRelease.forEach(Releasable::close);
                            coordinating.close();
                            nextItems.run();
                        }));
                    } else {
                        nextItems.run();
                    }
                } else {
                    nextItems.run();
                }
            }
        }

        public void lastItems(List<DocWriteRequest<?>> items, Releasable releasable, ActionListener<BulkResponse> listener) {
            assert bulkInProgress == false;
            ActionListener<BulkResponse> finalListener = ActionListener.runBefore(listener, () -> {
                cancelTimeout();
                unregisterSessionTask();
            });
            if (bulkActionLevelFailure != null) {
                shortCircuitDueToTopLevelFailure(items, releasable);
                errorResponse(finalListener);
            } else {
                assert bulkRequest != null;
                if (internalAddItems(items, releasable)) {
                    Releasable coordinating = incrementalOperation.split();
                    final ArrayList<Releasable> toRelease = new ArrayList<>(releasables);
                    releasables.clear();
                    // We do not need to set this back to false as this will be the last request.
                    bulkInProgress = true;
                    client.bulk(bulkRequest, ActionListener.runBefore(new ActionListener<>() {

                        private final boolean isFirstRequest = incrementalRequestSubmitted == false;

                        @Override
                        public void onResponse(BulkResponse bulkResponse) {
                            handleBulkSuccess(bulkResponse);
                            finalListener.onResponse(BulkResponse.combine(responses));
                        }

                        @Override
                        public void onFailure(Exception e) {
                            handleBulkFailure(isFirstRequest, e);
                            errorResponse(finalListener);
                        }
                    }, () -> {
                        toRelease.forEach(Releasable::close);
                        coordinating.close();
                    }));
                } else {
                    errorResponse(finalListener);
                }
            }
        }

        @Override
        public void close() {
            if (closed == false) {
                closed = true;
                cancelTimeout();
                incrementalOperation.close();
                releasables.forEach(Releasable::close);
                releasables.clear();
                cancelAndUnregisterSessionTask("handler closed");
            }
        }

        private void shortCircuitDueToTopLevelFailure(List<DocWriteRequest<?>> items, Releasable releasable) {
            assert releasables.isEmpty();
            assert incrementalOperation.currentOperationsSize() == 0;
            assert bulkRequest == null;
            if (globalFailure == false) {
                addItemLevelFailures(items);
            }
            Releasables.close(releasable);
        }

        private void errorResponse(ActionListener<BulkResponse> listener) {
            if (globalFailure) {
                listener.onFailure(bulkActionLevelFailure);
            } else {
                listener.onResponse(BulkResponse.combine(responses));
            }
        }

        private void handleBulkSuccess(BulkResponse bulkResponse) {
            responses.add(bulkResponse);
            bulkRequest = null;
        }

        private void handleBulkFailure(boolean isFirstRequest, Exception e) {
            assert bulkActionLevelFailure == null;
            globalFailure = isFirstRequest;
            bulkActionLevelFailure = e instanceof TaskCancelledException tce
                ? new ElasticsearchStatusException(tce.getMessage(), RestStatus.TOO_MANY_REQUESTS, tce)
                : e;
            addItemLevelFailures(bulkRequest.requests());
            bulkRequest = null;
        }

        private void addItemLevelFailures(List<DocWriteRequest<?>> items) {
            BulkItemResponse[] bulkItemResponses = new BulkItemResponse[items.size()];
            int idx = 0;
            for (DocWriteRequest<?> item : items) {
                BulkItemResponse.Failure failure = new BulkItemResponse.Failure(item.index(), item.id(), bulkActionLevelFailure);
                bulkItemResponses[idx++] = BulkItemResponse.failure(idx, item.opType(), failure);
            }

            responses.add(new BulkResponse(bulkItemResponses, 0, 0));
        }

        private boolean internalAddItems(List<DocWriteRequest<?>> items, Releasable releasable) {
            bulkRequest.add(items);
            releasables.add(releasable);
            if (bulkSessionTask != null && bulkSessionTask.isCancelled()) {
                failAndRelease(bulkSessionTask.getTaskCancelledException());
                return false;
            } else {
                try {
                    long ramBytesUsed = 0;
                    for (final var item : items) {
                        ramBytesUsed += item.ramBytesUsed();
                    }
                    incrementalOperation.increment(items.size(), ramBytesUsed);
                    return true;
                } catch (EsRejectedExecutionException e) {
                    failAndRelease(e);
                    return false;
                }
            }
        }

        private void failAndRelease(Exception e) {
            handleBulkFailure(incrementalRequestSubmitted == false, e);
            incrementalOperation.split().close();
            releasables.forEach(Releasable::close);
            releasables.clear();
        }

        private void createNewBulkRequest(BulkRequest.IncrementalState incrementalState) {
            assert bulkRequest == null;
            assert taskManager != null;
            bulkRequest = new BulkRequest();
            if (bulkSessionTask != null) {
                bulkRequest.setParentTask(new TaskId(taskManager.getNodeId(), bulkSessionTask.getId()));
            }
            bulkRequest.incrementalState(incrementalState);

            if (waitForActiveShards != null) {
                bulkRequest.waitForActiveShards(waitForActiveShards);
            }
            if (timeout != null) {
                bulkRequest.timeout(timeout);
            }
            if (refresh != null) {
                bulkRequest.setRefreshPolicy(refresh);
            }
            bulkRequest.requestParamsUsed(paramsUsed);
        }

        // Visible for testing; null unless the request timeout is active
        @Nullable
        protected Task getBulkSessionTask() {
            return bulkSessionTask;
        }
    }
}
