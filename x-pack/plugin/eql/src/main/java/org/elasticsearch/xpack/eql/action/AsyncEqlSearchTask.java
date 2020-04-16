/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.eql.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.node.tasks.cancel.CancelTasksRequest;
import org.elasticsearch.action.admin.cluster.node.tasks.cancel.CancelTasksResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.threadpool.Scheduler.Cancellable;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.async.AsyncExecutionId;
import org.elasticsearch.xpack.core.async.AsyncTask;
import org.elasticsearch.xpack.core.eql.action.AsyncEqlSearchResponse;
import org.elasticsearch.xpack.core.eql.action.EqlSearchResponse;
import org.elasticsearch.xpack.core.eql.action.EqlSearchTask;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;

/**
 * Task that tracks the progress of a currently running {@link SearchRequest}.
 */
public final class AsyncEqlSearchTask extends EqlSearchTask implements AsyncTask {
    private final BooleanSupplier checkSubmitCancellation;
    private final AsyncExecutionId searchId;
    private final Client client;
    private final ThreadPool threadPool;
    private final Listener progressListener;

    private final Map<String, String> originHeaders;

    private boolean hasInitialized;
    private boolean hasCompleted;
    private long completionId;
    private final List<Runnable> initListeners = new ArrayList<>();
    private final Map<Long, Consumer<AsyncEqlSearchResponse>> completionListeners = new HashMap<>();

    private volatile long expirationTimeMillis;
    private final AtomicBoolean isCancelling = new AtomicBoolean(false);

    private final AtomicReference<MutableEqlSearchResponse> searchResponse = new AtomicReference<>();

    /**
     * Creates an instance of {@link AsyncEqlSearchTask}.
     *
     * @param id The id of the task.
     * @param type The type of the task.
     * @param action The action name.
     * @param parentTaskId The parent task id.
     * @param checkSubmitCancellation A boolean supplier that checks if the submit task has been cancelled.
     * @param originHeaders All the request context headers.
     * @param taskHeaders The filtered request headers for the task.
     * @param searchId The {@link AsyncExecutionId} of the task.
     * @param threadPool The threadPool to schedule runnable.
     */
    public AsyncEqlSearchTask(long id,
                       String type,
                       String action,
                       TaskId parentTaskId,
                       BooleanSupplier checkSubmitCancellation,
                       TimeValue keepAlive,
                       Map<String, String> originHeaders,
                       Map<String, String> taskHeaders,
                       AsyncExecutionId searchId,
                       Client client,
                       ThreadPool threadPool) {
        super(id, type, action, () -> "async_search", parentTaskId, taskHeaders);
        this.checkSubmitCancellation = checkSubmitCancellation;
        this.expirationTimeMillis = getStartTime() + keepAlive.getMillis();
        this.originHeaders = originHeaders;
        this.searchId = searchId;
        this.client = client;
        this.threadPool = threadPool;
        this.progressListener = new Listener();
        setProgressListener(progressListener);
    }

    /**
     * Returns all of the request contexts headers
     */
    @Override
    public Map<String, String> getOriginHeaders() {
        return originHeaders;
    }

    /**
     * Returns the {@link AsyncExecutionId} of the task
     */
    @Override
    public AsyncExecutionId getExecutionId() {
        return searchId;
    }

    public Listener getSearchProgressActionListener() {
        return progressListener;
    }

    /**
     * Update the expiration time of the (partial) response.
     */
    public void setExpirationTime(long expirationTimeMillis) {
        this.expirationTimeMillis = expirationTimeMillis;
    }

    /**
     * Cancels the running task and its children.
     */
    public void cancelTask(Runnable runnable) {
        if (isCancelled() == false && isCancelling.compareAndSet(false, true)) {
            CancelTasksRequest req = new CancelTasksRequest().setTaskId(searchId.getTaskId());
            client.admin().cluster().cancelTasks(req, new ActionListener<>() {
                @Override
                public void onResponse(CancelTasksResponse cancelTasksResponse) {
                    runnable.run();
                }

                @Override
                public void onFailure(Exception exc) {
                    // cancelling failed
                    isCancelling.compareAndSet(true, false);
                    runnable.run();
                }
            });
        } else {
            runnable.run();
       }
    }

    @Override
    protected void onCancelled() {
        super.onCancelled();
        isCancelling.compareAndSet(true, false);
    }

    /**
     * Creates a listener that listens for an {@link AsyncEqlSearchResponse} and executes the
     * consumer when the task is finished or when the provided <code>waitForCompletion</code>
     * timeout occurs. In such case the consumed {@link AsyncEqlSearchResponse} will contain partial results.
     */
    public void addCompletionListener(ActionListener<AsyncEqlSearchResponse> listener, TimeValue waitForCompletion) {
        boolean executeImmediately = false;
        long startTime = threadPool.relativeTimeInMillis();
        synchronized (this) {
            if (hasCompleted) {
                executeImmediately = true;
            } else {
                addInitListener(() -> {
                    final TimeValue remainingWaitForCompletion;
                    if (waitForCompletion.getMillis() > 0) {
                        long elapsedTime = threadPool.relativeTimeInMillis() - startTime;
                        // subtract the initialization time from the provided waitForCompletion.
                        remainingWaitForCompletion = TimeValue.timeValueMillis(Math.max(0, waitForCompletion.getMillis() - elapsedTime));
                    } else {
                        remainingWaitForCompletion = TimeValue.ZERO;
                    }
                    internalAddCompletionListener(listener, remainingWaitForCompletion);
                });
            }
        }
        if (executeImmediately) {
            listener.onResponse(getResponseWithHeaders());
        }
    }

    /**
     * Creates a listener that listens for an {@link AsyncEqlSearchResponse} and executes the
     * consumer when the task is finished.
     */
    public void addCompletionListener(Consumer<AsyncEqlSearchResponse>  listener) {
        boolean executeImmediately = false;
        synchronized (this) {
            if (hasCompleted) {
                executeImmediately = true;
            } else {
                completionListeners.put(completionId++, listener);
            }
        }
        if (executeImmediately) {
            listener.accept(getResponseWithHeaders());
        }
    }

    private void internalAddCompletionListener(ActionListener<AsyncEqlSearchResponse> listener, TimeValue waitForCompletion) {
        boolean executeImmediately = false;
        synchronized (this) {
            if (hasCompleted || waitForCompletion.getMillis() == 0) {
                executeImmediately = true;
            } else {
                // ensure that we consumes the listener only once
                AtomicBoolean hasRun = new AtomicBoolean(false);
                long id = completionId++;

                final Cancellable cancellable;
                try {
                    cancellable = threadPool.schedule(() -> {
                        if (hasRun.compareAndSet(false, true)) {
                            // timeout occurred before completion
                            removeCompletionListener(id);
                            listener.onResponse(getResponseWithHeaders());
                        }
                    }, waitForCompletion, "generic");
                } catch (EsRejectedExecutionException exc) {
                    listener.onFailure(exc);
                    return;
                }
                completionListeners.put(id, resp -> {
                    if (hasRun.compareAndSet(false, true)) {
                        // completion occurred before timeout
                        cancellable.cancel();
                        listener.onResponse(resp);
                    }
                });
            }
        }
        if (executeImmediately) {
            listener.onResponse(getResponseWithHeaders());
        }
    }

    private void removeCompletionListener(long id) {
        synchronized (this) {
            if (hasCompleted == false) {
                completionListeners.remove(id);
            }
        }
    }

    private void addInitListener(Runnable listener) {
        boolean executeImmediately = false;
        synchronized (this) {
            if (hasInitialized) {
                executeImmediately = true;
            } else {
                initListeners.add(listener);
            }
        }
        if (executeImmediately) {
            listener.run();
        }
    }

    private void executeInitListeners() {
        synchronized (this) {
            if (hasInitialized) {
                return;
            }
            hasInitialized = true;
        }
        for (Runnable listener : initListeners) {
            listener.run();
        }
        initListeners.clear();
    }

    private void executeCompletionListeners() {
        synchronized (this) {
            if (hasCompleted) {
                return;
            }
            hasCompleted = true;
        }
        // we don't need to restore the response headers, they should be included in the current
        // context since we are called by the search action listener.
        AsyncEqlSearchResponse finalResponse = getResponse();
        for (Consumer<AsyncEqlSearchResponse> listener : completionListeners.values()) {
            listener.accept(finalResponse);
        }
        completionListeners.clear();
    }

    /**
     * Returns the current {@link AsyncEqlSearchResponse}.
     */
    private AsyncEqlSearchResponse getResponse() {
        assert searchResponse.get() != null;
        return searchResponse.get().toAsyncEqlSearchResponse(this, expirationTimeMillis);
    }

    /**
     * Returns the current {@link AsyncEqlSearchResponse} and restores the response headers
     * in the local thread context.
     */
    private AsyncEqlSearchResponse getResponseWithHeaders() {
        assert searchResponse.get() != null;
        return searchResponse.get().toAsyncEqlSearchResponseWithHeaders(this, expirationTimeMillis);
    }

    // checks if the search task should be cancelled
    private void checkCancellation() {
        long now = System.currentTimeMillis();
        if (expirationTimeMillis < now || checkSubmitCancellation.getAsBoolean()) {
            // we cancel the search task if the initial submit task was cancelled,
            // this is needed because the task cancellation mechanism doesn't
            // handle the cancellation of grand-children.
            cancelTask(() -> {});
        }
    }

    class Listener extends EqlSearchProgressActionListener {
        @Override
        protected void onPreAnalyze() {
            // best effort to cancel expired tasks
            checkCancellation();
            searchResponse.compareAndSet(null,new MutableEqlSearchResponse(threadPool.getThreadContext()));
            executeInitListeners();
        }

        @Override
        public void onPartialResult(EqlSearchResponse response) {
            // best effort to cancel expired tasks
            checkCancellation();
            searchResponse.get().updatePartialResponse(response);
        }

        @Override
        public void onResponse(EqlSearchResponse response) {
            searchResponse.get().updateFinalResponse(response);
            executeCompletionListeners();
        }

        @Override
        public void onFailure(Exception exc) {
            if (searchResponse.get() == null) {
                // if the failure occurred before calling on PreAnalyze
                searchResponse.compareAndSet(null, new MutableEqlSearchResponse(threadPool.getThreadContext()));
            }
            searchResponse.get().updateWithFailure(exc);
            executeInitListeners();
            executeCompletionListeners();
        }
    }
}
