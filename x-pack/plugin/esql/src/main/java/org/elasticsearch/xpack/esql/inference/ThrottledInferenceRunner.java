/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.inference;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.inference.action.InferenceAction;
import org.elasticsearch.xpack.esql.inference.bulk.BulkInferenceExecutionState;
import org.elasticsearch.xpack.esql.inference.bulk.BulkInferenceRequestIterator;
import org.elasticsearch.xpack.esql.inference.bulk.BulkResponseHandler;
import org.elasticsearch.xpack.esql.plugin.EsqlPlugin;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Semaphore;
import java.util.function.Consumer;

import static org.elasticsearch.xpack.core.ClientHelper.INFERENCE_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.executeAsyncWithOrigin;

/**
 * Implementation of {@link InferenceRunner} that provides throttling and concurrency control.
 * <p>
 * This runner limits the number of concurrent inference requests using a semaphore-based
 * permit system. When all permits are exhausted, additional requests are queued and
 * executed as permits become available.
 * </p>

 */
class ThrottledInferenceRunner implements InferenceRunner {

    private final Client client;
    private final ExecutorService executorService;
    private final BlockingQueue<AbstractRunnable> pendingRequestsQueue;
    private final Semaphore permits;

    /**
     * Constructs a new throttled inference runner with the specified configuration.
     *
     * @param client          The Elasticsearch client for executing inference requests
     * @param maxRunningTasks The maximum number of concurrent inference requests allowed
     */
    ThrottledInferenceRunner(Client client, int maxRunningTasks) {
        this.executorService = executorService(client.threadPool());
        this.permits = new Semaphore(maxRunningTasks);
        this.client = client;
        this.pendingRequestsQueue = new ArrayBlockingQueue<>(maxRunningTasks);
    }

    /**
     * Schedules the inference task for execution. If a permit is available, the task runs immediately; otherwise, it is queued.
     *
     * @param request  The inference request.
     * @param listener The listener to notify on response or failure.
     */
    public void execute(InferenceAction.Request request, ActionListener<InferenceAction.Response> listener) {
        enqueueTask(request, listener);
        executePendingRequests();
    }

    /**
     * Executes multiple inference requests in bulk and collects all responses.
     *
     * @param requests An iterator over the inference requests to execute
     * @param listener Called with the list of all responses in request order
     */
    public void executeBulk(BulkInferenceRequestIterator requests, ActionListener<List<InferenceAction.Response>> listener) {
        List<InferenceAction.Response> responses = new ArrayList<>(requests.estimatedSize());
        executeBulk(requests, responses::add, listener.map(ignored -> responses));
    }

    /**
     * Executes multiple inference requests in bulk with streaming response handling.
     *
     * @param requests           An iterator over the inference requests to execute
     * @param responseConsumer   Called for each successful inference response
     * @param completionListener Called when all requests are complete or if any error occurs
     */
    public void executeBulk(
        BulkInferenceRequestIterator requests,
        Consumer<InferenceAction.Response> responseConsumer,
        ActionListener<Void> completionListener
    ) {
        if (requests.hasNext() == false) {
            completionListener.onResponse(null);
            return;
        }

        final BulkInferenceExecutionState bulkExecutionState = new BulkInferenceExecutionState();
        final BulkResponseHandler responseHandler = new BulkResponseHandler(bulkExecutionState, responseConsumer, completionListener);

        while (bulkExecutionState.finished() == false && requests.hasNext()) {
            InferenceAction.Request request = requests.next();
            long seqNo = bulkExecutionState.generateSeqNo();

            if (requests.hasNext() == false) {
                bulkExecutionState.finish();
            }

            ActionListener<InferenceAction.Response> inferenceResponseListener = ActionListener.runAfter(
                ActionListener.wrap(
                    r -> bulkExecutionState.onInferenceResponse(seqNo, r),
                    e -> bulkExecutionState.onInferenceException(seqNo, e)
                ),
                responseHandler::persistPendingResponses
            );

            if (request == null) {
                inferenceResponseListener.onResponse(null);
            } else {
                execute(request, inferenceResponseListener);
            }
        }
    }

    @Override
    public ThreadPool threadPool() {
        return client.threadPool();
    }

    /**
     * Attempts to execute as many pending inference tasks as possible, limited by available permits.
     */
    private void executePendingRequests() {
        while (permits.tryAcquire()) {
            AbstractRunnable task = pendingRequestsQueue.poll();

            if (task == null) {
                permits.release();
                return;
            }

            try {
                executorService.execute(task);
            } catch (Exception e) {
                task.onFailure(e);
                permits.release();
            }
        }
    }

    /**
     * Add an inference task to the queue.
     *
     * @param request  The inference request.
     * @param listener The listener to notify on response or failure.
     */
    private void enqueueTask(InferenceAction.Request request, ActionListener<InferenceAction.Response> listener) {
        try {
            pendingRequestsQueue.put(createTask(request, listener));
        } catch (Exception e) {
            listener.onFailure(new IllegalStateException("An error occurred while adding the inference request to the queue", e));
        }
    }

    /**
     * Wraps an inference request into an {@link AbstractRunnable} that releases its permit on completion and triggers any remaining
     * queued tasks.
     *
     * @param request  The inference request.
     * @param listener The listener to notify on completion.
     * @return A runnable task encapsulating the request.
     */
    private AbstractRunnable createTask(InferenceAction.Request request, ActionListener<InferenceAction.Response> listener) {
        final ActionListener<InferenceAction.Response> completionListener = ActionListener.runAfter(listener, () -> {
            permits.release();
            executePendingRequests();
        });

        return new AbstractRunnable() {
            @Override
            protected void doRun() {
                try {
                    executeAsyncWithOrigin(client, INFERENCE_ORIGIN, InferenceAction.INSTANCE, request, completionListener);
                } catch (Throwable e) {
                    completionListener.onFailure(new RuntimeException("Unexpected failure while running inference", e));
                }
            }

            @Override
            public void onFailure(Exception e) {
                completionListener.onFailure(e);
            }
        };
    }

    /**
     * Returns the executor service for ESQL worker threads.
     *
     * @param threadPool Thread pool to use to run inference tasks
     * @return The executor service for ESQL worker threads
     */
    private static ExecutorService executorService(ThreadPool threadPool) {
        return threadPool.executor(EsqlPlugin.ESQL_WORKER_THREAD_POOL_NAME);
    }

    /**
     * Factory for throttled inference runners.
     */
    record Factory(Client client) implements InferenceRunner.Factory {

        @Override
        public InferenceRunner create(InferenceRunnerConfig inferenceRunnerConfig) {
            return new ThrottledInferenceRunner(client, inferenceRunnerConfig.maxOutstandingRequests());
        }
    }
}
