/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.inference.bulk;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.inference.action.InferenceAction;
import org.elasticsearch.xpack.esql.inference.InferenceRunner;
import org.elasticsearch.xpack.esql.plugin.EsqlPlugin;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Executes a sequence of inference requests in bulk with throttling and concurrency control.
 */
public class BulkInferenceExecutor {
    private final ThrottledInferenceRunner throttledInferenceRunner;
    private final BulkInferenceExecutionConfig bulkExecutionConfig;

    /**
     * Constructs a new {@code BulkInferenceExecutor}.
     *
     * @param inferenceRunner      The inference runner used to execute individual inference requests.
     * @param threadPool           The thread pool for executing inference tasks.
     * @param bulkExecutionConfig  Configuration options (throttling and concurrency limits).
     */
    public BulkInferenceExecutor(InferenceRunner inferenceRunner, ThreadPool threadPool, BulkInferenceExecutionConfig bulkExecutionConfig) {
        this.throttledInferenceRunner = ThrottledInferenceRunner.create(inferenceRunner, executorService(threadPool), bulkExecutionConfig);
        this.bulkExecutionConfig = bulkExecutionConfig;
    }

    /**
     * Executes the provided bulk inference requests.
     * <p>
     * Each request is sent to the {@link ThrottledInferenceRunner} to be executed.
     *  The final listener is notified with all successful responses once all requests are completed.
     *
     * @param requests An iterator over the inference requests to be executed.
     * @param listener A listener notified with the complete list of responses or a failure.
     */
    public void execute(BulkInferenceRequestIterator requests, ActionListener<List<InferenceAction.Response>> listener) {
        if (requests.hasNext() == false) {
            listener.onResponse(List.of());
            return;
        }

        final BulkInferenceExecutionState bulkExecutionState = new BulkInferenceExecutionState(
            bulkExecutionConfig.maxOutstandingRequests()
        );
        final ResponseHandler responseHandler = new ResponseHandler(bulkExecutionState, listener, requests.estimatedSize());

        while (bulkExecutionState.finished() == false && requests.hasNext()) {
            InferenceAction.Request request = requests.next();
            long seqNo = bulkExecutionState.generateSeqNo();

            if (requests.hasNext() == false) {
                bulkExecutionState.finish();
            }

            throttledInferenceRunner.doInference(
                request,
                ActionListener.runAfter(
                    ActionListener.wrap(
                        r -> bulkExecutionState.onInferenceResponse(seqNo, r),
                        e -> bulkExecutionState.onInferenceException(seqNo, e)
                    ),
                    responseHandler::persistPendingResponses
                )
            );
        }
    }

    /**
     * Handles collection and delivery of inference responses once they are complete.
     */
    private static class ResponseHandler {
        private final List<InferenceAction.Response> responses;
        private final ActionListener<List<InferenceAction.Response>> listener;
        private final BulkInferenceExecutionState bulkExecutionState;
        private final AtomicBoolean responseSent = new AtomicBoolean(false);

        private ResponseHandler(
            BulkInferenceExecutionState bulkExecutionState,
            ActionListener<List<InferenceAction.Response>> listener,
            int estimatedSize
        ) {
            this.listener = listener;
            this.bulkExecutionState = bulkExecutionState;
            this.responses = new ArrayList<>(estimatedSize);
        }

        /**
         * Persists all buffered responses that can be delivered in order, and sends the final response if all requests are finished.
         */
        public synchronized void persistPendingResponses() {
            long persistedSeqNo = bulkExecutionState.getPersistedCheckpoint();

            while (persistedSeqNo < bulkExecutionState.getProcessedCheckpoint()) {
                persistedSeqNo++;
                if (bulkExecutionState.hasFailure() == false) {
                    try {
                        InferenceAction.Response response = bulkExecutionState.fetchBufferedResponse(persistedSeqNo);
                        assert response != null;
                        responses.add(response);
                    } catch (Exception e) {
                        bulkExecutionState.addFailure(e);
                    }
                }
                bulkExecutionState.markSeqNoAsPersisted(persistedSeqNo);
            }

            sendResponseOnCompletion();
        }

        /**
         * Sends the final response or failure once all inference tasks have completed.
         */
        private void sendResponseOnCompletion() {
            if (bulkExecutionState.finished() && responseSent.compareAndSet(false, true)) {
                if (bulkExecutionState.hasFailure() == false) {
                    try {
                        listener.onResponse(responses);
                        return;
                    } catch (Exception e) {
                        bulkExecutionState.addFailure(e);
                    }
                }

                listener.onFailure(bulkExecutionState.getFailure());
            }
        }
    }

    /**
     * Manages throttled inference tasks execution.
     */
    private static class ThrottledInferenceRunner {
        private final InferenceRunner inferenceRunner;
        private final ExecutorService executorService;
        private final BlockingQueue<AbstractRunnable> pendingRequestsQueue;
        private final Semaphore permits;

        private ThrottledInferenceRunner(InferenceRunner inferenceRunner, ExecutorService executorService, int maxRunningTasks) {
            this.executorService = executorService;
            this.permits = new Semaphore(maxRunningTasks);
            this.inferenceRunner = inferenceRunner;
            this.pendingRequestsQueue = new ArrayBlockingQueue<>(maxRunningTasks);
        }

        /**
         * Creates a new {@code ThrottledInferenceRunner} with the specified configuration.
         *
         * @param inferenceRunner     TThe inference runner used to execute individual inference requests.
         * @param executorService     The executor used for asynchronous execution.
         * @param bulkExecutionConfig Configuration options (throttling and concurrency limits).
         */
        public static ThrottledInferenceRunner create(
            InferenceRunner inferenceRunner,
            ExecutorService executorService,
            BulkInferenceExecutionConfig bulkExecutionConfig
        ) {
            return new ThrottledInferenceRunner(inferenceRunner, executorService, bulkExecutionConfig.maxOutstandingRequests());
        }

        /**
         * Schedules the inference task for execution. If a permit is available, the task runs immediately; otherwise, it is queued.
         *
         * @param request  The inference request.
         * @param listener The listener to notify on response or failure.
         */
        public void doInference(InferenceAction.Request request, ActionListener<InferenceAction.Response> listener) {
            enqueueTask(request, listener);
            executePendingRequests();
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
        *        * @param listener The listener to notify on response or failure.
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
                    inferenceRunner.doInference(request, completionListener);
                }

                @Override
                public void onFailure(Exception e) {
                    completionListener.onFailure(e);
                }
            };
        }
    }

    private static ExecutorService executorService(ThreadPool threadPool) {
        return threadPool.executor(EsqlPlugin.ESQL_WORKER_THREAD_POOL_NAME);
    }
}
