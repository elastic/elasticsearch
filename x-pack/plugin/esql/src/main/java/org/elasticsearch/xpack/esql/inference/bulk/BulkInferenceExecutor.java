/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.inference.bulk;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.util.concurrent.ThrottledTaskRunner;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.inference.action.InferenceAction;
import org.elasticsearch.xpack.esql.inference.InferenceRunner;
import org.elasticsearch.xpack.esql.plugin.EsqlPlugin;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

public class BulkInferenceExecutor {
    private static final String TASK_RUNNER_NAME = "bulk_inference_operation";
    private final ThrottledInferenceRunner throttledInferenceRunner;

    public BulkInferenceExecutor(InferenceRunner inferenceRunner, ThreadPool threadPool, BulkInferenceExecutionConfig bulkExecutionConfig) {
        this.throttledInferenceRunner = ThrottledInferenceRunner.create(inferenceRunner, executorService(threadPool), bulkExecutionConfig);
    }

    public void execute(BulkInferenceRequestIterator requests, ActionListener<InferenceAction.Response[]> listener) throws Exception {
        if (requests.hasNext() == false) {
            listener.onResponse(new InferenceAction.Response[0]);
            return;
        }

        final BulkInferenceExecutionState bulkExecutionState = new BulkInferenceExecutionState();
        final ResponseHandler responseHandler = new ResponseHandler(bulkExecutionState, listener);

        while (bulkExecutionState.finished() == false && requests.hasNext()) {
            InferenceAction.Request request = requests.next();
            long seqNo = bulkExecutionState.generateSeqNo();
            throttledInferenceRunner.doInference(request, responseHandler.inferenceResponseListener(seqNo));
        }
        bulkExecutionState.finish();
    }

    private static class ResponseHandler {
        private final List<InferenceAction.Response> responses = new ArrayList<>();
        private final BulkInferenceExecutionState bulkExecutionState;
        private final ActionListener<InferenceAction.Response[]> completionListener;
        private final AtomicBoolean responseSent = new AtomicBoolean(false);

        private ResponseHandler(BulkInferenceExecutionState bulkExecutionState, ActionListener<InferenceAction.Response[]> completionListener) {
            this.bulkExecutionState = bulkExecutionState;
            this.completionListener = completionListener;
        }

        ActionListener<InferenceAction.Response> inferenceResponseListener(long seqNo) {
            return ActionListener.runAfter(ActionListener.wrap(
                r -> bulkExecutionState.onInferenceResponse(seqNo, r),
                e -> bulkExecutionState.onInferenceException(seqNo, e)
            ), this::persistPendingResponses);
        }

        private void persistPendingResponses() {
            synchronized (bulkExecutionState) {
                long persistedSeqNo = bulkExecutionState.getPersistedCheckpoint();

                while (persistedSeqNo < bulkExecutionState.getProcessedCheckpoint()) {
                    persistedSeqNo++;
                    InferenceAction.Response response = bulkExecutionState.fetchBufferedResponse(persistedSeqNo);
                    assert response != null || bulkExecutionState.hasFailure();

                    if (bulkExecutionState.hasFailure() == false) {
                        responses.add(response);
                    }

                    bulkExecutionState.markSeqNoAsPersisted(persistedSeqNo);
                }
            }

            sendResponseOnCompletion();
        }

        private void sendResponseOnCompletion() {
            if (bulkExecutionState.finished() && responseSent.compareAndSet(false, true)) {
                if (bulkExecutionState.hasFailure() == false) {
                    completionListener.onResponse(responses.toArray(InferenceAction.Response[]::new));
                    return;
                }

                completionListener.onFailure(bulkExecutionState.getFailure());
            }
        }
    }

    private static class ThrottledInferenceRunner extends ThrottledTaskRunner {
        private final InferenceRunner inferenceRunner;

        private ThrottledInferenceRunner(InferenceRunner inferenceRunner, ExecutorService executorService, int maxRunningTasks) {
            super(TASK_RUNNER_NAME, maxRunningTasks, executorService);
            this.inferenceRunner = inferenceRunner;
        }

        public static ThrottledInferenceRunner create(
            InferenceRunner inferenceRunner,
            ExecutorService executorService,
            BulkInferenceExecutionConfig bulkExecutionConfig
        ) {
            return new ThrottledInferenceRunner(inferenceRunner, executorService, bulkExecutionConfig.workers());
        }

        public void doInference(InferenceAction.Request request, ActionListener<InferenceAction.Response> listener) {
            this.enqueueTask(listener.delegateFailureAndWrap((l, releasable) -> {
                try (releasable) {
                    inferenceRunner.doInference(request, l);
                }
            }));
        }
    }

    private static ExecutorService executorService(ThreadPool threadPool) {
        return threadPool.executor(EsqlPlugin.ESQL_WORKER_THREAD_POOL_NAME);
    }
}
