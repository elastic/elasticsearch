/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.inference.bulk;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.util.concurrent.ThrottledTaskRunner;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.inference.action.InferenceAction;
import org.elasticsearch.xpack.esql.inference.InferenceRunner;
import org.elasticsearch.xpack.esql.plugin.EsqlPlugin;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeoutException;

import static org.elasticsearch.common.logging.LoggerMessageFormat.format;

public class BulkInferenceExecutor<IR extends InferenceServiceResults, OutputType> {
    private static final String TASK_RUNNER_NAME = "bulk_inference_operation";
    private static final int INFERENCE_RESPONSE_TIMEOUT = 30; // TODO: should be in the config.
    private final ThrottledInferenceRunner throttledInferenceRunner;
    private final ExecutorService executorService;

    public BulkInferenceExecutor(InferenceRunner inferenceRunner, ThreadPool threadPool, BulkInferenceExecutionConfig bulkExecutionConfig) {
        executorService = executorService(threadPool);
        throttledInferenceRunner = ThrottledInferenceRunner.create(inferenceRunner, executorService, bulkExecutionConfig);
    }

    public void execute(
        BulkInferenceRequestIterator requests,
        BulkInferenceOutputBuilder<IR, OutputType> outputBuilder,
        ActionListener<OutputType> listener
    ) throws Exception {
        final ResponseHandler<IR, OutputType> responseHandler = new ResponseHandler<>(outputBuilder);
        runInferenceRequests(requests, listener.delegateFailureAndWrap(responseHandler::handleResponses));
    }

    private void runInferenceRequests(BulkInferenceRequestIterator requests, ActionListener<BulkInferenceExecutionState> listener) {
        final BulkInferenceExecutionState bulkExecutionState = new BulkInferenceExecutionState();
        try {
            executorService.execute(() -> {
                while (bulkExecutionState.finished() == false && requests.hasNext()) {
                    InferenceAction.Request request = requests.next();
                    long seqNo = bulkExecutionState.generateSeqNo();
                    throttledInferenceRunner.doInference(
                        request,
                        ActionListener.wrap(
                            r -> bulkExecutionState.onInferenceResponse(seqNo, r),
                            e -> bulkExecutionState.onInferenceException(seqNo, e)
                        )
                    );
                }
                bulkExecutionState.finish();
            });
        } catch (RejectedExecutionException e) {
            bulkExecutionState.addFailure(new IllegalStateException("Unable to enqueue inference requests", e));
            bulkExecutionState.finish();
        } finally {
            listener.onResponse(bulkExecutionState);
        }
    }

    private static class ResponseHandler<IR extends InferenceServiceResults, OutputType> {
        private final BulkInferenceOutputBuilder<IR, OutputType> outputBuilder;

        private ResponseHandler(BulkInferenceOutputBuilder<IR, OutputType> outputBuilder) {
            this.outputBuilder = outputBuilder;
        }

        private void handleResponses(ActionListener<OutputType> listener, BulkInferenceExecutionState bulkExecutionState) {

            try {
                persistsInferenceResponses(bulkExecutionState);
            } catch (InterruptedException | TimeoutException e) {
                bulkExecutionState.addFailure(e);
                bulkExecutionState.finish();
            }

            if (bulkExecutionState.hasFailure() == false) {
                try {
                    listener.onResponse(outputBuilder.buildOutput());
                    return;
                } catch (Exception e) {
                    bulkExecutionState.addFailure(e);
                }
            }

            listener.onFailure(bulkExecutionState.getFailure());
        }

        private void persistsInferenceResponses(BulkInferenceExecutionState bulkExecutionState) throws TimeoutException,
            InterruptedException {
            while (bulkExecutionState.finished() == false && bulkExecutionState.fetchProcessedSeqNo(INFERENCE_RESPONSE_TIMEOUT) >= 0) {
                long persistedSeqNo = bulkExecutionState.getPersistedCheckpoint();

                while (persistedSeqNo < bulkExecutionState.getProcessedCheckpoint()) {
                    persistedSeqNo++;
                    InferenceAction.Response response = bulkExecutionState.fetchBufferedResponse(persistedSeqNo);
                    assert response != null || bulkExecutionState.hasFailure();
                    if (bulkExecutionState.hasFailure() == false) {
                        try {
                            persistsInferenceResponse(response);
                        } catch (Exception e) {
                            bulkExecutionState.addFailure(e);
                        }
                    }
                    bulkExecutionState.markSeqNoAsPersisted(persistedSeqNo);
                }
            }
        }

        private void persistsInferenceResponse(InferenceAction.Response inferenceResponse) {
            InferenceServiceResults results = inferenceResponse.getResults();
            if (outputBuilder.inferenceResultsClass().isInstance(results)) {
                outputBuilder.addInferenceResults(outputBuilder.inferenceResultsClass().cast(results));
                return;
            }

            throw new IllegalStateException(
                format(
                    "Inference result has wrong type. Got [{}] while expecting [{}]",
                    results.getClass().getName(),
                    outputBuilder.inferenceResultsClass().getName()
                )
            );
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
