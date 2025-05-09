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

import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;

import static org.elasticsearch.common.logging.LoggerMessageFormat.format;

public class BulkInferenceExecutor<IR extends InferenceServiceResults, OutputType> {
    private static final String TASK_RUNNER_NAME = "bulk_inference_operation";
    private final ThrottledInferenceRunner throttledInferenceRunner;

    public BulkInferenceExecutor(InferenceRunner inferenceRunner, ThreadPool threadPool, BulkInferenceExecutionConfig bulkExecutionConfig) {
        throttledInferenceRunner = ThrottledInferenceRunner.create(inferenceRunner, threadPool, bulkExecutionConfig);
    }

    public void execute(
        BulkInferenceRequestIterator requests,
        BulkInferenceOutputBuilder<IR, OutputType> outputBuilder,
        ActionListener<OutputType> listener
    ) {
        try {
            listener.onResponse(doExecute(requests, outputBuilder));
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    public OutputType doExecute(BulkInferenceRequestIterator requests, BulkInferenceOutputBuilder<IR, OutputType> outputBuilder)
        throws Exception {
        final BulkInferenceExecutionState bulkExecutionState = new BulkInferenceExecutionState();

        if (requests.hasNext()) {
            enqueueRequests(requests, bulkExecutionState);
            persistsInferenceResponses(bulkExecutionState, this.inferenceResultPersister(outputBuilder));
        }

        if (bulkExecutionState.hasFailure()) {
            throw bulkExecutionState.getFailure();
        }

        return outputBuilder.buildOutput();
    }

    private Consumer<InferenceAction.Response> inferenceResultPersister(BulkInferenceOutputBuilder<IR, OutputType> outputBuilder) {
        return inferenceResponse -> {
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
        };
    }

    private void enqueueRequests(BulkInferenceRequestIterator requests, BulkInferenceExecutionState bulkExecutionState) {
        while (requests.hasNext()) {
            InferenceAction.Request request = requests.next();
            long seqNo = bulkExecutionState.generateSeqNo();
            ActionListener<InferenceAction.Response> listener = ActionListener.wrap(
                r -> bulkExecutionState.onInferenceResponse(seqNo, r),
                e -> bulkExecutionState.onInferenceException(seqNo, e)
            );
            throttledInferenceRunner.doInference(request, listener);
        }
    }

    private void persistsInferenceResponses(BulkInferenceExecutionState bulkExecutionState, Consumer<InferenceAction.Response> persister)
        throws TimeoutException {
        // TODO: retry should be from config
        int retry = 30;
        while (bulkExecutionState.getPersistedCheckpoint() < bulkExecutionState.getMaxSeqNo()) {
            Long seqNo = bulkExecutionState.fetchProcessedSeqNo();
            retry--;

            if (seqNo == null) {
                if (retry < 0) {
                    throw new TimeoutException("timeout waiting for inference response");
                }
                break;
            }

            retry = 30;

            long persistedSeqNo = bulkExecutionState.getPersistedCheckpoint();

            while (persistedSeqNo < bulkExecutionState.getProcessedCheckpoint()) {
                persistedSeqNo++;
                InferenceAction.Response response = bulkExecutionState.fetchBufferedResponse(persistedSeqNo);
                assert response != null || bulkExecutionState.hasFailure();
                if (bulkExecutionState.hasFailure() == false) {
                    try {
                        persister.accept(response);
                    } catch (Exception e) {
                        bulkExecutionState.addFailure(e);
                    }
                }
                bulkExecutionState.markSeqNoAsPersisted(persistedSeqNo);
            }
        }
    }

    private static class ThrottledInferenceRunner extends ThrottledTaskRunner {
        private final InferenceRunner inferenceRunner;

        private ThrottledInferenceRunner(InferenceRunner inferenceRunner, ThreadPool threadPool, int maxRunningTasks) {
            this(inferenceRunner, maxRunningTasks, executorService(threadPool));
        }

        private ThrottledInferenceRunner(InferenceRunner inferenceRunner, int maxRunningTasks, Executor executor) {
            super(TASK_RUNNER_NAME, maxRunningTasks, executor);
            this.inferenceRunner = inferenceRunner;
        }

        public static ThrottledInferenceRunner create(
            InferenceRunner inferenceRunner,
            ThreadPool threadPool,
            BulkInferenceExecutionConfig bulkExecutionConfig
        ) {
            return new ThrottledInferenceRunner(inferenceRunner, threadPool, bulkExecutionConfig.workers());
        }

        public void doInference(InferenceAction.Request request, ActionListener<InferenceAction.Response> listener) {
            this.enqueueTask(listener.delegateFailureAndWrap((l, releasable) -> {
                try (releasable) {
                    inferenceRunner.doInference(request, l);
                }
            }));
        }

        private static ExecutorService executorService(ThreadPool threadPool) {
            return threadPool.executor(EsqlPlugin.ESQL_WORKER_THREAD_POOL_NAME);
        }
    }
}
