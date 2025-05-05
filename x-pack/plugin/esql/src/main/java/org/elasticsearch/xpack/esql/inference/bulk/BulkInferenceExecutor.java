/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.inference.bulk;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.util.concurrent.ThrottledTaskRunner;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.inference.action.InferenceAction;
import org.elasticsearch.xpack.esql.inference.InferenceRunner;

import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeoutException;

public class BulkInferenceExecutor<InferenceResult extends InferenceServiceResults, OutputType> {
    private static final String TASK_RUNNER_NAME = "bulk_inference_operation";
    private final ThrottledInferenceRunner throttledInferenceRunner;

    public BulkInferenceExecutor(InferenceRunner inferenceRunner, ThreadPool threadPool, BulkInferenceExecutionConfig bulkExecutionConfig) {
        throttledInferenceRunner = ThrottledInferenceRunner.create(inferenceRunner, threadPool, bulkExecutionConfig);
    }

    public void execute(
        BulkInferenceRequestIterator requests,
        BulkInferenceOutputBuilder<InferenceResult, OutputType> outputBuilder,
        ActionListener<OutputType> listener
    ) {

        final ActionListener<OutputType> completionListener = ActionListener.releaseBefore(
            Releasables.wrap(requests, outputBuilder),
            listener
        );

        doExecute(requests, outputBuilder, completionListener);
    }

    private void doExecute(
        BulkInferenceRequestIterator requests,
        BulkInferenceOutputBuilder<InferenceResult, OutputType> outputBuilder,
        ActionListener<OutputType> listener
    ) {
        final BulkInferenceExecutionState<OutputType> bulkExecutionState = new BulkInferenceExecutionState<>();

        if (requests.hasNext() == false) {
            bulkExecutionState.markAllRequestsSent();
            bulkExecutionState.maybeSendResponse(outputBuilder::buildOutput, listener);
        }

        while (requests.hasNext() && bulkExecutionState.responseSent() == false) {
            long seqNo = bulkExecutionState.generateSeqNo();
            InferenceAction.Request request = requests.next();
            try {
                throttledInferenceRunner.doInference(
                    request,
                    ActionListener.runAfter(
                        ActionListener.wrap(
                            r -> bulkExecutionState.onInferenceResponse(seqNo, r),
                            e -> bulkExecutionState.onInferenceException(seqNo, e)
                        ),
                        () -> {
                            if (bulkExecutionState.responseSent()) {
                                return;
                            }

                            bulkExecutionState.persistsResponse(outputBuilder::onInferenceResponse);
                            bulkExecutionState.maybeSendResponse(outputBuilder::buildOutput, listener);
                        }
                    )
                );
            } catch (InterruptedException | TimeoutException e) {
                bulkExecutionState.addFailure(e);
                bulkExecutionState.maybeSendResponse(outputBuilder::buildOutput, listener);
            }
        }

        bulkExecutionState.markAllRequestsSent();
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

        public void doInference(InferenceAction.Request request, ActionListener<InferenceAction.Response> listener)
            throws InterruptedException, TimeoutException {
            this.enqueueTask(listener.delegateFailureAndWrap((l, releasable) -> {
                try (releasable) {
                    inferenceRunner.doInference(request, l);
                }
            }));
        }
    }

    private static ExecutorService executorService(ThreadPool threadPool) {
        return threadPool.executor(ThreadPool.Names.SEARCH);
    }
}
