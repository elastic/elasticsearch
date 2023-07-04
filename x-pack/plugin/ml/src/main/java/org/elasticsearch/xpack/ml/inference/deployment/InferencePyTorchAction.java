/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.deployment;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.ml.inference.results.InferenceResults;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.InferenceConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.NlpConfig;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.ml.inference.nlp.NlpTask;
import org.elasticsearch.xpack.ml.inference.nlp.tokenizers.TokenizationResult;
import org.elasticsearch.xpack.ml.inference.pytorch.results.PyTorchResult;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import static org.elasticsearch.core.Strings.format;

class InferencePyTorchAction extends AbstractPyTorchAction<InferenceResults> {

    private static final Logger logger = LogManager.getLogger(InferencePyTorchAction.class);

    private final InferenceConfig config;
    private final NlpInferenceInput input;
    @Nullable
    private final CancellableTask parentActionTask;

    InferencePyTorchAction(
        String deploymentId,
        long requestId,
        TimeValue timeout,
        DeploymentManager.ProcessContext processContext,
        InferenceConfig config,
        NlpInferenceInput input,
        ThreadPool threadPool,
        @Nullable CancellableTask parentActionTask,
        ActionListener<InferenceResults> listener
    ) {
        super(deploymentId, requestId, timeout, processContext, threadPool, listener);
        this.config = config;
        this.input = input;
        this.parentActionTask = parentActionTask;
    }

    private boolean isCancelled() {
        if (parentActionTask != null) {
            try {
                parentActionTask.ensureNotCancelled();
            } catch (TaskCancelledException ex) {
                logger.warn(() -> format("[%s] %s", getDeploymentId(), ex.getMessage()));
                return true;
            }
        }
        return false;
    }

    @Override
    protected void doRun() throws Exception {
        if (isNotified()) {
            // Should not execute request as it has already timed out while waiting in the queue
            logger.debug(() -> format("[%s] skipping inference on request [%s] as it has timed out", getDeploymentId(), getRequestId()));
            return;
        }
        if (isCancelled()) {
            onFailure("inference task cancelled");
            return;
        }

        final String requestIdStr = String.valueOf(getRequestId());
        try {
            // The request builder expect a list of inputs which are then batched.
            // TODO batching was implemented for expected use-cases such as zero-shot classification but is not used here.
            List<String> text = Collections.singletonList(input.extractInput(getProcessContext().getModelInput().get()));
            NlpTask.Processor processor = getProcessContext().getNlpTaskProcessor().get();
            processor.validateInputs(text);
            assert config instanceof NlpConfig;
            NlpConfig nlpConfig = (NlpConfig) config;
            NlpTask.Request request = processor.getRequestBuilder(nlpConfig)
                .buildRequest(text, requestIdStr, nlpConfig.getTokenization().getTruncate(), nlpConfig.getTokenization().getSpan());
            logger.trace(() -> format("handling request [%s]", requestIdStr));

            // Tokenization is non-trivial, so check for cancellation one last time before sending request to the native process
            if (isCancelled()) {
                onFailure("inference task cancelled");
                return;
            }
            getProcessContext().getResultProcessor()
                .registerRequest(
                    requestIdStr,
                    ActionListener.wrap(
                        result -> processResult(result, request.tokenization(), processor.getResultProcessor(nlpConfig)),
                        this::onFailure
                    )
                );
            getProcessContext().getProcess().get().writeInferenceRequest(request.processInput());
        } catch (IOException e) {
            logger.error(() -> "[" + getDeploymentId() + "] error writing to inference process", e);
            onFailure(ExceptionsHelper.serverError("Error writing to inference process", e));
        } catch (Exception e) {
            onFailure(e);
        }
    }

    private void processResult(
        PyTorchResult pyTorchResult,
        TokenizationResult tokenization,
        NlpTask.ResultProcessor inferenceResultsProcessor
    ) {
        if (pyTorchResult.isError()) {
            onFailure(pyTorchResult.errorResult().error());
            return;
        }

        logger.debug(() -> format("[%s] retrieved result for request [%s]", getDeploymentId(), getRequestId()));
        if (isNotified()) {
            // The request has timed out. No need to spend cycles processing the result.
            logger.debug(
                () -> format(
                    "[%s] skipping result processing for request [%s] as the request has timed out",
                    getDeploymentId(),
                    getRequestId()
                )
            );
            return;
        }
        if (isCancelled()) {
            onFailure("inference task cancelled");
            return;
        }
        InferenceResults results = inferenceResultsProcessor.processResult(tokenization, pyTorchResult.inferenceResult());
        logger.trace(() -> format("[%s] processed result for request [%s]", getDeploymentId(), getRequestId()));
        onSuccess(results);
    }

    @Override
    protected Logger getLogger() {
        return logger;
    }
}
