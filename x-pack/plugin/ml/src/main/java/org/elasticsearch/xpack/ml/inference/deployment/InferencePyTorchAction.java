/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.deployment;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.core.TimeValue;
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
import java.util.Map;

class InferencePyTorchAction extends AbstractPyTorchAction<InferenceResults> {

    private static final Logger logger = LogManager.getLogger(InferencePyTorchAction.class);

    private final InferenceConfig config;
    private final Map<String, Object> doc;

    InferencePyTorchAction(
        String modelId,
        long requestId,
        TimeValue timeout,
        DeploymentManager.ProcessContext processContext,
        InferenceConfig config,
        Map<String, Object> doc,
        ThreadPool threadPool,
        ActionListener<InferenceResults> listener
    ) {
        super(modelId, requestId, timeout, processContext, threadPool, listener);
        this.config = config;
        this.doc = doc;
    }

    @Override
    protected void doRun() throws Exception {
        if (isNotified()) {
            // Should not execute request as it has already timed out while waiting in the queue
            logger.debug(
                () -> new ParameterizedMessage("[{}] skipping inference on request [{}] as it has timed out", getModelId(), getRequestId())
            );
            return;
        }

        final String requestIdStr = String.valueOf(getRequestId());
        try {
            // The request builder expect a list of inputs which are then batched.
            // TODO batching was implemented for expected use-cases such as zero-shot
            // classification but is not used here.
            List<String> text = Collections.singletonList(NlpTask.extractInput(getProcessContext().getModelInput().get(), doc));
            NlpTask.Processor processor = getProcessContext().getNlpTaskProcessor().get();
            processor.validateInputs(text);
            assert config instanceof NlpConfig;
            NlpConfig nlpConfig = (NlpConfig) config;
            NlpTask.Request request = processor.getRequestBuilder(nlpConfig)
                .buildRequest(text, requestIdStr, nlpConfig.getTokenization().getTruncate(), nlpConfig.getTokenization().getSpan());
            logger.debug(() -> "Inference Request " + request.processInput().utf8ToString());
            if (request.tokenization().anyTruncated()) {
                logger.debug("[{}] [{}] input truncated", getModelId(), getRequestId());
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
            logger.error(() -> "[" + getModelId() + "] error writing to inference process", e);
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

        logger.debug(() -> new ParameterizedMessage("[{}] retrieved result for request [{}]", getModelId(), getRequestId()));
        if (isNotified()) {
            // The request has timed out. No need to spend cycles processing the result.
            logger.debug(
                () -> new ParameterizedMessage(
                    "[{}] skipping result processing for request [{}] as the request has timed out",
                    getModelId(),
                    getRequestId()
                )
            );
            return;
        }
        InferenceResults results = inferenceResultsProcessor.processResult(tokenization, pyTorchResult.inferenceResult());
        logger.debug(() -> new ParameterizedMessage("[{}] processed result for request [{}]", getModelId(), getRequestId()));
        onSuccess(results);
    }

    @Override
    protected Logger getLogger() {
        return logger;
    }
}
