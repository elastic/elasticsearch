/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.deployment;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.Strings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.inference.InferenceResults;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelPrefixStrings;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.InferenceConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.NlpConfig;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.ml.inference.nlp.NlpTask;
import org.elasticsearch.xpack.ml.inference.nlp.tokenizers.NlpTokenizer;
import org.elasticsearch.xpack.ml.inference.nlp.tokenizers.TokenizationResult;
import org.elasticsearch.xpack.ml.inference.pytorch.results.PyTorchResult;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.core.Strings.format;

class InferencePyTorchAction extends AbstractPyTorchAction<InferenceResults> {

    private static final Logger logger = LogManager.getLogger(InferencePyTorchAction.class);

    private final InferenceConfig config;
    private final NlpInferenceInput input;
    @Nullable
    private final CancellableTask parentActionTask;
    private final TrainedModelPrefixStrings.PrefixType prefixType;
    private final boolean chunkResponse;

    InferencePyTorchAction(
        String deploymentId,
        long requestId,
        TimeValue timeout,
        DeploymentManager.ProcessContext processContext,
        InferenceConfig config,
        NlpInferenceInput input,
        TrainedModelPrefixStrings.PrefixType prefixType,
        ThreadPool threadPool,
        @Nullable CancellableTask parentActionTask,
        boolean chunkResponse,
        ActionListener<InferenceResults> listener
    ) {
        super(deploymentId, requestId, timeout, processContext, threadPool, listener);
        this.config = config;
        this.input = input;
        this.prefixType = prefixType;
        this.parentActionTask = parentActionTask;
        this.chunkResponse = chunkResponse;
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
        final String requestIdStr = String.valueOf(getRequestId());
        if (isCancelled()) {
            onCancel();
            return;
        }
        try {
            String inputText = input.extractInput(getProcessContext().getModelInput().get());
            if (prefixType != TrainedModelPrefixStrings.PrefixType.NONE) {
                var prefixStrings = getProcessContext().getPrefixStrings().get();
                if (prefixStrings != null) {
                    switch (prefixType) {
                        case SEARCH: {
                            if (Strings.isNullOrEmpty(prefixStrings.searchPrefix()) == false) {
                                inputText = prefixStrings.searchPrefix() + inputText;
                            }
                        }
                            break;
                        case INGEST: {
                            if (Strings.isNullOrEmpty(prefixStrings.ingestPrefix()) == false) {
                                inputText = prefixStrings.ingestPrefix() + inputText;
                            }
                        }
                            break;
                        default:
                            throw new IllegalStateException("[" + getDeploymentId() + "] Unhandled input prefix type [" + prefixType + "]");
                    }
                }
            }

            // The request builder expect a list of inputs which are then batched.
            // TODO batching was implemented for expected use-cases such as zero-shot classification but is not used here.
            var inputs = List.of(inputText);

            NlpTask.Processor processor = getProcessContext().getNlpTaskProcessor().get();
            processor.validateInputs(inputs);
            assert config instanceof NlpConfig;
            NlpConfig nlpConfig = (NlpConfig) config;

            int span = nlpConfig.getTokenization().getSpan();
            if (chunkResponse && nlpConfig.getTokenization().getSpan() <= 0) {
                // set to special value that means find and use the default for chunking
                span = NlpTokenizer.CALC_DEFAULT_SPAN_VALUE;
            }

            NlpTask.Request request = processor.getRequestBuilder(nlpConfig)
                .buildRequest(
                    inputs,
                    requestIdStr,
                    nlpConfig.getTokenization().getTruncate(),
                    span,
                    nlpConfig.getTokenization().maxSequenceLength()
                );
            logger.debug(() -> format("handling request [%s]", requestIdStr));

            // Tokenization is non-trivial, so check for cancellation one last time before sending request to the native process
            if (isCancelled()) {
                onCancel();
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
        } catch (ElasticsearchException e) {
            // Don't log problems related to the shape of the input as errors
            if (e.status().getStatus() >= RestStatus.INTERNAL_SERVER_ERROR.getStatus()) {
                logger.error(() -> "[" + getDeploymentId() + "] internal server error running inference", e);
            } else {
                logger.debug(() -> "[" + getDeploymentId() + "] error running inference due to input", e);
            }
            onFailure(e);
        } catch (IllegalArgumentException e) {
            logger.debug(() -> "[" + getDeploymentId() + "] illegal argument running inference", e);
            onFailure(e);
        } catch (Exception e) {
            logger.error(() -> "[" + getDeploymentId() + "] error running inference", e);
            onFailure(e);
        }
    }

    private void processResult(
        PyTorchResult pyTorchResult,
        TokenizationResult tokenization,
        NlpTask.ResultProcessor inferenceResultsProcessor
    ) {
        if (pyTorchResult.isError()) {
            onFailure(pyTorchResult.errorResult());
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
            onCancel();
            return;
        }

        getProcessContext().getResultProcessor().updateStats(pyTorchResult);
        InferenceResults results = inferenceResultsProcessor.processResult(
            tokenization,
            pyTorchResult.inferenceResult(),
            this.chunkResponse
        );
        logger.debug(() -> format("[%s] processed result for request [%s]", getDeploymentId(), getRequestId()));
        onSuccess(results);
    }

    @Override
    protected Logger getLogger() {
        return logger;
    }
}
