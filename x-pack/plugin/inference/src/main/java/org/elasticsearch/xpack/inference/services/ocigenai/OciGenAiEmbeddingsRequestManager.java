/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.ocigenai;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.inference.common.Truncator;
import org.elasticsearch.xpack.inference.external.http.retry.RequestSender;
import org.elasticsearch.xpack.inference.external.http.retry.ResponseHandler;
import org.elasticsearch.xpack.inference.external.http.sender.EmbeddingsInput;
import org.elasticsearch.xpack.inference.external.http.sender.ExecutableInferenceRequest;
import org.elasticsearch.xpack.inference.external.http.sender.InferenceInputs;
import org.elasticsearch.xpack.inference.services.ocigenai.embeddings.OciGenAiEmbeddingsModel;
import org.elasticsearch.xpack.inference.services.ocigenai.request.OciGenAiEmbeddingsRequest;
import org.elasticsearch.xpack.inference.services.ocigenai.response.OciGenAiEmbeddingsResponseEntity;

import java.util.Objects;
import java.util.function.Supplier;

import static org.elasticsearch.xpack.inference.common.Truncator.truncate;

public class OciGenAiEmbeddingsRequestManager extends OciGenAiRequestManager {

    private static final Logger logger = LogManager.getLogger(OciGenAiEmbeddingsRequestManager.class);

    private static final ResponseHandler HANDLER = new OciGenAiResponseHandler(
        "OCI Generative AI embeddings",
        OciGenAiEmbeddingsResponseEntity::fromResponse
    );

    private final OciGenAiEmbeddingsModel model;
    private final Truncator truncator;

    public OciGenAiEmbeddingsRequestManager(OciGenAiEmbeddingsModel model, Truncator truncator, ThreadPool threadPool) {
        super(threadPool, model);
        this.model = Objects.requireNonNull(model);
        this.truncator = Objects.requireNonNull(truncator);
    }

    @Override
    public void execute(
        InferenceInputs inferenceInputs,
        RequestSender requestSender,
        Supplier<Boolean> hasRequestCompletedFunction,
        ActionListener<InferenceServiceResults> listener
    ) {
        var embeddingsInput = inferenceInputs.castTo(EmbeddingsInput.class);
        var truncatedInput = truncate(embeddingsInput.getTextInputs(), model.getServiceSettings().maxInputTokens());
        var request = new OciGenAiEmbeddingsRequest(truncator, truncatedInput, embeddingsInput.getInputType(), model);

        execute(new ExecutableInferenceRequest(requestSender, logger, request, HANDLER, hasRequestCompletedFunction, listener));
    }
}
