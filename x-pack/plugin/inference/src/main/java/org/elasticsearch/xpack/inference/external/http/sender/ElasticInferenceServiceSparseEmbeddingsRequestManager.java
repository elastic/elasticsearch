/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.http.sender;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.xpack.inference.common.Truncator;
import org.elasticsearch.xpack.inference.external.elastic.ElasticInferenceServiceResponseHandler;
import org.elasticsearch.xpack.inference.external.http.retry.RequestSender;
import org.elasticsearch.xpack.inference.external.http.retry.ResponseHandler;
import org.elasticsearch.xpack.inference.external.request.elastic.ElasticInferenceServiceSparseEmbeddingsRequest;
import org.elasticsearch.xpack.inference.external.response.elastic.ElasticInferenceServiceSparseEmbeddingsResponseEntity;
import org.elasticsearch.xpack.inference.services.ServiceComponents;
import org.elasticsearch.xpack.inference.services.elastic.ElasticInferenceServiceSparseEmbeddingsModel;

import java.util.List;
import java.util.function.Supplier;

import static org.elasticsearch.xpack.inference.common.Truncator.truncate;

public class ElasticInferenceServiceSparseEmbeddingsRequestManager extends ElasticInferenceServiceRequestManager {

    private static final Logger logger = LogManager.getLogger(ElasticInferenceServiceSparseEmbeddingsRequestManager.class);

    private static final ResponseHandler HANDLER = createSparseEmbeddingsHandler();

    private final ElasticInferenceServiceSparseEmbeddingsModel model;

    private final Truncator truncator;

    private static ResponseHandler createSparseEmbeddingsHandler() {
        return new ElasticInferenceServiceResponseHandler(
            "Elastic Inference Service sparse embeddings",
            ElasticInferenceServiceSparseEmbeddingsResponseEntity::fromResponse
        );
    }

    public ElasticInferenceServiceSparseEmbeddingsRequestManager(
        ElasticInferenceServiceSparseEmbeddingsModel model,
        ServiceComponents serviceComponents
    ) {
        super(serviceComponents.threadPool(), model);
        this.model = model;
        this.truncator = serviceComponents.truncator();
    }

    @Override
    public void execute(
        InferenceInputs inferenceInputs,
        RequestSender requestSender,
        Supplier<Boolean> hasRequestCompletedFunction,
        ActionListener<InferenceServiceResults> listener
    ) {
        List<String> docsInput = DocumentsOnlyInput.of(inferenceInputs).getInputs();
        var truncatedInput = truncate(docsInput, model.getServiceSettings().maxInputTokens());

        ElasticInferenceServiceSparseEmbeddingsRequest request = new ElasticInferenceServiceSparseEmbeddingsRequest(
            truncator,
            truncatedInput,
            model
        );
        execute(new ExecutableInferenceRequest(requestSender, logger, request, HANDLER, hasRequestCompletedFunction, listener));
    }
}
