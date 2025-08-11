/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.elastic;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.inference.InputType;
import org.elasticsearch.xpack.inference.common.Truncator;
import org.elasticsearch.xpack.inference.external.http.retry.RequestSender;
import org.elasticsearch.xpack.inference.external.http.retry.ResponseHandler;
import org.elasticsearch.xpack.inference.external.http.sender.EmbeddingsInput;
import org.elasticsearch.xpack.inference.external.http.sender.ExecutableInferenceRequest;
import org.elasticsearch.xpack.inference.external.http.sender.InferenceInputs;
import org.elasticsearch.xpack.inference.services.ServiceComponents;
import org.elasticsearch.xpack.inference.services.elastic.request.ElasticInferenceServiceSparseEmbeddingsRequest;
import org.elasticsearch.xpack.inference.services.elastic.response.ElasticInferenceServiceSparseEmbeddingsResponseEntity;
import org.elasticsearch.xpack.inference.telemetry.TraceContext;

import java.util.List;
import java.util.Locale;
import java.util.function.Supplier;

import static org.elasticsearch.xpack.inference.common.Truncator.truncate;
import static org.elasticsearch.xpack.inference.services.elastic.ElasticInferenceService.ELASTIC_INFERENCE_SERVICE_IDENTIFIER;

// TODO: remove and use GenericRequestManager in ElasticInferenceServiceActionCreator
public class ElasticInferenceServiceSparseEmbeddingsRequestManager extends ElasticInferenceServiceRequestManager {

    private static final Logger logger = LogManager.getLogger(ElasticInferenceServiceSparseEmbeddingsRequestManager.class);

    private static final ResponseHandler HANDLER = createSparseEmbeddingsHandler();

    private final ElasticInferenceServiceSparseEmbeddingsModel model;

    private final Truncator truncator;

    private final TraceContext traceContext;

    private static ResponseHandler createSparseEmbeddingsHandler() {
        return new ElasticInferenceServiceResponseHandler(
            String.format(Locale.ROOT, "%s sparse embeddings", ELASTIC_INFERENCE_SERVICE_IDENTIFIER),
            ElasticInferenceServiceSparseEmbeddingsResponseEntity::fromResponse
        );
    }

    public ElasticInferenceServiceSparseEmbeddingsRequestManager(
        ElasticInferenceServiceSparseEmbeddingsModel model,
        ServiceComponents serviceComponents,
        TraceContext traceContext
    ) {
        super(serviceComponents.threadPool(), model);
        this.model = model;
        this.truncator = serviceComponents.truncator();
        this.traceContext = traceContext;
    }

    @Override
    public void execute(
        InferenceInputs inferenceInputs,
        RequestSender requestSender,
        Supplier<Boolean> hasRequestCompletedFunction,
        ActionListener<InferenceServiceResults> listener
    ) {
        EmbeddingsInput input = EmbeddingsInput.of(inferenceInputs);
        List<String> docsInput = input.getStringInputs();
        InputType inputType = input.getInputType();

        var truncatedInput = truncate(docsInput, model.getServiceSettings().maxInputTokens());

        ElasticInferenceServiceSparseEmbeddingsRequest request = new ElasticInferenceServiceSparseEmbeddingsRequest(
            truncator,
            truncatedInput,
            model,
            traceContext,
            requestMetadata(),
            inputType
        );

        execute(new ExecutableInferenceRequest(requestSender, logger, request, HANDLER, hasRequestCompletedFunction, listener));
    }
}
