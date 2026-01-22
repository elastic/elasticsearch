/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.elastic.action;

import org.elasticsearch.common.Strings;
import org.elasticsearch.xpack.inference.external.http.retry.ResponseHandler;
import org.elasticsearch.xpack.inference.external.http.sender.EmbeddingsInput;
import org.elasticsearch.xpack.inference.external.http.sender.GenericRequestManager;
import org.elasticsearch.xpack.inference.external.http.sender.QueryAndDocsInputs;
import org.elasticsearch.xpack.inference.external.http.sender.RequestManager;
import org.elasticsearch.xpack.inference.external.response.elastic.ElasticInferenceServiceDenseTextEmbeddingsResponseEntity;
import org.elasticsearch.xpack.inference.external.response.elastic.ElasticInferenceServiceRerankResponseEntity;
import org.elasticsearch.xpack.inference.services.ServiceComponents;
import org.elasticsearch.xpack.inference.services.elastic.ElasticInferenceServiceModel;
import org.elasticsearch.xpack.inference.services.elastic.ElasticInferenceServiceResponseHandler;
import org.elasticsearch.xpack.inference.services.elastic.ElasticInferenceServiceSparseEmbeddingsRequestManager;
import org.elasticsearch.xpack.inference.services.elastic.ElasticInferenceServiceUnifiedCompletionRequestManager;
import org.elasticsearch.xpack.inference.services.elastic.ccm.CCMAuthenticationApplierFactory;
import org.elasticsearch.xpack.inference.services.elastic.completion.ElasticInferenceServiceCompletionModel;
import org.elasticsearch.xpack.inference.services.elastic.densetextembeddings.ElasticInferenceServiceDenseTextEmbeddingsModel;
import org.elasticsearch.xpack.inference.services.elastic.request.ElasticInferenceServiceDenseTextEmbeddingsRequest;
import org.elasticsearch.xpack.inference.services.elastic.request.ElasticInferenceServiceRerankRequest;
import org.elasticsearch.xpack.inference.services.elastic.rerank.ElasticInferenceServiceRerankModel;
import org.elasticsearch.xpack.inference.services.elastic.sparseembeddings.ElasticInferenceServiceSparseEmbeddingsModel;
import org.elasticsearch.xpack.inference.telemetry.TraceContext;

import static org.elasticsearch.xpack.inference.services.elastic.ElasticInferenceService.ELASTIC_INFERENCE_SERVICE_IDENTIFIER;
import static org.elasticsearch.xpack.inference.services.elastic.request.ElasticInferenceServiceRequest.extractRequestMetadataFromThreadContext;

record ModelStrategyFactory(ServiceComponents serviceComponents) {

    public interface Strategy<T extends ElasticInferenceServiceModel> {
        RequestManager createRequestManager(
            T model,
            ServiceComponents serviceComponents,
            TraceContext traceContext,
            CCMAuthenticationApplierFactory.AuthApplier authApplier
        );

        String requestDescription();
    }

    private static final String SPARSE_EMBEDDINGS_REQUEST_DESCRIPTION = Strings.format(
        "%s sparse embeddings",
        ELASTIC_INFERENCE_SERVICE_IDENTIFIER
    );

    private static final Strategy<ElasticInferenceServiceSparseEmbeddingsModel> SPARSE_EMBEDDINGS_STRATEGY = new Strategy<>() {
        @Override
        public RequestManager createRequestManager(
            ElasticInferenceServiceSparseEmbeddingsModel model,
            ServiceComponents serviceComponents,
            TraceContext traceContext,
            CCMAuthenticationApplierFactory.AuthApplier authApplier
        ) {
            return new ElasticInferenceServiceSparseEmbeddingsRequestManager(model, serviceComponents, traceContext, authApplier);
        }

        @Override
        public String requestDescription() {
            return SPARSE_EMBEDDINGS_REQUEST_DESCRIPTION;
        }
    };

    private static final String RERANK_REQUEST_DESCRIPTION = Strings.format("%s rerank", ELASTIC_INFERENCE_SERVICE_IDENTIFIER);

    private static final ResponseHandler RERANK_HANDLER = new ElasticInferenceServiceResponseHandler(
        RERANK_REQUEST_DESCRIPTION,
        (request, response) -> ElasticInferenceServiceRerankResponseEntity.fromResponse(response)
    );

    private static final Strategy<ElasticInferenceServiceRerankModel> RERANK_STRATEGY = new Strategy<>() {
        @Override
        public RequestManager createRequestManager(
            ElasticInferenceServiceRerankModel model,
            ServiceComponents serviceComponents,
            TraceContext traceContext,
            CCMAuthenticationApplierFactory.AuthApplier authApplier
        ) {
            var metadata = extractRequestMetadataFromThreadContext(serviceComponents.threadPool().getThreadContext());
            return new GenericRequestManager<>(
                serviceComponents.threadPool(),
                model,
                RERANK_HANDLER,
                (rerankInput) -> new ElasticInferenceServiceRerankRequest(
                    rerankInput.getQuery(),
                    rerankInput.getChunks(),
                    rerankInput.getTopN(),
                    model,
                    traceContext,
                    metadata,
                    authApplier
                ),
                QueryAndDocsInputs.class
            );
        }

        @Override
        public String requestDescription() {
            return RERANK_REQUEST_DESCRIPTION;
        }
    };

    private static final String DENSE_TEXT_EMBEDDINGS_REQUEST_DESCRIPTION = Strings.format(
        "%s dense text embeddings",
        ELASTIC_INFERENCE_SERVICE_IDENTIFIER
    );

    private static final ResponseHandler DENSE_TEXT_EMBEDDINGS_HANDLER = new ElasticInferenceServiceResponseHandler(
        DENSE_TEXT_EMBEDDINGS_REQUEST_DESCRIPTION,
        ElasticInferenceServiceDenseTextEmbeddingsResponseEntity::fromResponse
    );

    private static final Strategy<ElasticInferenceServiceDenseTextEmbeddingsModel> EMBEDDING_STRATEGY = new Strategy<>() {
        @Override
        public RequestManager createRequestManager(
            ElasticInferenceServiceDenseTextEmbeddingsModel model,
            ServiceComponents serviceComponents,
            TraceContext traceContext,
            CCMAuthenticationApplierFactory.AuthApplier authApplier
        ) {
            var metadata = extractRequestMetadataFromThreadContext(serviceComponents.threadPool().getThreadContext());
            return new GenericRequestManager<>(
                serviceComponents.threadPool(),
                model,
                DENSE_TEXT_EMBEDDINGS_HANDLER,
                (embeddingsInput) -> new ElasticInferenceServiceDenseTextEmbeddingsRequest(
                    model,
                    embeddingsInput.getTextInputs(),
                    traceContext,
                    metadata,
                    embeddingsInput.getInputType(),
                    authApplier
                ),
                EmbeddingsInput.class
            );
        }

        @Override
        public String requestDescription() {
            return DENSE_TEXT_EMBEDDINGS_REQUEST_DESCRIPTION;
        }
    };

    private static final String CHAT_COMPLETIONS_REQUEST_DESCRIPTION = Strings.format(
        "%s chat completions",
        ELASTIC_INFERENCE_SERVICE_IDENTIFIER
    );

    private static final Strategy<ElasticInferenceServiceCompletionModel> CHAT_COMPLETIONS_STRATEGY = new Strategy<>() {
        @Override
        public RequestManager createRequestManager(
            ElasticInferenceServiceCompletionModel model,
            ServiceComponents serviceComponents,
            TraceContext traceContext,
            CCMAuthenticationApplierFactory.AuthApplier authApplier
        ) {
            return ElasticInferenceServiceUnifiedCompletionRequestManager.of(
                model,
                serviceComponents.threadPool(),
                traceContext,
                authApplier
            );
        }

        @Override
        public String requestDescription() {
            return CHAT_COMPLETIONS_REQUEST_DESCRIPTION;
        }
    };

    @SuppressWarnings("unchecked")
    public static <T extends ElasticInferenceServiceModel> Strategy<T> getStrategy(T model) {
        return switch (model) {
            case ElasticInferenceServiceSparseEmbeddingsModel ignored -> (Strategy<T>) SPARSE_EMBEDDINGS_STRATEGY;
            case ElasticInferenceServiceRerankModel ignored -> (Strategy<T>) RERANK_STRATEGY;
            case ElasticInferenceServiceDenseTextEmbeddingsModel ignored -> (Strategy<T>) EMBEDDING_STRATEGY;
            case ElasticInferenceServiceCompletionModel ignored -> (Strategy<T>) CHAT_COMPLETIONS_STRATEGY;
            default -> throw new IllegalArgumentException("No strategy found for model type: " + model.getClass().getSimpleName());
        };
    }
}
