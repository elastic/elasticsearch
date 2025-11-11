/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.elastic.action;

import org.elasticsearch.common.Strings;
import org.elasticsearch.xpack.inference.external.http.sender.EmbeddingsInput;
import org.elasticsearch.xpack.inference.external.http.sender.GenericRequestManager;
import org.elasticsearch.xpack.inference.external.http.sender.QueryAndDocsInputs;
import org.elasticsearch.xpack.inference.external.http.sender.RequestManager;
import org.elasticsearch.xpack.inference.services.ServiceComponents;
import org.elasticsearch.xpack.inference.services.elastic.ElasticInferenceServiceModel;
import org.elasticsearch.xpack.inference.services.elastic.ElasticInferenceServiceSparseEmbeddingsRequestManager;
import org.elasticsearch.xpack.inference.services.elastic.ccm.CCMAuthenticationApplierFactory;
import org.elasticsearch.xpack.inference.services.elastic.densetextembeddings.ElasticInferenceServiceDenseTextEmbeddingsModel;
import org.elasticsearch.xpack.inference.services.elastic.request.ElasticInferenceServiceDenseTextEmbeddingsRequest;
import org.elasticsearch.xpack.inference.services.elastic.request.ElasticInferenceServiceRerankRequest;
import org.elasticsearch.xpack.inference.services.elastic.rerank.ElasticInferenceServiceRerankModel;
import org.elasticsearch.xpack.inference.services.elastic.sparseembeddings.ElasticInferenceServiceSparseEmbeddingsModel;
import org.elasticsearch.xpack.inference.telemetry.TraceContext;

import static org.elasticsearch.xpack.inference.services.elastic.ElasticInferenceService.ELASTIC_INFERENCE_SERVICE_IDENTIFIER;
import static org.elasticsearch.xpack.inference.services.elastic.action.ElasticInferenceServiceActionCreator.DENSE_TEXT_EMBEDDINGS_HANDLER;
import static org.elasticsearch.xpack.inference.services.elastic.action.ElasticInferenceServiceActionCreator.RERANK_HANDLER;
import static org.elasticsearch.xpack.inference.services.elastic.request.ElasticInferenceServiceRequest.extractRequestMetadataFromThreadContext;

public record ModelStrategyFactory(ServiceComponents serviceComponents) {

    public interface Strategy<T extends ElasticInferenceServiceModel> {
        RequestManager createRequestManager(
            T model,
            ServiceComponents serviceComponents,
            TraceContext traceContext,
            CCMAuthenticationApplierFactory.AuthApplier authApplier
        );

        String requestDescription();
    }

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
            return Strings.format("%s sparse embeddings", ELASTIC_INFERENCE_SERVICE_IDENTIFIER);
        }
    };

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
            return Strings.format("%s rerank", ELASTIC_INFERENCE_SERVICE_IDENTIFIER);
        }
    };

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
                    embeddingsInput.getInputs(),
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
            return Strings.format("%s dense text embeddings", ELASTIC_INFERENCE_SERVICE_IDENTIFIER);
        }
    };

    @SuppressWarnings("unchecked")
    public static <T extends ElasticInferenceServiceModel> Strategy<T> getStrategy(T model) {
        return switch (model) {
            case ElasticInferenceServiceSparseEmbeddingsModel ignored -> (Strategy<T>) SPARSE_EMBEDDINGS_STRATEGY;
            case ElasticInferenceServiceRerankModel ignored -> (Strategy<T>) RERANK_STRATEGY;
            case ElasticInferenceServiceDenseTextEmbeddingsModel ignored -> (Strategy<T>) EMBEDDING_STRATEGY;
            default -> throw new IllegalArgumentException("No strategy found for model type: " + model.getClass().getSimpleName());
        };
    }
}
