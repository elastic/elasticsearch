/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.voyageai.action;

import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.inference.external.http.sender.RequestManager;
import org.elasticsearch.xpack.inference.services.voyageai.VoyageAIContextualEmbeddingsRequestManager;
import org.elasticsearch.xpack.inference.services.voyageai.VoyageAIEmbeddingsRequestManager;
import org.elasticsearch.xpack.inference.services.voyageai.VoyageAIModel;
import org.elasticsearch.xpack.inference.services.voyageai.VoyageAIMultimodalEmbeddingsRequestManager;
import org.elasticsearch.xpack.inference.services.voyageai.VoyageAIRerankRequestManager;
import org.elasticsearch.xpack.inference.services.voyageai.embeddings.contextual.VoyageAIContextualEmbeddingsModel;
import org.elasticsearch.xpack.inference.services.voyageai.embeddings.multimodal.VoyageAIMultimodalEmbeddingsModel;
import org.elasticsearch.xpack.inference.services.voyageai.embeddings.text.VoyageAIEmbeddingsModel;
import org.elasticsearch.xpack.inference.services.voyageai.rerank.VoyageAIRerankModel;

import java.util.Map;

/**
 * Factory for creating model-specific strategies for VoyageAI models.
 * Uses a type-safe approach to handle different model types.
 */
public class VoyageAIModelStrategyFactory {
    
    private static final VoyageAIModelStrategy<VoyageAIEmbeddingsModel> EMBEDDINGS_STRATEGY = 
        new VoyageAIModelStrategy<>() {
            @Override
            public VoyageAIEmbeddingsModel createOverriddenModel(VoyageAIEmbeddingsModel model, Map<String, Object> taskSettings) {
                return VoyageAIEmbeddingsModel.of(model, taskSettings);
            }

            @Override
            public RequestManager createRequestManager(VoyageAIEmbeddingsModel model, ThreadPool threadPool) {
                return VoyageAIEmbeddingsRequestManager.of(model, threadPool);
            }

            @Override
            public String getServiceName() {
                return "VoyageAI embeddings";
            }
        };

    private static final VoyageAIModelStrategy<VoyageAIMultimodalEmbeddingsModel> MULTIMODAL_EMBEDDINGS_STRATEGY = 
        new VoyageAIModelStrategy<>() {
            @Override
            public VoyageAIMultimodalEmbeddingsModel createOverriddenModel(
                VoyageAIMultimodalEmbeddingsModel model, 
                Map<String, Object> taskSettings
            ) {
                return VoyageAIMultimodalEmbeddingsModel.of(model, taskSettings);
            }

            @Override
            public RequestManager createRequestManager(VoyageAIMultimodalEmbeddingsModel model, ThreadPool threadPool) {
                return VoyageAIMultimodalEmbeddingsRequestManager.of(model, threadPool);
            }

            @Override
            public String getServiceName() {
                return "VoyageAI multimodal embeddings";
            }
        };

    private static final VoyageAIModelStrategy<VoyageAIContextualEmbeddingsModel> CONTEXTUAL_EMBEDDINGS_STRATEGY = 
        new VoyageAIModelStrategy<>() {
            @Override
            public VoyageAIContextualEmbeddingsModel createOverriddenModel(
                VoyageAIContextualEmbeddingsModel model, 
                Map<String, Object> taskSettings
            ) {
                return VoyageAIContextualEmbeddingsModel.of(model, taskSettings);
            }

            @Override
            public RequestManager createRequestManager(VoyageAIContextualEmbeddingsModel model, ThreadPool threadPool) {
                return VoyageAIContextualEmbeddingsRequestManager.of(model, threadPool);
            }

            @Override
            public String getServiceName() {
                return "VoyageAI contextual embeddings";
            }
        };

    private static final VoyageAIModelStrategy<VoyageAIRerankModel> RERANK_STRATEGY = 
        new VoyageAIModelStrategy<>() {
            @Override
            public VoyageAIRerankModel createOverriddenModel(VoyageAIRerankModel model, Map<String, Object> taskSettings) {
                return VoyageAIRerankModel.of(model, taskSettings);
            }

            @Override
            public RequestManager createRequestManager(VoyageAIRerankModel model, ThreadPool threadPool) {
                return VoyageAIRerankRequestManager.of(model, threadPool);
            }

            @Override
            public String getServiceName() {
                return "VoyageAI rerank";
            }
        };

    /**
     * Returns the appropriate strategy for the given model type.
     * This method uses type safety to ensure the correct strategy is returned.
     */
    @SuppressWarnings("unchecked")
    public static <T extends VoyageAIModel> VoyageAIModelStrategy<T> getStrategy(T model) {
        return switch (model) {
            case VoyageAIEmbeddingsModel ignored -> 
                (VoyageAIModelStrategy<T>) EMBEDDINGS_STRATEGY;
            case VoyageAIMultimodalEmbeddingsModel ignored -> 
                (VoyageAIModelStrategy<T>) MULTIMODAL_EMBEDDINGS_STRATEGY;
            case VoyageAIContextualEmbeddingsModel ignored -> 
                (VoyageAIModelStrategy<T>) CONTEXTUAL_EMBEDDINGS_STRATEGY;
            case VoyageAIRerankModel ignored -> 
                (VoyageAIModelStrategy<T>) RERANK_STRATEGY;
            default -> throw new IllegalArgumentException("Unsupported VoyageAI model type: " + model.getClass().getSimpleName());
        };
    }
}