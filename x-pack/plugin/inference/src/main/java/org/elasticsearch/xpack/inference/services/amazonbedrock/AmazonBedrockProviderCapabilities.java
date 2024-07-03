/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.amazonbedrock;

import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.inference.TaskType;

import java.util.List;
import java.util.Map;

public final class AmazonBedrockProviderCapabilities {
    private static final List<AmazonBedrockProvider> embeddingProviders = List.of(
        AmazonBedrockProvider.AMAZONTITAN,
        AmazonBedrockProvider.COHERE
    );

    private static final List<AmazonBedrockProvider> chatCompletionProviders = List.of(
        AmazonBedrockProvider.AMAZONTITAN,
        AmazonBedrockProvider.ANTHROPIC,
        AmazonBedrockProvider.AI21LABS,
        AmazonBedrockProvider.COHERE,
        AmazonBedrockProvider.META,
        AmazonBedrockProvider.MISTRAL
    );

    private static final List<AmazonBedrockProvider> chatCompletionProvidersWithTopK = List.of(
        AmazonBedrockProvider.ANTHROPIC,
        AmazonBedrockProvider.COHERE,
        AmazonBedrockProvider.MISTRAL
    );

    private static final Map<AmazonBedrockProvider, SimilarityMeasure> embeddingsDefaultSimilarityMeasure = Map.of(
        AmazonBedrockProvider.AMAZONTITAN,
        SimilarityMeasure.COSINE,
        AmazonBedrockProvider.COHERE,
        SimilarityMeasure.DOT_PRODUCT
    );

    public static boolean providerAllowsTaskType(AmazonBedrockProvider provider, TaskType taskType) {
        switch (taskType) {
            case COMPLETION -> {
                return chatCompletionProviders.contains(provider);
            }
            case TEXT_EMBEDDING -> {
                return embeddingProviders.contains(provider);
            }
            default -> {
                return false;
            }
        }
    }

    public static boolean chatCompletionProviderHasTopKParameter(AmazonBedrockProvider provider) {
        return chatCompletionProvidersWithTopK.contains(provider);
    }

    public static SimilarityMeasure getProviderDefaultSimilarityMeasure(AmazonBedrockProvider provider) {
        if (embeddingsDefaultSimilarityMeasure.containsKey(provider)) {
            return embeddingsDefaultSimilarityMeasure.get(provider);
        }

        return SimilarityMeasure.COSINE;
    }

}
