/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.azureaistudio;

import org.elasticsearch.inference.TaskType;

import java.util.List;

public final class AzureAiStudioProviderCapabilities {

    // these providers have embeddings inference
    public static final List<AzureAiStudioProvider> embeddingProviders = List.of(
        AzureAiStudioProvider.OPENAI,
        AzureAiStudioProvider.COHERE
    );

    // these providers have chat completion inference (all providers at the moment)
    public static final List<AzureAiStudioProvider> chatCompletionProviders = List.of(AzureAiStudioProvider.values());

    // these providers allow token ("pay as you go") embeddings endpoints
    public static final List<AzureAiStudioProvider> tokenEmbeddingsProviders = List.of(
        AzureAiStudioProvider.OPENAI,
        AzureAiStudioProvider.COHERE
    );

    // these providers allow realtime embeddings endpoints (none at the moment)
    public static final List<AzureAiStudioProvider> realtimeEmbeddingsProviders = List.of();

    // these providers allow token ("pay as you go") chat completion endpoints
    public static final List<AzureAiStudioProvider> tokenChatCompletionProviders = List.of(
        AzureAiStudioProvider.OPENAI,
        AzureAiStudioProvider.META,
        AzureAiStudioProvider.COHERE
    );

    // these providers allow realtime chat completion endpoints
    public static final List<AzureAiStudioProvider> realtimeChatCompletionProviders = List.of(
        AzureAiStudioProvider.MISTRAL,
        AzureAiStudioProvider.META,
        AzureAiStudioProvider.MICROSOFT_PHI,
        AzureAiStudioProvider.DATABRICKS
    );

    public static boolean providerAllowsTaskType(AzureAiStudioProvider provider, TaskType taskType) {
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

    public static boolean providerAllowsEndpointTypeForTask(
        AzureAiStudioProvider provider,
        TaskType taskType,
        AzureAiStudioEndpointType endpointType
    ) {
        switch (taskType) {
            case COMPLETION -> {
                return (endpointType == AzureAiStudioEndpointType.TOKEN)
                    ? tokenChatCompletionProviders.contains(provider)
                    : realtimeChatCompletionProviders.contains(provider);
            }
            case TEXT_EMBEDDING -> {
                return (endpointType == AzureAiStudioEndpointType.TOKEN)
                    ? tokenEmbeddingsProviders.contains(provider)
                    : realtimeEmbeddingsProviders.contains(provider);
            }
            default -> {
                return false;
            }
        }
    }

}
