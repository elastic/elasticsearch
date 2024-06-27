/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.amazonbedrock;

import org.elasticsearch.inference.TaskType;

import java.util.List;

public final class AmazonBedrockProviderCapabilities {
    public static final List<AmazonBedrockProvider> embeddingProviders = List.of(
        AmazonBedrockProvider.AmazonTitan,
        AmazonBedrockProvider.Cohere
    );

    public static final List<AmazonBedrockProvider> chatCompletionProviders = List.of(
        AmazonBedrockProvider.AmazonTitan,
        AmazonBedrockProvider.Anthropic,
        AmazonBedrockProvider.AI21Labs,
        AmazonBedrockProvider.Cohere,
        AmazonBedrockProvider.Meta,
        AmazonBedrockProvider.Mistral
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

}
