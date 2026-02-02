/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.amazonbedrock;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xpack.inference.services.amazonbedrock.completion.AmazonBedrockChatCompletionModel;
import org.elasticsearch.xpack.inference.services.amazonbedrock.embeddings.AmazonBedrockEmbeddingsModel;

import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.inference.services.amazonbedrock.AmazonBedrockConstants.TOP_K_FIELD;

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

    private static final Map<AmazonBedrockProvider, Integer> embeddingsDefaultChunkSize = Map.of(
        AmazonBedrockProvider.AMAZONTITAN,
        8192,
        AmazonBedrockProvider.COHERE,
        2048
    );

    private static final Map<AmazonBedrockProvider, Integer> embeddingsMaxBatchSize = Map.of(
        AmazonBedrockProvider.AMAZONTITAN,
        1,
        AmazonBedrockProvider.COHERE,
        96
    );

    private static boolean providerAllowsTaskType(AmazonBedrockProvider provider, TaskType taskType) {
        switch (taskType) {
            case COMPLETION, CHAT_COMPLETION -> {
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

    private static boolean chatCompletionProviderHasTopKParameter(AmazonBedrockProvider provider) {
        return chatCompletionProvidersWithTopK.contains(provider);
    }

    public static SimilarityMeasure getProviderDefaultSimilarityMeasure(AmazonBedrockProvider provider) {
        if (embeddingsDefaultSimilarityMeasure.containsKey(provider)) {
            return embeddingsDefaultSimilarityMeasure.get(provider);
        }

        return SimilarityMeasure.COSINE;
    }

    public static int getEmbeddingsMaxBatchSize(AmazonBedrockProvider provider) {
        if (embeddingsMaxBatchSize.containsKey(provider)) {
            return embeddingsMaxBatchSize.get(provider);
        }

        return 1;
    }

    /**
     * Checks if the given provider supports the specified task type.
     * If not, throws an ElasticsearchStatusException with a BAD_REQUEST status.
     * @param taskType the task type to check
     * @param provider the Amazon Bedrock provider to check
     */
    public static void checkProviderForTask(TaskType taskType, AmazonBedrockProvider provider) {
        if (providerAllowsTaskType(provider, taskType) == false) {
            throw new ElasticsearchStatusException(
                Strings.format("The [%s] task type for provider [%s] is not available", taskType, provider),
                RestStatus.BAD_REQUEST
            );
        }
    }

    /**
     * Checks if the given chat completion model's provider supports the topK parameter.
     * If not, and if the topK parameter is set in the model's task settings,
     * throws an ElasticsearchStatusException with a BAD_REQUEST status.
     * @param model the Amazon Bedrock chat completion model to check
     */
    public static void checkChatCompletionProviderForTopKParameter(AmazonBedrockChatCompletionModel model) {
        var taskSettings = model.getTaskSettings();
        if (taskSettings.topK() != null && chatCompletionProviderHasTopKParameter(model.provider()) == false) {
            throw new ElasticsearchStatusException(
                Strings.format("The [%s] task parameter is not available for provider [%s]", TOP_K_FIELD, model.provider()),
                RestStatus.BAD_REQUEST
            );
        }
    }

    /**
     * Checks if the given text embedding model's provider allows the truncation field in task settings.
     * If not, and if the truncation field is set in the model's task settings,
     * throws an ElasticsearchStatusException with a BAD_REQUEST status.
     * @param model the Amazon Bedrock text embedding model to check
     */
    public static void checkTaskSettingsForTextEmbeddingModel(AmazonBedrockEmbeddingsModel model) {
        if (model.provider() != AmazonBedrockProvider.COHERE && model.getTaskSettings().truncation() != null) {
            throw new ElasticsearchStatusException(
                "The [{}] task type for provider [{}] does not allow [truncate] field",
                RestStatus.BAD_REQUEST,
                TaskType.TEXT_EMBEDDING,
                model.provider()
            );
        }
    }
}
