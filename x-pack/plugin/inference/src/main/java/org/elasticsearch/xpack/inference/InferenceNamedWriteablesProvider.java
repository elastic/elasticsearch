/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.inference.EmptyTaskSettings;
import org.elasticsearch.inference.InferenceResults;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.inference.SecretSettings;
import org.elasticsearch.inference.ServiceSettings;
import org.elasticsearch.inference.TaskSettings;
import org.elasticsearch.xpack.core.inference.results.ChatCompletionResults;
import org.elasticsearch.xpack.core.inference.results.ErrorChunkedInferenceResults;
import org.elasticsearch.xpack.core.inference.results.InferenceChunkedSparseEmbeddingResults;
import org.elasticsearch.xpack.core.inference.results.InferenceChunkedTextEmbeddingByteResults;
import org.elasticsearch.xpack.core.inference.results.InferenceChunkedTextEmbeddingFloatResults;
import org.elasticsearch.xpack.core.inference.results.InferenceTextEmbeddingByteResults;
import org.elasticsearch.xpack.core.inference.results.InferenceTextEmbeddingFloatResults;
import org.elasticsearch.xpack.core.inference.results.LegacyTextEmbeddingResults;
import org.elasticsearch.xpack.core.inference.results.RankedDocsResults;
import org.elasticsearch.xpack.core.inference.results.SparseEmbeddingResults;
import org.elasticsearch.xpack.inference.services.amazonbedrock.AmazonBedrockSecretSettings;
import org.elasticsearch.xpack.inference.services.amazonbedrock.completion.AmazonBedrockChatCompletionServiceSettings;
import org.elasticsearch.xpack.inference.services.amazonbedrock.completion.AmazonBedrockChatCompletionTaskSettings;
import org.elasticsearch.xpack.inference.services.amazonbedrock.embeddings.AmazonBedrockEmbeddingsServiceSettings;
import org.elasticsearch.xpack.inference.services.anthropic.completion.AnthropicChatCompletionServiceSettings;
import org.elasticsearch.xpack.inference.services.anthropic.completion.AnthropicChatCompletionTaskSettings;
import org.elasticsearch.xpack.inference.services.azureaistudio.completion.AzureAiStudioChatCompletionServiceSettings;
import org.elasticsearch.xpack.inference.services.azureaistudio.completion.AzureAiStudioChatCompletionTaskSettings;
import org.elasticsearch.xpack.inference.services.azureaistudio.embeddings.AzureAiStudioEmbeddingsServiceSettings;
import org.elasticsearch.xpack.inference.services.azureaistudio.embeddings.AzureAiStudioEmbeddingsTaskSettings;
import org.elasticsearch.xpack.inference.services.azureopenai.AzureOpenAiSecretSettings;
import org.elasticsearch.xpack.inference.services.azureopenai.completion.AzureOpenAiCompletionServiceSettings;
import org.elasticsearch.xpack.inference.services.azureopenai.completion.AzureOpenAiCompletionTaskSettings;
import org.elasticsearch.xpack.inference.services.azureopenai.embeddings.AzureOpenAiEmbeddingsServiceSettings;
import org.elasticsearch.xpack.inference.services.azureopenai.embeddings.AzureOpenAiEmbeddingsTaskSettings;
import org.elasticsearch.xpack.inference.services.cohere.CohereServiceSettings;
import org.elasticsearch.xpack.inference.services.cohere.completion.CohereCompletionServiceSettings;
import org.elasticsearch.xpack.inference.services.cohere.embeddings.CohereEmbeddingsServiceSettings;
import org.elasticsearch.xpack.inference.services.cohere.embeddings.CohereEmbeddingsTaskSettings;
import org.elasticsearch.xpack.inference.services.cohere.rerank.CohereRerankServiceSettings;
import org.elasticsearch.xpack.inference.services.cohere.rerank.CohereRerankTaskSettings;
import org.elasticsearch.xpack.inference.services.elasticsearch.CustomElandInternalServiceSettings;
import org.elasticsearch.xpack.inference.services.elasticsearch.CustomElandInternalTextEmbeddingServiceSettings;
import org.elasticsearch.xpack.inference.services.elasticsearch.CustomElandRerankTaskSettings;
import org.elasticsearch.xpack.inference.services.elasticsearch.ElasticsearchInternalServiceSettings;
import org.elasticsearch.xpack.inference.services.elasticsearch.MultilingualE5SmallInternalServiceSettings;
import org.elasticsearch.xpack.inference.services.elser.ElserInternalServiceSettings;
import org.elasticsearch.xpack.inference.services.elser.ElserMlNodeTaskSettings;
import org.elasticsearch.xpack.inference.services.googleaistudio.completion.GoogleAiStudioCompletionServiceSettings;
import org.elasticsearch.xpack.inference.services.googleaistudio.embeddings.GoogleAiStudioEmbeddingsServiceSettings;
import org.elasticsearch.xpack.inference.services.googlevertexai.GoogleVertexAiSecretSettings;
import org.elasticsearch.xpack.inference.services.googlevertexai.embeddings.GoogleVertexAiEmbeddingsServiceSettings;
import org.elasticsearch.xpack.inference.services.googlevertexai.embeddings.GoogleVertexAiEmbeddingsTaskSettings;
import org.elasticsearch.xpack.inference.services.googlevertexai.rerank.GoogleVertexAiRerankServiceSettings;
import org.elasticsearch.xpack.inference.services.googlevertexai.rerank.GoogleVertexAiRerankTaskSettings;
import org.elasticsearch.xpack.inference.services.huggingface.HuggingFaceServiceSettings;
import org.elasticsearch.xpack.inference.services.huggingface.elser.HuggingFaceElserServiceSettings;
import org.elasticsearch.xpack.inference.services.mistral.embeddings.MistralEmbeddingsServiceSettings;
import org.elasticsearch.xpack.inference.services.openai.completion.OpenAiChatCompletionServiceSettings;
import org.elasticsearch.xpack.inference.services.openai.completion.OpenAiChatCompletionTaskSettings;
import org.elasticsearch.xpack.inference.services.openai.embeddings.OpenAiEmbeddingsServiceSettings;
import org.elasticsearch.xpack.inference.services.openai.embeddings.OpenAiEmbeddingsTaskSettings;
import org.elasticsearch.xpack.inference.services.settings.DefaultSecretSettings;

import java.util.ArrayList;
import java.util.List;

public class InferenceNamedWriteablesProvider {

    private InferenceNamedWriteablesProvider() {}

    /**
     * Registers and provides the `NamedWriteable` objects.
     * Any new classes which implements NamedWriteable should be added here.
     * In practice, that is anything which implements TaskSettings, ServiceSettings, or InferenceServiceResults.
     */
    @SuppressWarnings("deprecation")
    public static List<NamedWriteableRegistry.Entry> getNamedWriteables() {
        List<NamedWriteableRegistry.Entry> namedWriteables = new ArrayList<>();

        // Legacy inference results
        namedWriteables.add(
            new NamedWriteableRegistry.Entry(InferenceResults.class, LegacyTextEmbeddingResults.NAME, LegacyTextEmbeddingResults::new)
        );

        addInferenceResultsNamedWriteables(namedWriteables);
        addChunkedInferenceResultsNamedWriteables(namedWriteables);

        // Empty default task settings
        namedWriteables.add(new NamedWriteableRegistry.Entry(TaskSettings.class, EmptyTaskSettings.NAME, EmptyTaskSettings::new));

        // Default secret settings
        namedWriteables.add(new NamedWriteableRegistry.Entry(SecretSettings.class, DefaultSecretSettings.NAME, DefaultSecretSettings::new));

        addInternalNamedWriteables(namedWriteables);

        addHuggingFaceNamedWriteables(namedWriteables);
        addOpenAiNamedWriteables(namedWriteables);
        addCohereNamedWriteables(namedWriteables);
        addAzureOpenAiNamedWriteables(namedWriteables);
        addAzureAiStudioNamedWriteables(namedWriteables);
        addGoogleAiStudioNamedWritables(namedWriteables);
        addGoogleVertexAiNamedWriteables(namedWriteables);
        addMistralNamedWriteables(namedWriteables);
        addCustomElandWriteables(namedWriteables);
        addAnthropicNamedWritables(namedWriteables);
        addAmazonBedrockNamedWriteables(namedWriteables);

        return namedWriteables;
    }

    private static void addAmazonBedrockNamedWriteables(List<NamedWriteableRegistry.Entry> namedWriteables) {
        namedWriteables.add(
            new NamedWriteableRegistry.Entry(
                AmazonBedrockSecretSettings.class,
                AmazonBedrockSecretSettings.NAME,
                AmazonBedrockSecretSettings::new
            )
        );

        namedWriteables.add(
            new NamedWriteableRegistry.Entry(
                ServiceSettings.class,
                AmazonBedrockEmbeddingsServiceSettings.NAME,
                AmazonBedrockEmbeddingsServiceSettings::new
            )
        );

        // no task settings for Amazon Bedrock Embeddings

        namedWriteables.add(
            new NamedWriteableRegistry.Entry(
                ServiceSettings.class,
                AmazonBedrockChatCompletionServiceSettings.NAME,
                AmazonBedrockChatCompletionServiceSettings::new
            )
        );
        namedWriteables.add(
            new NamedWriteableRegistry.Entry(
                TaskSettings.class,
                AmazonBedrockChatCompletionTaskSettings.NAME,
                AmazonBedrockChatCompletionTaskSettings::new
            )
        );
    }

    private static void addMistralNamedWriteables(List<NamedWriteableRegistry.Entry> namedWriteables) {
        namedWriteables.add(
            new NamedWriteableRegistry.Entry(
                ServiceSettings.class,
                MistralEmbeddingsServiceSettings.NAME,
                MistralEmbeddingsServiceSettings::new
            )
        );

        // note - no task settings for Mistral embeddings...
    }

    private static void addAzureAiStudioNamedWriteables(List<NamedWriteableRegistry.Entry> namedWriteables) {
        namedWriteables.add(
            new NamedWriteableRegistry.Entry(
                ServiceSettings.class,
                AzureAiStudioEmbeddingsServiceSettings.NAME,
                AzureAiStudioEmbeddingsServiceSettings::new
            )
        );
        namedWriteables.add(
            new NamedWriteableRegistry.Entry(
                TaskSettings.class,
                AzureAiStudioEmbeddingsTaskSettings.NAME,
                AzureAiStudioEmbeddingsTaskSettings::new
            )
        );

        namedWriteables.add(
            new NamedWriteableRegistry.Entry(
                ServiceSettings.class,
                AzureAiStudioChatCompletionServiceSettings.NAME,
                AzureAiStudioChatCompletionServiceSettings::new
            )
        );
        namedWriteables.add(
            new NamedWriteableRegistry.Entry(
                TaskSettings.class,
                AzureAiStudioChatCompletionTaskSettings.NAME,
                AzureAiStudioChatCompletionTaskSettings::new
            )
        );
    }

    private static void addAzureOpenAiNamedWriteables(List<NamedWriteableRegistry.Entry> namedWriteables) {
        namedWriteables.add(
            new NamedWriteableRegistry.Entry(
                AzureOpenAiSecretSettings.class,
                AzureOpenAiSecretSettings.NAME,
                AzureOpenAiSecretSettings::new
            )
        );

        namedWriteables.add(
            new NamedWriteableRegistry.Entry(
                ServiceSettings.class,
                AzureOpenAiEmbeddingsServiceSettings.NAME,
                AzureOpenAiEmbeddingsServiceSettings::new
            )
        );
        namedWriteables.add(
            new NamedWriteableRegistry.Entry(
                TaskSettings.class,
                AzureOpenAiEmbeddingsTaskSettings.NAME,
                AzureOpenAiEmbeddingsTaskSettings::new
            )
        );

        namedWriteables.add(
            new NamedWriteableRegistry.Entry(
                ServiceSettings.class,
                AzureOpenAiCompletionServiceSettings.NAME,
                AzureOpenAiCompletionServiceSettings::new
            )
        );
        namedWriteables.add(
            new NamedWriteableRegistry.Entry(
                TaskSettings.class,
                AzureOpenAiCompletionTaskSettings.NAME,
                AzureOpenAiCompletionTaskSettings::new
            )
        );
    }

    private static void addCohereNamedWriteables(List<NamedWriteableRegistry.Entry> namedWriteables) {
        namedWriteables.add(
            new NamedWriteableRegistry.Entry(ServiceSettings.class, CohereServiceSettings.NAME, CohereServiceSettings::new)
        );
        namedWriteables.add(
            new NamedWriteableRegistry.Entry(
                ServiceSettings.class,
                CohereEmbeddingsServiceSettings.NAME,
                CohereEmbeddingsServiceSettings::new
            )
        );
        namedWriteables.add(
            new NamedWriteableRegistry.Entry(TaskSettings.class, CohereEmbeddingsTaskSettings.NAME, CohereEmbeddingsTaskSettings::new)
        );
        namedWriteables.add(
            new NamedWriteableRegistry.Entry(ServiceSettings.class, CohereRerankServiceSettings.NAME, CohereRerankServiceSettings::new)
        );
        namedWriteables.add(
            new NamedWriteableRegistry.Entry(TaskSettings.class, CohereRerankTaskSettings.NAME, CohereRerankTaskSettings::new)
        );
        namedWriteables.add(
            new NamedWriteableRegistry.Entry(
                ServiceSettings.class,
                CohereCompletionServiceSettings.NAME,
                CohereCompletionServiceSettings::new
            )
        );
    }

    private static void addOpenAiNamedWriteables(List<NamedWriteableRegistry.Entry> namedWriteables) {
        namedWriteables.add(
            new NamedWriteableRegistry.Entry(
                ServiceSettings.class,
                OpenAiEmbeddingsServiceSettings.NAME,
                OpenAiEmbeddingsServiceSettings::new
            )
        );
        namedWriteables.add(
            new NamedWriteableRegistry.Entry(TaskSettings.class, OpenAiEmbeddingsTaskSettings.NAME, OpenAiEmbeddingsTaskSettings::new)
        );
        namedWriteables.add(
            new NamedWriteableRegistry.Entry(
                ServiceSettings.class,
                OpenAiChatCompletionServiceSettings.NAME,
                OpenAiChatCompletionServiceSettings::new
            )
        );
        namedWriteables.add(
            new NamedWriteableRegistry.Entry(
                TaskSettings.class,
                OpenAiChatCompletionTaskSettings.NAME,
                OpenAiChatCompletionTaskSettings::new
            )
        );
    }

    private static void addHuggingFaceNamedWriteables(List<NamedWriteableRegistry.Entry> namedWriteables) {
        namedWriteables.add(
            new NamedWriteableRegistry.Entry(
                ServiceSettings.class,
                HuggingFaceElserServiceSettings.NAME,
                HuggingFaceElserServiceSettings::new
            )
        );
        namedWriteables.add(
            new NamedWriteableRegistry.Entry(ServiceSettings.class, HuggingFaceServiceSettings.NAME, HuggingFaceServiceSettings::new)
        );
    }

    private static void addGoogleAiStudioNamedWritables(List<NamedWriteableRegistry.Entry> namedWriteables) {
        namedWriteables.add(
            new NamedWriteableRegistry.Entry(
                ServiceSettings.class,
                GoogleAiStudioCompletionServiceSettings.NAME,
                GoogleAiStudioCompletionServiceSettings::new
            )
        );
        namedWriteables.add(
            new NamedWriteableRegistry.Entry(
                ServiceSettings.class,
                GoogleAiStudioEmbeddingsServiceSettings.NAME,
                GoogleAiStudioEmbeddingsServiceSettings::new
            )
        );
    }

    private static void addGoogleVertexAiNamedWriteables(List<NamedWriteableRegistry.Entry> namedWriteables) {
        namedWriteables.add(
            new NamedWriteableRegistry.Entry(SecretSettings.class, GoogleVertexAiSecretSettings.NAME, GoogleVertexAiSecretSettings::new)
        );

        namedWriteables.add(
            new NamedWriteableRegistry.Entry(
                ServiceSettings.class,
                GoogleVertexAiEmbeddingsServiceSettings.NAME,
                GoogleVertexAiEmbeddingsServiceSettings::new
            )
        );

        namedWriteables.add(
            new NamedWriteableRegistry.Entry(
                TaskSettings.class,
                GoogleVertexAiEmbeddingsTaskSettings.NAME,
                GoogleVertexAiEmbeddingsTaskSettings::new
            )
        );

        namedWriteables.add(
            new NamedWriteableRegistry.Entry(
                ServiceSettings.class,
                GoogleVertexAiRerankServiceSettings.NAME,
                GoogleVertexAiRerankServiceSettings::new
            )
        );

        namedWriteables.add(
            new NamedWriteableRegistry.Entry(
                TaskSettings.class,
                GoogleVertexAiRerankTaskSettings.NAME,
                GoogleVertexAiRerankTaskSettings::new
            )
        );
    }

    private static void addInternalNamedWriteables(List<NamedWriteableRegistry.Entry> namedWriteables) {
        namedWriteables.add(
            new NamedWriteableRegistry.Entry(ServiceSettings.class, ElserInternalServiceSettings.NAME, ElserInternalServiceSettings::new)
        );
        namedWriteables.add(
            new NamedWriteableRegistry.Entry(TaskSettings.class, ElserMlNodeTaskSettings.NAME, ElserMlNodeTaskSettings::new)
        );
        namedWriteables.add(
            new NamedWriteableRegistry.Entry(
                ServiceSettings.class,
                ElasticsearchInternalServiceSettings.NAME,
                ElasticsearchInternalServiceSettings::new
            )
        );
        namedWriteables.add(
            new NamedWriteableRegistry.Entry(
                ServiceSettings.class,
                MultilingualE5SmallInternalServiceSettings.NAME,
                MultilingualE5SmallInternalServiceSettings::new
            )
        );

    }

    private static void addChunkedInferenceResultsNamedWriteables(List<NamedWriteableRegistry.Entry> namedWriteables) {
        namedWriteables.add(
            new NamedWriteableRegistry.Entry(
                InferenceServiceResults.class,
                ErrorChunkedInferenceResults.NAME,
                ErrorChunkedInferenceResults::new
            )
        );
        namedWriteables.add(
            new NamedWriteableRegistry.Entry(
                InferenceServiceResults.class,
                InferenceChunkedSparseEmbeddingResults.NAME,
                InferenceChunkedSparseEmbeddingResults::new
            )
        );
        namedWriteables.add(
            new NamedWriteableRegistry.Entry(
                InferenceServiceResults.class,
                InferenceChunkedTextEmbeddingFloatResults.NAME,
                InferenceChunkedTextEmbeddingFloatResults::new
            )
        );
        namedWriteables.add(
            new NamedWriteableRegistry.Entry(
                InferenceServiceResults.class,
                InferenceChunkedTextEmbeddingByteResults.NAME,
                InferenceChunkedTextEmbeddingByteResults::new
            )
        );
    }

    private static void addInferenceResultsNamedWriteables(List<NamedWriteableRegistry.Entry> namedWriteables) {
        namedWriteables.add(
            new NamedWriteableRegistry.Entry(InferenceServiceResults.class, SparseEmbeddingResults.NAME, SparseEmbeddingResults::new)
        );
        namedWriteables.add(
            new NamedWriteableRegistry.Entry(
                InferenceServiceResults.class,
                InferenceTextEmbeddingFloatResults.NAME,
                InferenceTextEmbeddingFloatResults::new
            )
        );
        namedWriteables.add(
            new NamedWriteableRegistry.Entry(
                InferenceServiceResults.class,
                InferenceTextEmbeddingByteResults.NAME,
                InferenceTextEmbeddingByteResults::new
            )
        );
        namedWriteables.add(
            new NamedWriteableRegistry.Entry(InferenceServiceResults.class, ChatCompletionResults.NAME, ChatCompletionResults::new)
        );
        namedWriteables.add(
            new NamedWriteableRegistry.Entry(InferenceServiceResults.class, RankedDocsResults.NAME, RankedDocsResults::new)
        );
    }

    private static void addCustomElandWriteables(final List<NamedWriteableRegistry.Entry> namedWriteables) {
        namedWriteables.add(
            new NamedWriteableRegistry.Entry(
                ServiceSettings.class,
                CustomElandInternalServiceSettings.NAME,
                CustomElandInternalServiceSettings::new
            )
        );
        namedWriteables.add(
            new NamedWriteableRegistry.Entry(
                ServiceSettings.class,
                CustomElandInternalTextEmbeddingServiceSettings.NAME,
                CustomElandInternalTextEmbeddingServiceSettings::new
            )
        );
        namedWriteables.add(
            new NamedWriteableRegistry.Entry(TaskSettings.class, CustomElandRerankTaskSettings.NAME, CustomElandRerankTaskSettings::new)
        );
    }

    private static void addAnthropicNamedWritables(List<NamedWriteableRegistry.Entry> namedWriteables) {
        namedWriteables.add(
            new NamedWriteableRegistry.Entry(
                ServiceSettings.class,
                AnthropicChatCompletionServiceSettings.NAME,
                AnthropicChatCompletionServiceSettings::new
            )
        );
        namedWriteables.add(
            new NamedWriteableRegistry.Entry(
                TaskSettings.class,
                AnthropicChatCompletionTaskSettings.NAME,
                AnthropicChatCompletionTaskSettings::new
            )
        );
    }
}
