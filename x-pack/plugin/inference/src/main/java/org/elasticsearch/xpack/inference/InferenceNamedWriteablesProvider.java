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
import org.elasticsearch.xpack.core.inference.results.ChunkedSparseEmbeddingResults;
import org.elasticsearch.xpack.core.inference.results.ChunkedTextEmbeddingByteResults;
import org.elasticsearch.xpack.core.inference.results.ChunkedTextEmbeddingFloatResults;
import org.elasticsearch.xpack.core.inference.results.ChunkedTextEmbeddingResults;
import org.elasticsearch.xpack.core.inference.results.ErrorChunkedInferenceResults;
import org.elasticsearch.xpack.core.inference.results.LegacyTextEmbeddingResults;
import org.elasticsearch.xpack.core.inference.results.RankedDocsResults;
import org.elasticsearch.xpack.core.inference.results.SparseEmbeddingResults;
import org.elasticsearch.xpack.core.inference.results.TextEmbeddingByteResults;
import org.elasticsearch.xpack.core.inference.results.TextEmbeddingResults;
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
import org.elasticsearch.xpack.inference.services.elasticsearch.ElasticsearchInternalServiceSettings;
import org.elasticsearch.xpack.inference.services.elasticsearch.MultilingualE5SmallInternalServiceSettings;
import org.elasticsearch.xpack.inference.services.elser.ElserInternalServiceSettings;
import org.elasticsearch.xpack.inference.services.elser.ElserMlNodeTaskSettings;
import org.elasticsearch.xpack.inference.services.googleaistudio.completion.GoogleAiStudioCompletionServiceSettings;
import org.elasticsearch.xpack.inference.services.googleaistudio.embeddings.GoogleAiStudioEmbeddingsServiceSettings;
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

        addInternalElserNamedWriteables(namedWriteables);

        // Internal TextEmbedding service config
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

        addHuggingFaceNamedWriteables(namedWriteables);
        addOpenAiNamedWriteables(namedWriteables);
        addCohereNamedWriteables(namedWriteables);
        addAzureOpenAiNamedWriteables(namedWriteables);
        addAzureAiStudioNamedWriteables(namedWriteables);
        addGoogleAiStudioNamedWritables(namedWriteables);
        addMistralNamedWriteables(namedWriteables);

        return namedWriteables;
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

    private static void addInternalElserNamedWriteables(List<NamedWriteableRegistry.Entry> namedWriteables) {
        namedWriteables.add(
            new NamedWriteableRegistry.Entry(ServiceSettings.class, ElserInternalServiceSettings.NAME, ElserInternalServiceSettings::new)
        );
        namedWriteables.add(
            new NamedWriteableRegistry.Entry(TaskSettings.class, ElserMlNodeTaskSettings.NAME, ElserMlNodeTaskSettings::new)
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
                ChunkedSparseEmbeddingResults.NAME,
                ChunkedSparseEmbeddingResults::new
            )
        );
        namedWriteables.add(
            new NamedWriteableRegistry.Entry(
                InferenceServiceResults.class,
                ChunkedTextEmbeddingResults.NAME,
                ChunkedTextEmbeddingResults::new
            )
        );
        namedWriteables.add(
            new NamedWriteableRegistry.Entry(
                InferenceServiceResults.class,
                ChunkedTextEmbeddingFloatResults.NAME,
                ChunkedTextEmbeddingFloatResults::new
            )
        );
        namedWriteables.add(
            new NamedWriteableRegistry.Entry(
                InferenceServiceResults.class,
                ChunkedTextEmbeddingByteResults.NAME,
                ChunkedTextEmbeddingByteResults::new
            )
        );
    }

    private static void addInferenceResultsNamedWriteables(List<NamedWriteableRegistry.Entry> namedWriteables) {
        namedWriteables.add(
            new NamedWriteableRegistry.Entry(InferenceServiceResults.class, SparseEmbeddingResults.NAME, SparseEmbeddingResults::new)
        );
        namedWriteables.add(
            new NamedWriteableRegistry.Entry(InferenceServiceResults.class, TextEmbeddingResults.NAME, TextEmbeddingResults::new)
        );
        namedWriteables.add(
            new NamedWriteableRegistry.Entry(InferenceServiceResults.class, TextEmbeddingByteResults.NAME, TextEmbeddingByteResults::new)
        );
        namedWriteables.add(
            new NamedWriteableRegistry.Entry(InferenceServiceResults.class, ChatCompletionResults.NAME, ChatCompletionResults::new)
        );
        namedWriteables.add(
            new NamedWriteableRegistry.Entry(InferenceServiceResults.class, RankedDocsResults.NAME, RankedDocsResults::new)
        );
    }

}
