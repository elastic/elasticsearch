/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.inference.ChunkingSettings;
import org.elasticsearch.inference.EmptySecretSettings;
import org.elasticsearch.inference.EmptyTaskSettings;
import org.elasticsearch.inference.InferenceResults;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.inference.SecretSettings;
import org.elasticsearch.inference.ServiceSettings;
import org.elasticsearch.inference.TaskSettings;
import org.elasticsearch.inference.UnifiedCompletionRequest;
import org.elasticsearch.xpack.core.inference.results.ChatCompletionResults;
import org.elasticsearch.xpack.core.inference.results.LegacyTextEmbeddingResults;
import org.elasticsearch.xpack.core.inference.results.RankedDocsResults;
import org.elasticsearch.xpack.core.inference.results.SparseEmbeddingResults;
import org.elasticsearch.xpack.core.inference.results.StreamingChatCompletionResults;
import org.elasticsearch.xpack.core.inference.results.StreamingUnifiedChatCompletionResults;
import org.elasticsearch.xpack.core.inference.results.TextEmbeddingByteResults;
import org.elasticsearch.xpack.core.inference.results.TextEmbeddingFloatResults;
import org.elasticsearch.xpack.inference.action.task.StreamingTaskManager;
import org.elasticsearch.xpack.inference.chunking.SentenceBoundaryChunkingSettings;
import org.elasticsearch.xpack.inference.chunking.WordBoundaryChunkingSettings;
import org.elasticsearch.xpack.inference.common.amazon.AwsSecretSettings;
import org.elasticsearch.xpack.inference.services.alibabacloudsearch.AlibabaCloudSearchServiceSettings;
import org.elasticsearch.xpack.inference.services.alibabacloudsearch.completion.AlibabaCloudSearchCompletionServiceSettings;
import org.elasticsearch.xpack.inference.services.alibabacloudsearch.completion.AlibabaCloudSearchCompletionTaskSettings;
import org.elasticsearch.xpack.inference.services.alibabacloudsearch.embeddings.AlibabaCloudSearchEmbeddingsServiceSettings;
import org.elasticsearch.xpack.inference.services.alibabacloudsearch.embeddings.AlibabaCloudSearchEmbeddingsTaskSettings;
import org.elasticsearch.xpack.inference.services.alibabacloudsearch.rerank.AlibabaCloudSearchRerankServiceSettings;
import org.elasticsearch.xpack.inference.services.alibabacloudsearch.rerank.AlibabaCloudSearchRerankTaskSettings;
import org.elasticsearch.xpack.inference.services.alibabacloudsearch.sparse.AlibabaCloudSearchSparseServiceSettings;
import org.elasticsearch.xpack.inference.services.alibabacloudsearch.sparse.AlibabaCloudSearchSparseTaskSettings;
import org.elasticsearch.xpack.inference.services.amazonbedrock.completion.AmazonBedrockChatCompletionServiceSettings;
import org.elasticsearch.xpack.inference.services.amazonbedrock.completion.AmazonBedrockChatCompletionTaskSettings;
import org.elasticsearch.xpack.inference.services.amazonbedrock.embeddings.AmazonBedrockEmbeddingsServiceSettings;
import org.elasticsearch.xpack.inference.services.amazonbedrock.embeddings.AmazonBedrockEmbeddingsTaskSettings;
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
import org.elasticsearch.xpack.inference.services.custom.CustomSecretSettings;
import org.elasticsearch.xpack.inference.services.custom.CustomServiceSettings;
import org.elasticsearch.xpack.inference.services.custom.CustomTaskSettings;
import org.elasticsearch.xpack.inference.services.custom.response.CompletionResponseParser;
import org.elasticsearch.xpack.inference.services.custom.response.CustomResponseParser;
import org.elasticsearch.xpack.inference.services.custom.response.NoopResponseParser;
import org.elasticsearch.xpack.inference.services.custom.response.RerankResponseParser;
import org.elasticsearch.xpack.inference.services.custom.response.SparseEmbeddingResponseParser;
import org.elasticsearch.xpack.inference.services.custom.response.TextEmbeddingResponseParser;
import org.elasticsearch.xpack.inference.services.deepseek.DeepSeekChatCompletionModel;
import org.elasticsearch.xpack.inference.services.elastic.completion.ElasticInferenceServiceCompletionServiceSettings;
import org.elasticsearch.xpack.inference.services.elastic.sparseembeddings.ElasticInferenceServiceSparseEmbeddingsServiceSettings;
import org.elasticsearch.xpack.inference.services.elasticsearch.CustomElandInternalServiceSettings;
import org.elasticsearch.xpack.inference.services.elasticsearch.CustomElandInternalTextEmbeddingServiceSettings;
import org.elasticsearch.xpack.inference.services.elasticsearch.ElasticRerankerServiceSettings;
import org.elasticsearch.xpack.inference.services.elasticsearch.ElasticsearchInternalServiceSettings;
import org.elasticsearch.xpack.inference.services.elasticsearch.ElserInternalServiceSettings;
import org.elasticsearch.xpack.inference.services.elasticsearch.ElserMlNodeTaskSettings;
import org.elasticsearch.xpack.inference.services.elasticsearch.MultilingualE5SmallInternalServiceSettings;
import org.elasticsearch.xpack.inference.services.elasticsearch.RerankTaskSettings;
import org.elasticsearch.xpack.inference.services.googleaistudio.completion.GoogleAiStudioCompletionServiceSettings;
import org.elasticsearch.xpack.inference.services.googleaistudio.embeddings.GoogleAiStudioEmbeddingsServiceSettings;
import org.elasticsearch.xpack.inference.services.googlevertexai.GoogleVertexAiSecretSettings;
import org.elasticsearch.xpack.inference.services.googlevertexai.embeddings.GoogleVertexAiEmbeddingsServiceSettings;
import org.elasticsearch.xpack.inference.services.googlevertexai.embeddings.GoogleVertexAiEmbeddingsTaskSettings;
import org.elasticsearch.xpack.inference.services.googlevertexai.rerank.GoogleVertexAiRerankServiceSettings;
import org.elasticsearch.xpack.inference.services.googlevertexai.rerank.GoogleVertexAiRerankTaskSettings;
import org.elasticsearch.xpack.inference.services.huggingface.HuggingFaceServiceSettings;
import org.elasticsearch.xpack.inference.services.huggingface.completion.HuggingFaceChatCompletionServiceSettings;
import org.elasticsearch.xpack.inference.services.huggingface.elser.HuggingFaceElserServiceSettings;
import org.elasticsearch.xpack.inference.services.ibmwatsonx.embeddings.IbmWatsonxEmbeddingsServiceSettings;
import org.elasticsearch.xpack.inference.services.ibmwatsonx.rerank.IbmWatsonxRerankServiceSettings;
import org.elasticsearch.xpack.inference.services.ibmwatsonx.rerank.IbmWatsonxRerankTaskSettings;
import org.elasticsearch.xpack.inference.services.jinaai.JinaAIServiceSettings;
import org.elasticsearch.xpack.inference.services.jinaai.embeddings.JinaAIEmbeddingsServiceSettings;
import org.elasticsearch.xpack.inference.services.jinaai.embeddings.JinaAIEmbeddingsTaskSettings;
import org.elasticsearch.xpack.inference.services.jinaai.rerank.JinaAIRerankServiceSettings;
import org.elasticsearch.xpack.inference.services.jinaai.rerank.JinaAIRerankTaskSettings;
import org.elasticsearch.xpack.inference.services.mistral.embeddings.MistralEmbeddingsServiceSettings;
import org.elasticsearch.xpack.inference.services.openai.completion.OpenAiChatCompletionServiceSettings;
import org.elasticsearch.xpack.inference.services.openai.completion.OpenAiChatCompletionTaskSettings;
import org.elasticsearch.xpack.inference.services.openai.embeddings.OpenAiEmbeddingsServiceSettings;
import org.elasticsearch.xpack.inference.services.openai.embeddings.OpenAiEmbeddingsTaskSettings;
import org.elasticsearch.xpack.inference.services.sagemaker.model.SageMakerModel;
import org.elasticsearch.xpack.inference.services.sagemaker.schema.SageMakerSchemas;
import org.elasticsearch.xpack.inference.services.settings.DefaultSecretSettings;
import org.elasticsearch.xpack.inference.services.voyageai.VoyageAIServiceSettings;
import org.elasticsearch.xpack.inference.services.voyageai.embeddings.VoyageAIEmbeddingsServiceSettings;
import org.elasticsearch.xpack.inference.services.voyageai.embeddings.VoyageAIEmbeddingsTaskSettings;
import org.elasticsearch.xpack.inference.services.voyageai.rerank.VoyageAIRerankServiceSettings;
import org.elasticsearch.xpack.inference.services.voyageai.rerank.VoyageAIRerankTaskSettings;

import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.xpack.inference.CustomServiceFeatureFlag.CUSTOM_SERVICE_FEATURE_FLAG;

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

        // Empty default task settings
        namedWriteables.add(new NamedWriteableRegistry.Entry(TaskSettings.class, EmptyTaskSettings.NAME, EmptyTaskSettings::new));

        addChunkingSettingsNamedWriteables(namedWriteables);

        // Empty default secret settings
        namedWriteables.add(new NamedWriteableRegistry.Entry(SecretSettings.class, EmptySecretSettings.NAME, EmptySecretSettings::new));

        // Default secret settings
        namedWriteables.add(new NamedWriteableRegistry.Entry(SecretSettings.class, DefaultSecretSettings.NAME, DefaultSecretSettings::new));

        addInternalNamedWriteables(namedWriteables);

        addHuggingFaceNamedWriteables(namedWriteables);
        addOpenAiNamedWriteables(namedWriteables);
        addCohereNamedWriteables(namedWriteables);
        addAzureOpenAiNamedWriteables(namedWriteables);
        addAzureAiStudioNamedWriteables(namedWriteables);
        addGoogleAiStudioNamedWritables(namedWriteables);
        addIbmWatsonxNamedWritables(namedWriteables);
        addGoogleVertexAiNamedWriteables(namedWriteables);
        addMistralNamedWriteables(namedWriteables);
        addCustomElandWriteables(namedWriteables);
        addAnthropicNamedWritables(namedWriteables);
        addAmazonBedrockNamedWriteables(namedWriteables);
        addAwsNamedWriteables(namedWriteables);
        addEisNamedWriteables(namedWriteables);
        addAlibabaCloudSearchNamedWriteables(namedWriteables);
        addJinaAINamedWriteables(namedWriteables);
        addVoyageAINamedWriteables(namedWriteables);
        addCustomNamedWriteables(namedWriteables);

        addUnifiedNamedWriteables(namedWriteables);

        namedWriteables.addAll(StreamingTaskManager.namedWriteables());
        namedWriteables.addAll(DeepSeekChatCompletionModel.namedWriteables());
        namedWriteables.addAll(SageMakerModel.namedWriteables());
        namedWriteables.addAll(SageMakerSchemas.namedWriteables());

        return namedWriteables;
    }

    private static void addCustomNamedWriteables(List<NamedWriteableRegistry.Entry> namedWriteables) {
        if (CUSTOM_SERVICE_FEATURE_FLAG.isEnabled() == false) {
            return;
        }

        namedWriteables.add(
            new NamedWriteableRegistry.Entry(ServiceSettings.class, CustomServiceSettings.NAME, CustomServiceSettings::new)
        );

        namedWriteables.add(new NamedWriteableRegistry.Entry(TaskSettings.class, CustomTaskSettings.NAME, CustomTaskSettings::new));

        namedWriteables.add(new NamedWriteableRegistry.Entry(SecretSettings.class, CustomSecretSettings.NAME, CustomSecretSettings::new));

        namedWriteables.add(
            new NamedWriteableRegistry.Entry(CustomResponseParser.class, TextEmbeddingResponseParser.NAME, TextEmbeddingResponseParser::new)
        );

        namedWriteables.add(
            new NamedWriteableRegistry.Entry(
                CustomResponseParser.class,
                SparseEmbeddingResponseParser.NAME,
                SparseEmbeddingResponseParser::new
            )
        );

        namedWriteables.add(
            new NamedWriteableRegistry.Entry(CustomResponseParser.class, RerankResponseParser.NAME, RerankResponseParser::new)
        );

        namedWriteables.add(new NamedWriteableRegistry.Entry(CustomResponseParser.class, NoopResponseParser.NAME, NoopResponseParser::new));

        namedWriteables.add(
            new NamedWriteableRegistry.Entry(CustomResponseParser.class, CompletionResponseParser.NAME, CompletionResponseParser::new)
        );
    }

    private static void addUnifiedNamedWriteables(List<NamedWriteableRegistry.Entry> namedWriteables) {
        var writeables = UnifiedCompletionRequest.getNamedWriteables();
        namedWriteables.addAll(writeables);
    }

    private static void addAmazonBedrockNamedWriteables(List<NamedWriteableRegistry.Entry> namedWriteables) {
        namedWriteables.add(
            new NamedWriteableRegistry.Entry(
                ServiceSettings.class,
                AmazonBedrockEmbeddingsServiceSettings.NAME,
                AmazonBedrockEmbeddingsServiceSettings::new
            )
        );
        namedWriteables.add(
            new NamedWriteableRegistry.Entry(
                TaskSettings.class,
                AmazonBedrockEmbeddingsTaskSettings.NAME,
                AmazonBedrockEmbeddingsTaskSettings::new
            )
        );

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

    private static void addAwsNamedWriteables(List<NamedWriteableRegistry.Entry> namedWriteables) {
        namedWriteables.add(new NamedWriteableRegistry.Entry(AwsSecretSettings.class, AwsSecretSettings.NAME, AwsSecretSettings::new));
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
        namedWriteables.add(
            new NamedWriteableRegistry.Entry(
                ServiceSettings.class,
                HuggingFaceChatCompletionServiceSettings.NAME,
                HuggingFaceChatCompletionServiceSettings::new
            )
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

    private static void addIbmWatsonxNamedWritables(List<NamedWriteableRegistry.Entry> namedWriteables) {
        namedWriteables.add(
            new NamedWriteableRegistry.Entry(
                ServiceSettings.class,
                IbmWatsonxEmbeddingsServiceSettings.NAME,
                IbmWatsonxEmbeddingsServiceSettings::new
            )
        );

        namedWriteables.add(
            new NamedWriteableRegistry.Entry(
                ServiceSettings.class,
                IbmWatsonxRerankServiceSettings.NAME,
                IbmWatsonxRerankServiceSettings::new
            )
        );
        namedWriteables.add(
            new NamedWriteableRegistry.Entry(TaskSettings.class, IbmWatsonxRerankTaskSettings.NAME, IbmWatsonxRerankTaskSettings::new)
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
        namedWriteables.add(
            new NamedWriteableRegistry.Entry(
                ServiceSettings.class,
                ElasticRerankerServiceSettings.NAME,
                ElasticRerankerServiceSettings::new
            )
        );
    }

    private static void addChunkingSettingsNamedWriteables(List<NamedWriteableRegistry.Entry> namedWriteables) {
        namedWriteables.add(
            new NamedWriteableRegistry.Entry(ChunkingSettings.class, WordBoundaryChunkingSettings.NAME, WordBoundaryChunkingSettings::new)
        );
        namedWriteables.add(
            new NamedWriteableRegistry.Entry(
                ChunkingSettings.class,
                SentenceBoundaryChunkingSettings.NAME,
                SentenceBoundaryChunkingSettings::new
            )
        );
    }

    private static void addInferenceResultsNamedWriteables(List<NamedWriteableRegistry.Entry> namedWriteables) {
        namedWriteables.add(
            new NamedWriteableRegistry.Entry(InferenceServiceResults.class, SparseEmbeddingResults.NAME, SparseEmbeddingResults::new)
        );
        namedWriteables.add(
            new NamedWriteableRegistry.Entry(InferenceServiceResults.class, TextEmbeddingFloatResults.NAME, TextEmbeddingFloatResults::new)
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
        namedWriteables.add(
            new NamedWriteableRegistry.Entry(
                StreamingChatCompletionResults.Results.class,
                StreamingChatCompletionResults.Results.NAME,
                StreamingChatCompletionResults.Results::new
            )
        );
        namedWriteables.add(
            new NamedWriteableRegistry.Entry(
                StreamingUnifiedChatCompletionResults.Results.class,
                StreamingUnifiedChatCompletionResults.Results.NAME,
                StreamingUnifiedChatCompletionResults.Results::new
            )
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
        namedWriteables.add(new NamedWriteableRegistry.Entry(TaskSettings.class, RerankTaskSettings.NAME, RerankTaskSettings::new));
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

    private static void addAlibabaCloudSearchNamedWriteables(List<NamedWriteableRegistry.Entry> namedWriteables) {
        namedWriteables.add(
            new NamedWriteableRegistry.Entry(
                ServiceSettings.class,
                AlibabaCloudSearchServiceSettings.NAME,
                AlibabaCloudSearchServiceSettings::new
            )
        );
        namedWriteables.add(
            new NamedWriteableRegistry.Entry(
                ServiceSettings.class,
                AlibabaCloudSearchEmbeddingsServiceSettings.NAME,
                AlibabaCloudSearchEmbeddingsServiceSettings::new
            )
        );
        namedWriteables.add(
            new NamedWriteableRegistry.Entry(
                TaskSettings.class,
                AlibabaCloudSearchEmbeddingsTaskSettings.NAME,
                AlibabaCloudSearchEmbeddingsTaskSettings::new
            )
        );
        namedWriteables.add(
            new NamedWriteableRegistry.Entry(
                ServiceSettings.class,
                AlibabaCloudSearchSparseServiceSettings.NAME,
                AlibabaCloudSearchSparseServiceSettings::new
            )
        );
        namedWriteables.add(
            new NamedWriteableRegistry.Entry(
                TaskSettings.class,
                AlibabaCloudSearchSparseTaskSettings.NAME,
                AlibabaCloudSearchSparseTaskSettings::new
            )
        );
        namedWriteables.add(
            new NamedWriteableRegistry.Entry(
                ServiceSettings.class,
                AlibabaCloudSearchRerankServiceSettings.NAME,
                AlibabaCloudSearchRerankServiceSettings::new
            )
        );
        namedWriteables.add(
            new NamedWriteableRegistry.Entry(
                TaskSettings.class,
                AlibabaCloudSearchRerankTaskSettings.NAME,
                AlibabaCloudSearchRerankTaskSettings::new
            )
        );
        namedWriteables.add(
            new NamedWriteableRegistry.Entry(
                ServiceSettings.class,
                AlibabaCloudSearchCompletionServiceSettings.NAME,
                AlibabaCloudSearchCompletionServiceSettings::new
            )
        );
        namedWriteables.add(
            new NamedWriteableRegistry.Entry(
                TaskSettings.class,
                AlibabaCloudSearchCompletionTaskSettings.NAME,
                AlibabaCloudSearchCompletionTaskSettings::new
            )
        );

    }

    private static void addJinaAINamedWriteables(List<NamedWriteableRegistry.Entry> namedWriteables) {
        namedWriteables.add(
            new NamedWriteableRegistry.Entry(ServiceSettings.class, JinaAIServiceSettings.NAME, JinaAIServiceSettings::new)
        );
        namedWriteables.add(
            new NamedWriteableRegistry.Entry(
                ServiceSettings.class,
                JinaAIEmbeddingsServiceSettings.NAME,
                JinaAIEmbeddingsServiceSettings::new
            )
        );
        namedWriteables.add(
            new NamedWriteableRegistry.Entry(TaskSettings.class, JinaAIEmbeddingsTaskSettings.NAME, JinaAIEmbeddingsTaskSettings::new)
        );
        namedWriteables.add(
            new NamedWriteableRegistry.Entry(ServiceSettings.class, JinaAIRerankServiceSettings.NAME, JinaAIRerankServiceSettings::new)
        );
        namedWriteables.add(
            new NamedWriteableRegistry.Entry(TaskSettings.class, JinaAIRerankTaskSettings.NAME, JinaAIRerankTaskSettings::new)
        );
    }

    private static void addVoyageAINamedWriteables(List<NamedWriteableRegistry.Entry> namedWriteables) {
        namedWriteables.add(
            new NamedWriteableRegistry.Entry(ServiceSettings.class, VoyageAIServiceSettings.NAME, VoyageAIServiceSettings::new)
        );
        namedWriteables.add(
            new NamedWriteableRegistry.Entry(
                ServiceSettings.class,
                VoyageAIEmbeddingsServiceSettings.NAME,
                VoyageAIEmbeddingsServiceSettings::new
            )
        );
        namedWriteables.add(
            new NamedWriteableRegistry.Entry(TaskSettings.class, VoyageAIEmbeddingsTaskSettings.NAME, VoyageAIEmbeddingsTaskSettings::new)
        );
        namedWriteables.add(
            new NamedWriteableRegistry.Entry(ServiceSettings.class, VoyageAIRerankServiceSettings.NAME, VoyageAIRerankServiceSettings::new)
        );
        namedWriteables.add(
            new NamedWriteableRegistry.Entry(TaskSettings.class, VoyageAIRerankTaskSettings.NAME, VoyageAIRerankTaskSettings::new)
        );
    }

    private static void addEisNamedWriteables(List<NamedWriteableRegistry.Entry> namedWriteables) {
        namedWriteables.add(
            new NamedWriteableRegistry.Entry(
                ServiceSettings.class,
                ElasticInferenceServiceSparseEmbeddingsServiceSettings.NAME,
                ElasticInferenceServiceSparseEmbeddingsServiceSettings::new
            )
        );
        namedWriteables.add(
            new NamedWriteableRegistry.Entry(
                ServiceSettings.class,
                ElasticInferenceServiceCompletionServiceSettings.NAME,
                ElasticInferenceServiceCompletionServiceSettings::new
            )
        );
    }
}
