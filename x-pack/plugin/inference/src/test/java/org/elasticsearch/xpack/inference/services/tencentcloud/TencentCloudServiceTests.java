/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.tencentcloud;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.TestPlainActionFuture;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.inference.ChunkInferenceInput;
import org.elasticsearch.inference.ChunkedInference;
import org.elasticsearch.inference.InferenceService;
import org.elasticsearch.inference.InputType;
import org.elasticsearch.inference.Model;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ModelSecrets;
import org.elasticsearch.inference.RerankingInferenceService;
import org.elasticsearch.inference.ServiceSettings;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.inference.UnparsedModel;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.inference.external.http.sender.HttpRequestSenderTests;
import org.elasticsearch.xpack.inference.services.InferenceServiceTestCase;
import org.elasticsearch.xpack.inference.services.settings.DefaultSecretSettings;
import org.elasticsearch.xpack.inference.services.tencentcloud.completion.TencentCloudChatCompletionModel;
import org.elasticsearch.xpack.inference.services.tencentcloud.completion.TencentCloudChatCompletionServiceSettings;
import org.elasticsearch.xpack.inference.services.tencentcloud.embeddings.TencentCloudEmbeddingsModel;
import org.elasticsearch.xpack.inference.services.tencentcloud.embeddings.TencentCloudEmbeddingsServiceSettings;
import org.elasticsearch.xpack.inference.services.tencentcloud.embeddings.TencentCloudEmbeddingsTaskSettings;
import org.elasticsearch.xpack.inference.services.tencentcloud.rerank.TencentCloudRerankModel;
import org.elasticsearch.xpack.inference.services.tencentcloud.rerank.TencentCloudRerankServiceSettings;
import org.elasticsearch.xpack.inference.services.tencentcloud.rerank.TencentCloudRerankTaskSettings;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.action.support.ActionTestUtils.assertNoFailureListener;
import static org.elasticsearch.action.support.ActionTestUtils.assertNoSuccessListener;
import static org.elasticsearch.common.Strings.format;
import static org.elasticsearch.xpack.inference.Utils.mockClusterServiceEmpty;
import static org.elasticsearch.xpack.inference.services.ServiceComponentsTests.createWithEmptySettings;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.isA;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TencentCloudServiceTests extends InferenceServiceTestCase {

    private static final TimeValue TIMEOUT = new TimeValue(30, TimeUnit.SECONDS);

    public void testName_IsTencentCloud() throws IOException {
        try (var service = createService()) {
            assertThat(service.name(), is("tencentcloud"));
        }
    }

    public void testSupportedTaskTypes_ContainsExpectedTasks() throws IOException {
        try (var service = createService()) {
            assertThat(service.supportedTaskTypes().contains(TaskType.TEXT_EMBEDDING), is(true));
            assertThat(service.supportedTaskTypes().contains(TaskType.COMPLETION), is(true));
            assertThat(service.supportedTaskTypes().contains(TaskType.CHAT_COMPLETION), is(true));
            assertThat(service.supportedTaskTypes().contains(TaskType.RERANK), is(true));
        }
    }

    public void testParseRequestConfig_TextEmbedding() throws IOException {
        var region = "bj";
        parseRequestConfig(TaskType.TEXT_EMBEDDING, format("""
            {
              "service_settings": {
                "api_key": "sk-12345",
                "model_id": "bge-m3",
                "region": "%s"
              }
            }
            """, region), assertNoFailureListener(model -> {
            assertThat(model, isA(TencentCloudEmbeddingsModel.class));
            var m = (TencentCloudEmbeddingsModel) model;
            assertThat(m.getServiceSettings().modelId(), equalTo("bge-m3"));
            assertThat(m.uri().toString(), equalTo("https://bj.aisearch.tencentelasticsearch.com/v1/embeddings"));
            assertThat(m.apiKey().toString(), equalTo("sk-12345"));
        }));
    }

    public void testParseRequestConfig_ChatCompletion_UsesRegionUri() throws IOException {
        parseRequestConfig(TaskType.CHAT_COMPLETION, """
            {
              "service_settings": {
                "api_key": "sk-12345",
                "model_id": "deepseek-v3",
                "region": "sh"
              }
            }
            """, assertNoFailureListener(model -> {
            assertThat(model, isA(TencentCloudChatCompletionModel.class));
            var m = (TencentCloudChatCompletionModel) model;
            assertThat(m.model(), equalTo("deepseek-v3"));
            assertThat(m.uri().toString(), equalTo("https://sh.aisearch.tencentelasticsearch.com/v1/chat/completions"));
        }));
    }

    public void testParseRequestConfig_Rerank_WithTaskSettings() throws IOException {
        parseRequestConfig(TaskType.RERANK, """
            {
              "service_settings": {
                "api_key": "sk-12345",
                "model_id": "bge-reranker-v2-m3"
              },
              "task_settings": {
                "top_n": 5,
                "return_documents": true
              }
            }
            """, assertNoFailureListener(model -> {
            assertThat(model, isA(TencentCloudRerankModel.class));
            var m = (TencentCloudRerankModel) model;
            assertThat(m.getServiceSettings().modelId(), equalTo("bge-reranker-v2-m3"));
            assertThat(m.getTaskSettings().getTopN(), equalTo(5));
            assertThat(m.getTaskSettings().getReturnDocuments(), equalTo(true));
        }));
    }

    public void testParseRequestConfig_MissingApiKey_Fails() throws IOException {
        parseRequestConfig(TaskType.TEXT_EMBEDDING, """
            {
              "service_settings": {
                "model_id": "bge-m3"
              }
            }
            """, assertNoSuccessListener(e -> {
            if (e instanceof ValidationException ve) {
                assertThat(ve.getMessage().contains("api_key"), is(true));
            }
        }));
    }

    public void testParseRequestConfig_MissingModelId_Fails() throws IOException {
        parseRequestConfig(TaskType.TEXT_EMBEDDING, """
            {
              "service_settings": {
                "api_key": "sk-12345"
              }
            }
            """, assertNoSuccessListener(e -> {
            if (e instanceof ValidationException ve) {
                assertThat(ve.getMessage().contains("model_id"), is(true));
            }
        }));
    }

    public void testParsePersistedConfig_ChatCompletion() throws IOException {
        var asMap = map("""
            {
              "service_settings": {
                "model_id": "deepseek-v3",
                "region": "sh"
              }
            }
            """);
        Map<String, Object> serviceSettings = new HashMap<>();
        serviceSettings.put(ModelConfigurations.SERVICE_SETTINGS, asMap.get(ModelConfigurations.SERVICE_SETTINGS));
        try (var service = createService()) {
            var model = service.parsePersistedConfig(
                new UnparsedModel("inference-id", TaskType.CHAT_COMPLETION, TencentCloudService.NAME, serviceSettings, null)
            );
            assertThat(model, isA(TencentCloudChatCompletionModel.class));
            var m = (TencentCloudChatCompletionModel) model;
            assertThat(m.model(), equalTo("deepseek-v3"));
            assertThat(m.uri().toString(), equalTo("https://sh.aisearch.tencentelasticsearch.com/v1/chat/completions"));
        }
    }

    public void testChunkedInfer_UnsupportedForNonEmbeddingModel() throws IOException {
        var modelConfigurations = new ModelConfigurations(
            "inference-id",
            TaskType.SPARSE_EMBEDDING,
            TencentCloudService.NAME,
            mock(ServiceSettings.class)
        );
        try (var service = createInferenceService()) {
            var e = expectThrows(
                ElasticsearchStatusException.class,
                () -> service.buildModelFromConfigAndSecrets(modelConfigurations, mock(ModelSecrets.class))
            );
            assertThat(
                e.getMessage(),
                is(format("The [%s] service does not support task type [%s]", "tencentcloud", TaskType.SPARSE_EMBEDDING))
            );
        }
    }

    public void testRerankerWindowSize_ReturnsConservativeValue() throws IOException {
        try (var service = createService()) {
            assertThat(service.rerankerWindowSize("bge-reranker-v2-m3"), is(350));
        }
    }

    @Override
    public EnumSet<TaskType> expectedStreamingTasks() {
        return EnumSet.of(TaskType.COMPLETION, TaskType.CHAT_COMPLETION);
    }

    @Override
    protected void assertRerankerWindowSize(RerankingInferenceService rerankingInferenceService) {
        assertThat(rerankingInferenceService.rerankerWindowSize("bge-reranker-v2-m3"), is(350));
    }

    @Override
    public Model createEmbeddingModel(SimilarityMeasure similarity) {
        var commonSettings = new TencentCloudCommonServiceSettings(
            "bge-m3",
            "bj",
            new org.elasticsearch.xpack.inference.services.settings.RateLimitSettings(20)
        );
        var serviceSettings = new TencentCloudEmbeddingsServiceSettings(commonSettings, similarity, null, null);
        return new TencentCloudEmbeddingsModel(
            "inference-id",
            serviceSettings,
            TencentCloudEmbeddingsTaskSettings.EMPTY_SETTINGS,
            null,
            new DefaultSecretSettings(new SecureString("sk-12345"))
        );
    }

    private TencentCloudService createService() {
        return new TencentCloudService(
            HttpRequestSenderTests.createSenderFactory(threadPool, clientManager),
            createWithEmptySettings(threadPool),
            mockClusterServiceEmpty()
        );
    }

    @Override
    public InferenceService createInferenceService() {
        return createService();
    }

    private TencentCloudChatCompletionModel createChatCompletionModel(TaskType taskType) throws URISyntaxException {
        var commonSettings = new TencentCloudCommonServiceSettings(
            "deepseek-v3",
            "bj",
            new org.elasticsearch.xpack.inference.services.settings.RateLimitSettings(5)
        );
        return new TencentCloudChatCompletionModel(
            "inference-id",
            taskType,
            new TencentCloudChatCompletionServiceSettings(commonSettings),
            new DefaultSecretSettings(new SecureString("sk-12345"))
        );
    }

    private TencentCloudRerankModel createRerankModel(String modelId) throws URISyntaxException {
        var commonSettings = new TencentCloudCommonServiceSettings(
            modelId,
            "bj",
            new org.elasticsearch.xpack.inference.services.settings.RateLimitSettings(20)
        );
        return new TencentCloudRerankModel(
            "inference-id",
            new TencentCloudRerankServiceSettings(commonSettings),
            TencentCloudRerankTaskSettings.EMPTY_SETTINGS,
            new DefaultSecretSettings(new SecureString("sk-12345"))
        );
    }

    private void parseRequestConfig(TaskType taskType, String json, ActionListener<Model> listener) throws IOException {
        try (var service = createService()) {
            service.parseRequestConfig("inference-id", taskType, map(json), listener);
        }
    }

    private Map<String, Object> map(String json) throws IOException {
        try (
            var parser = XContentType.JSON.xContent().createParser(XContentParserConfiguration.EMPTY, json.getBytes(StandardCharsets.UTF_8))
        ) {
            return parser.map();
        }
    }
}
