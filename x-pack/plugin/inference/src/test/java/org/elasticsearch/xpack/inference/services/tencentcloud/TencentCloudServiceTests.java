/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.tencentcloud;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.inference.InferenceService;
import org.elasticsearch.inference.InferenceServiceConfiguration;
import org.elasticsearch.inference.Model;
import org.elasticsearch.inference.RerankingInferenceService;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.inference.external.http.sender.HttpRequestSenderTests;
import org.elasticsearch.xpack.inference.services.InferenceServiceTestCase;
import org.elasticsearch.xpack.inference.services.settings.DefaultSecretSettings;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;
import org.elasticsearch.xpack.inference.services.tencentcloud.embeddings.TencentCloudEmbeddingsModel;
import org.elasticsearch.xpack.inference.services.tencentcloud.embeddings.TencentCloudEmbeddingsServiceSettings;
import org.elasticsearch.xpack.inference.services.tencentcloud.embeddings.TencentCloudEmbeddingsTaskSettings;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.EnumSet;
import java.util.Map;

import static org.elasticsearch.action.support.ActionTestUtils.assertNoSuccessListener;
import static org.elasticsearch.common.xcontent.XContentHelper.toXContent;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertToXContentEquivalent;
import static org.elasticsearch.xpack.inference.Utils.mockClusterServiceEmpty;
import static org.elasticsearch.xpack.inference.services.ServiceComponentsTests.createWithEmptySettings;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;

public class TencentCloudServiceTests extends InferenceServiceTestCase {

    private static final String MODEL_ID = "bge-m3";
    private static final String API_KEY = "sk-12345";
    private static final String INFERENCE_ID = "inference-id";
    private static final String DEFAULT_REGION = "bj";

    public void testParseRequestConfig_MissingApiKey_Fails() throws IOException {
        parseRequestConfig(TaskType.TEXT_EMBEDDING, """
            {
              "service_settings": {
                "model_id": "bge-m3"
              }
            }
            """, assertNoSuccessListener(e -> {
            assertThat(e, instanceOf(ValidationException.class));
            assertThat(e.getMessage(), containsString("api_key"));
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
            assertThat(e, instanceOf(IllegalArgumentException.class));
            assertThat(e.getMessage(), containsString("model_id"));
        }));
    }

    public void testRerankerWindowSize_ReturnsConservativeValue() throws IOException {
        try (var service = createService()) {
            assertThat(service.rerankerWindowSize("bge-reranker-v2-m3"), is(350));
        }
    }

    @SuppressWarnings("checkstyle:LineLength")
    public void testGetConfiguration() throws Exception {
        try (var service = createService()) {
            String content = XContentHelper.stripWhitespace(
                """
                    {
                        "service": "tencentcloud",
                        "name": "TencentCloud AI Gateway",
                        "task_types": ["text_embedding", "rerank", "completion", "chat_completion"],
                        "configurations": {
                            "model_id": {
                                "description": "The name of the model to use for the inference task, e.g. bge-m3 (embeddings), deepseek-v3 (chat/completions), bge-reranker-v2-m3 (rerank). The gateway supports additional models; check the TencentCloud AI Gateway documentation for the full list.",
                                "label": "Model ID",
                                "required": true,
                                "sensitive": false,
                                "updatable": false,
                                "type": "str",
                                "supported_task_types": ["text_embedding", "rerank", "completion", "chat_completion"]
                            },
                            "region": {
                                "default_value": "bj",
                                "description": "The TencentCloud AI Gateway region, e.g. bj, sh, gz. The endpoint URL is constructed as https://{region}.aisearch.tencentelasticsearch.com/v1/<task-path>.",
                                "label": "Region",
                                "required": false,
                                "sensitive": false,
                                "updatable": false,
                                "type": "str",
                                "supported_task_types": ["text_embedding", "rerank", "completion", "chat_completion"]
                            },
                            "api_key": {
                                "description": "The TencentCloud AI Gateway API key. Contact the administrator to obtain a token in the format sk-<your-api-key>.",
                                "label": "API Key",
                                "required": true,
                                "sensitive": true,
                                "updatable": true,
                                "type": "str",
                                "supported_task_types": ["text_embedding", "rerank", "completion", "chat_completion"]
                            },
                            "rate_limit.requests_per_minute": {
                                "description": "Minimize the number of rate limit errors.",
                                "label": "Rate Limit",
                                "required": false,
                                "sensitive": false,
                                "updatable": false,
                                "type": "int",
                                "supported_task_types": ["text_embedding", "rerank", "completion", "chat_completion"]
                            }
                        }
                    }
                    """
            );
            var configuration = InferenceServiceConfiguration.fromXContentBytes(new BytesArray(content), XContentType.JSON);
            boolean humanReadable = true;
            BytesReference originalBytes = toShuffledXContent(configuration, XContentType.JSON, ToXContent.EMPTY_PARAMS, humanReadable);
            var serviceConfiguration = service.getConfiguration();
            assertToXContentEquivalent(
                originalBytes,
                toXContent(serviceConfiguration, XContentType.JSON, humanReadable),
                XContentType.JSON
            );
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
        var serviceSettings = new TencentCloudEmbeddingsServiceSettings(
            MODEL_ID,
            DEFAULT_REGION,
            new RateLimitSettings(20),
            similarity,
            null,
            null
        );
        return new TencentCloudEmbeddingsModel(
            INFERENCE_ID,
            serviceSettings,
            TencentCloudEmbeddingsTaskSettings.EMPTY_SETTINGS,
            null,
            new DefaultSecretSettings(new SecureString(API_KEY))
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

    private void parseRequestConfig(TaskType taskType, String json, ActionListener<Model> listener) throws IOException {
        try (var service = createService()) {
            service.parseRequestConfig(INFERENCE_ID, taskType, map(json), listener);
        }
    }

    private Map<String, Object> map(String json) throws IOException {
        try (
            var parser = XContentType.JSON.xContent().createParser(XContentParserConfiguration.EMPTY, json.getBytes(StandardCharsets.UTF_8))
        ) {
            return parser.map();
        }
    }

    @Override
    public SimilarityMeasure getDefaultSimilarity() {
        return SimilarityMeasure.COSINE;
    }
}
