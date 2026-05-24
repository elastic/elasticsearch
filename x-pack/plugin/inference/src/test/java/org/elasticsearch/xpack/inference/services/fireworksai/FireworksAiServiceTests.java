/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.fireworksai;

import org.apache.http.HttpHeaders;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.inference.InferenceService;
import org.elasticsearch.inference.InferenceServiceConfiguration;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.inference.InputType;
import org.elasticsearch.inference.Model;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.test.http.MockResponse;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.inference.external.http.sender.HttpRequestSenderTests;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.InferenceServiceTestCase;
import org.elasticsearch.xpack.inference.services.fireworksai.embeddings.FireworksAiEmbeddingsModel;
import org.hamcrest.Matchers;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.common.xcontent.XContentHelper.toXContent;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertToXContentEquivalent;
import static org.elasticsearch.xpack.core.inference.results.DenseEmbeddingFloatResultsTests.buildExpectationFloat;
import static org.elasticsearch.xpack.inference.Utils.mockClusterServiceEmpty;
import static org.elasticsearch.xpack.inference.external.http.Utils.entityAsMap;
import static org.elasticsearch.xpack.inference.external.http.Utils.getUrl;
import static org.elasticsearch.xpack.inference.services.ServiceComponentsTests.createWithEmptySettings;
import static org.elasticsearch.xpack.inference.services.ServiceFields.MODEL_ID;
import static org.elasticsearch.xpack.inference.services.ServiceFields.SIMILARITY;
import static org.elasticsearch.xpack.inference.services.fireworksai.FireworksAiServiceParameterizedTestConfiguration.createInternalChatCompletionModel;
import static org.elasticsearch.xpack.inference.services.fireworksai.FireworksAiServiceParameterizedTestConfiguration.createInternalEmbeddingModel;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

public class FireworksAiServiceTests extends InferenceServiceTestCase {

    public void testInfer_SendsEmbeddingsRequest() throws IOException {
        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);

        try (var service = new FireworksAiService(senderFactory, createWithEmptySettings(threadPool), mockClusterServiceEmpty())) {

            String responseJson = """
                {
                  "object": "list",
                  "data": [
                      {
                          "object": "embedding",
                          "index": 0,
                          "embedding": [
                              0.0123,
                              -0.0123
                          ]
                      }
                  ],
                  "model": "nomic-ai/nomic-embed-text-v1.5",
                  "usage": {
                      "prompt_tokens": 8,
                      "total_tokens": 8
                  }
                }
                """;
            webServer.enqueue(new MockResponse().setResponseCode(200).setBody(responseJson));

            var model = createInternalEmbeddingModel(getUrl(webServer), "secret", "model", null, null);
            PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
            service.infer(model, null, null, null, List.of("abc"), false, new HashMap<>(), InputType.INTERNAL_INGEST, null, listener);

            var result = listener.actionGet(TEST_REQUEST_TIMEOUT);

            assertThat(result.asMap(), Matchers.is(buildExpectationFloat(List.of(new float[] { 0.0123F, -0.0123F }))));
            assertThat(webServer.requests(), hasSize(1));
            assertNull(webServer.requests().get(0).getUri().getQuery());
            assertThat(webServer.requests().get(0).getHeader(HttpHeaders.CONTENT_TYPE), equalTo(XContentType.JSON.mediaType()));
            assertThat(webServer.requests().get(0).getHeader(HttpHeaders.AUTHORIZATION), equalTo("Bearer secret"));

            var requestMap = entityAsMap(webServer.requests().get(0).getBody());
            assertThat(requestMap.size(), Matchers.is(2));
            assertThat(requestMap.get("input"), Matchers.is(List.of("abc")));
            assertThat(requestMap.get("model"), Matchers.is("model"));
        }
    }

    public void testInfer_SendsChatCompletionRequest() throws IOException {
        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);

        try (var service = new FireworksAiService(senderFactory, createWithEmptySettings(threadPool), mockClusterServiceEmpty())) {

            String responseJson = """
                {
                  "id": "chatcmpl-123",
                  "object": "chat.completion",
                  "created": 1677652288,
                  "model": "accounts/fireworks/models/llama-v3p1-70b-instruct",
                  "choices": [{
                    "index": 0,
                    "message": {
                      "role": "assistant",
                      "content": "Hello! How can I help you?"
                    },
                    "finish_reason": "stop"
                  }],
                  "usage": {
                    "prompt_tokens": 9,
                    "completion_tokens": 12,
                    "total_tokens": 21
                  }
                }
                """;
            webServer.enqueue(new MockResponse().setResponseCode(200).setBody(responseJson));

            var model = createInternalChatCompletionModel(getUrl(webServer), "secret", "test-model");
            PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
            service.infer(model, null, null, null, List.of("Hello"), false, new HashMap<>(), InputType.UNSPECIFIED, null, listener);

            var result = listener.actionGet(TEST_REQUEST_TIMEOUT);

            assertThat(webServer.requests(), hasSize(1));
            assertThat(webServer.requests().get(0).getHeader(HttpHeaders.CONTENT_TYPE), equalTo(XContentType.JSON.mediaType()));
            assertThat(webServer.requests().get(0).getHeader(HttpHeaders.AUTHORIZATION), equalTo("Bearer secret"));

            var requestMap = entityAsMap(webServer.requests().get(0).getBody());
            assertThat(requestMap.get("model"), Matchers.is("test-model"));
        }
    }

    public void testGetConfiguration() throws Exception {
        try (var service = createInferenceService()) {

            var dimensionsDescription =
                "The number of dimensions the resulting output embeddings should have. Only supported by some models. "
                    + "For more information refer to https://docs.fireworks.ai/guides/querying-embeddings-models.";
            String content = XContentHelper.stripWhitespace(Strings.format("""
                {
                     "service": "fireworksai",
                     "name": "Fireworks AI",
                     "task_types": [
                         "text_embedding",
                         "completion",
                         "chat_completion"
                     ],
                     "configurations": {
                         "api_key": {
                             "description": "API Key for the provider you're connecting to.",
                             "label": "API Key",
                             "required": true,
                             "sensitive": true,
                             "updatable": true,
                             "type": "str",
                             "supported_task_types": [
                                 "text_embedding",
                                 "completion",
                                 "chat_completion"
                             ]
                         },
                         "rate_limit.requests_per_minute": {
                             "description": "Minimize the number of rate limit errors.",
                             "label": "Rate Limit",
                             "required": false,
                             "sensitive": false,
                             "updatable": false,
                             "type": "int",
                             "supported_task_types": [
                                 "text_embedding",
                                 "completion",
                                 "chat_completion"
                             ]
                         },
                         "model_id": {
                             "description": "The model ID to use for Fireworks AI requests.",
                             "label": "Model ID",
                             "required": true,
                             "sensitive": false,
                             "updatable": false,
                             "type": "str",
                             "supported_task_types": [
                                 "text_embedding",
                                 "completion",
                                 "chat_completion"
                             ]
                         },
                         "url": {
                             "description": "The URL of the Fireworks AI endpoint. Useful for on-demand deployments.",
                             "label": "URL",
                             "required": false,
                             "sensitive": false,
                             "updatable": false,
                             "type": "str",
                             "supported_task_types": [
                                 "text_embedding",
                                 "completion",
                                 "chat_completion"
                             ]
                         },
                         "dimensions": {
                             "description": "%s",
                             "label": "Dimensions",
                             "required": false,
                             "sensitive": false,
                             "updatable": false,
                             "type": "int",
                             "supported_task_types": [
                                 "text_embedding"
                             ]
                         }
                     }
                 }
                """, dimensionsDescription));
            InferenceServiceConfiguration configuration = InferenceServiceConfiguration.fromXContentBytes(
                new BytesArray(content),
                XContentType.JSON
            );
            boolean humanReadable = true;
            BytesReference originalBytes = toShuffledXContent(configuration, XContentType.JSON, ToXContent.EMPTY_PARAMS, humanReadable);
            InferenceServiceConfiguration serviceConfiguration = service.getConfiguration();
            assertToXContentEquivalent(
                originalBytes,
                toXContent(serviceConfiguration, XContentType.JSON, humanReadable),
                XContentType.JSON
            );
        }
    }

    @Override
    public InferenceService createInferenceService() {
        return new FireworksAiService(
            HttpRequestSenderTests.createSenderFactory(threadPool, clientManager),
            createWithEmptySettings(threadPool),
            mockClusterServiceEmpty()
        );
    }

    @Override
    public Model createEmbeddingModel(SimilarityMeasure similarity) {
        var serviceSettings = new HashMap<String, Object>(Map.of(MODEL_ID, randomAlphaOfLength(8)));
        if (similarity != null) {
            serviceSettings.put(SIMILARITY, similarity.toString());
        }
        return new FireworksAiEmbeddingsModel(
            randomAlphaOfLength(8),
            randomAlphaOfLength(8),
            serviceSettings,
            null,
            null,
            null,
            ConfigurationParseContext.REQUEST
        );
    }

    @Override
    public SimilarityMeasure getDefaultSimilarity() {
        return SimilarityMeasure.COSINE;
    }
}
