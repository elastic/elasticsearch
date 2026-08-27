/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.fireworksai;

import org.apache.http.HttpHeaders;
import org.elasticsearch.action.support.TestPlainActionFuture;
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
import org.elasticsearch.inference.UnifiedCompletionRequest;
import org.elasticsearch.inference.completion.ContentString;
import org.elasticsearch.inference.completion.Message;
import org.elasticsearch.test.http.MockResponse;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.inference.external.http.sender.HttpRequestSenderTests;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.InferenceEventsAssertion;
import org.elasticsearch.xpack.inference.services.InferenceServiceTestCase;
import org.elasticsearch.xpack.inference.services.ServiceFields;
import org.elasticsearch.xpack.inference.services.fireworksai.embeddings.FireworksAiEmbeddingsModel;
import org.hamcrest.Matchers;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.common.xcontent.XContentHelper.stripWhitespace;
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
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

public class FireworksAiServiceTests extends InferenceServiceTestCase {

    public static final String API_KEY = "secret";
    public static final String MODEL_ID = "test-model";

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

            var model = createInternalEmbeddingModel(getUrl(webServer), API_KEY, MODEL_ID, null, null);
            TestPlainActionFuture<InferenceServiceResults> listener = new TestPlainActionFuture<>();
            var inputs = List.of("abc");
            service.infer(model, inputs, false, new HashMap<>(), InputType.INTERNAL_INGEST, null, listener);

            var result = listener.actionGet(TEST_REQUEST_TIMEOUT);

            assertThat(result.asMap(), Matchers.is(buildExpectationFloat(List.of(new float[] { 0.0123F, -0.0123F }))));
            assertThat(webServer.requests(), hasSize(1));
            assertNull(webServer.requests().get(0).getUri().getQuery());
            assertThat(webServer.requests().get(0).getHeader(HttpHeaders.CONTENT_TYPE), equalTo(XContentType.JSON.mediaType()));
            assertThat(webServer.requests().get(0).getHeader(HttpHeaders.AUTHORIZATION), equalTo(Strings.format("Bearer %s", API_KEY)));

            var requestMap = entityAsMap(webServer.requests().get(0).getBody());
            assertThat(requestMap.size(), Matchers.is(2));
            assertThat(requestMap.get("input"), Matchers.is(inputs));
            assertThat(requestMap.get("model"), Matchers.is(MODEL_ID));
        }
    }

    public void testUnifiedCompletionInfer_SendsChatCompletionRequest() throws Exception {
        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);

        try (var service = new FireworksAiService(senderFactory, createWithEmptySettings(threadPool), mockClusterServiceEmpty())) {

            String responseJson = Strings.format("""
                data: %s

                """, XContentHelper.stripWhitespace("""
                {
                    "id": "12345",
                    "object": "chat.completion.chunk",
                    "created": 123456789,
                    "model": "gpt-4o-mini",
                    "system_fingerprint": "123456789",
                    "choices": [{
                            "index": 0,
                            "delta": {
                                "content": "hello, world"
                            },
                            "logprobs": null,
                            "finish_reason": "stop"
                        }
                    ],
                    "usage": {
                        "prompt_tokens": 16,
                        "completion_tokens": 28,
                        "total_tokens": 44,
                        "prompt_tokens_details": {
                            "cached_tokens": 0,
                            "audio_tokens": 0
                        },
                        "completion_tokens_details": {
                            "reasoning_tokens": 0,
                            "audio_tokens": 0,
                            "accepted_prediction_tokens": 0,
                            "rejected_prediction_tokens": 0
                        }
                    }
                }
                """));
            webServer.enqueue(new MockResponse().setResponseCode(200).setBody(responseJson));

            var model = createInternalChatCompletionModel(getUrl(webServer), API_KEY, MODEL_ID);
            TestPlainActionFuture<InferenceServiceResults> listener = new TestPlainActionFuture<>();
            var request = UnifiedCompletionRequest.of(List.of(new Message(new ContentString("Hello"), "user", null, null)));

            service.unifiedCompletionInfer(model, request, null, listener);

            var inferenceServiceResults = listener.actionGet(TEST_REQUEST_TIMEOUT);

            InferenceEventsAssertion.assertThat(inferenceServiceResults)
                .hasFinishedStream()
                .hasNoErrors()
                .hasEvent(XContentHelper.stripWhitespace("""
                        {
                            "id":"12345",
                            "choices":[
                                {
                                    "delta":{"content":"hello, world"},
                                    "finish_reason":"stop",
                                    "index":0
                                }
                            ],
                            "model":"gpt-4o-mini",
                            "object":"chat.completion.chunk",
                            "usage":{
                                "completion_tokens":28,
                                "prompt_tokens":16,
                                "total_tokens":44,
                                "prompt_tokens_details":{"cached_tokens":0},
                                "completion_tokens_details":{"reasoning_tokens":0}
                            }
                        }
                    """));

            // Verify the request was sent
            assertThat(webServer.requests(), hasSize(1));
            assertThat(webServer.requests().getFirst().getHeader(HttpHeaders.CONTENT_TYPE), equalTo(XContentType.JSON.mediaType()));
            assertThat(webServer.requests().getFirst().getHeader(HttpHeaders.AUTHORIZATION), equalTo(Strings.format("Bearer %s", API_KEY)));

            var requestBody = webServer.requests().getFirst().getBody();

            String expectedJson = stripWhitespace(Strings.format("""
                {
                    "messages":[
                        {
                            "content":"Hello",
                            "role":"user"
                        }
                    ],
                    "model":"%s",
                    "n":1,
                    "stream":true,
                    "stream_options":{"include_usage":true}
                }
                """, MODEL_ID));
            assertThat(requestBody, is(expectedJson));
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
        var serviceSettings = new HashMap<String, Object>(Map.of(ServiceFields.MODEL_ID, randomAlphaOfLength(8)));
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
