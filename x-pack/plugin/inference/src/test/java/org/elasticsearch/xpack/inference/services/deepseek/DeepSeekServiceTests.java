/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.deepseek;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.inference.InputType;
import org.elasticsearch.inference.Model;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.inference.UnifiedCompletionRequest;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.http.MockResponse;
import org.elasticsearch.test.http.MockWebServer;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.inference.results.ChatCompletionResults;
import org.elasticsearch.xpack.inference.external.http.HttpClientManager;
import org.elasticsearch.xpack.inference.external.http.sender.HttpRequestSenderTests;
import org.elasticsearch.xpack.inference.logging.ThrottlerManager;
import org.elasticsearch.xpack.inference.services.InferenceEventsAssertion;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.action.support.ActionTestUtils.assertNoFailureListener;
import static org.elasticsearch.action.support.ActionTestUtils.assertNoSuccessListener;
import static org.elasticsearch.common.Strings.format;
import static org.elasticsearch.xpack.inference.Utils.inferenceUtilityPool;
import static org.elasticsearch.xpack.inference.Utils.mockClusterServiceEmpty;
import static org.elasticsearch.xpack.inference.services.ServiceComponentsTests.createWithEmptySettings;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.isA;
import static org.mockito.Mockito.mock;

public class DeepSeekServiceTests extends ESTestCase {
    private static final TimeValue TIMEOUT = new TimeValue(30, TimeUnit.SECONDS);
    private final MockWebServer webServer = new MockWebServer();
    private ThreadPool threadPool;
    private HttpClientManager clientManager;

    @Before
    public void init() throws Exception {
        webServer.start();
        threadPool = createThreadPool(inferenceUtilityPool());
        clientManager = HttpClientManager.create(Settings.EMPTY, threadPool, mockClusterServiceEmpty(), mock(ThrottlerManager.class));
    }

    @After
    public void shutdown() throws IOException {
        clientManager.close();
        terminate(threadPool);
        webServer.close();
    }

    public void testParseRequestConfig() throws IOException, URISyntaxException {
        parseRequestConfig(format("""
            {
              "service_settings": {
                "api_key": "12345",
                "model_id": "some-cool-model",
                "url": "%s"
              }
            }
            """, webServer.getUri(null).toString()), assertNoFailureListener(model -> {
            if (model instanceof DeepSeekChatCompletionModel deepSeekModel) {
                assertThat(deepSeekModel.apiKey().getChars(), equalTo("12345".toCharArray()));
                assertThat(deepSeekModel.model(), equalTo("some-cool-model"));
                assertThat(deepSeekModel.uri(), equalTo(webServer.getUri(null)));
            } else {
                fail("Expected DeepSeekModel, found " + (model != null ? model.getClass().getSimpleName() : "null"));
            }
        }));
    }

    public void testParseRequestConfigWithoutApiKey() throws IOException {
        parseRequestConfig("""
            {
              "service_settings": {
                "model_id": "some-cool-model"
              }
            }
            """, assertNoSuccessListener(e -> {
            if (e instanceof ValidationException ve) {
                assertThat(
                    ve.getMessage(),
                    equalTo("Validation Failed: 1: [service_settings] does not contain the required setting [api_key];")
                );
            }
        }));
    }

    public void testParseRequestConfigWithoutModel() throws IOException {
        parseRequestConfig("""
            {
              "service_settings": {
                "api_key": "1234"
              }
            }
            """, assertNoSuccessListener(e -> {
            if (e instanceof ValidationException ve) {
                assertThat(
                    ve.getMessage(),
                    equalTo("Validation Failed: 1: [service_settings] does not contain the required setting [model_id];")
                );
            }
        }));
    }

    public void testParseRequestConfigWithExtraSettings() throws IOException {
        parseRequestConfig(
            """
                {
                  "service_settings": {
                    "api_key": "12345",
                    "model_id": "some-cool-model",
                    "so": "extra"
                  }
                }
                """,
            assertNoSuccessListener(
                e -> assertThat(
                    e.getMessage(),
                    equalTo("Model configuration contains settings [{so=extra}] unknown to the [deepseek] service")
                )
            )
        );
    }

    public void testParsePersistedConfig() throws IOException {
        var deepSeekModel = parsePersistedConfig("""
            {
              "service_settings": {
                "model_id": "some-cool-model"
              },
              "secret_settings": {
                "api_key": "12345"
              }
            }
            """);
        assertThat(deepSeekModel.apiKey().getChars(), equalTo("12345".toCharArray()));
        assertThat(deepSeekModel.model(), equalTo("some-cool-model"));
    }

    public void testParsePersistedConfigWithUrl() throws IOException {
        var deepSeekModel = parsePersistedConfig("""
            {
              "service_settings": {
                "model_id": "some-cool-model",
                "url": "http://localhost:989"
              },
              "secret_settings": {
                "api_key": "12345"
              }
            }
            """);
        assertThat(deepSeekModel.apiKey().getChars(), equalTo("12345".toCharArray()));
        assertThat(deepSeekModel.model(), equalTo("some-cool-model"));
        assertThat(deepSeekModel.uri(), equalTo(URI.create("http://localhost:989")));
    }

    public void testParsePersistedConfigWithoutApiKey() {
        assertThrows(
            "Validation Failed: 1: [secret_settings] does not contain the required setting [api_key];",
            ValidationException.class,
            () -> parsePersistedConfig("""
                {
                  "service_settings": {
                    "model_id": "some-cool-model"
                  },
                  "secret_settings": {
                  }
                }
                """)
        );
    }

    public void testParsePersistedConfigWithoutModel() {
        assertThrows(
            "Validation Failed: 1: [service_settings] does not contain the required setting [model];",
            ValidationException.class,
            () -> parsePersistedConfig("""
                {
                  "service_settings": {
                  },
                  "secret_settings": {
                    "api_key": "12345"
                  }
                }
                """)
        );
    }

    public void testParsePersistedConfigWithoutServiceSettings() {
        assertThrows(
            "Validation Failed: 1: [service_settings] does not contain the required setting [model];",
            ElasticsearchStatusException.class,
            () -> parsePersistedConfig("""
                {
                  "secret_settings": {
                    "api_key": "12345"
                  }
                }
                """)
        );
    }

    public void testDoUnifiedInfer() throws Exception {
        webServer.enqueue(new MockResponse().setResponseCode(200).setBody("""
            data: {"choices": [{"delta": {"content": "hello, world", "role": "assistant"}, "finish_reason": null, "index": 0, \
            "logprobs": null}], "created": 1718345013, "id": "12345", "model": "deepseek-chat", \
            "object": "chat.completion.chunk", "system_fingerprint": "fp_1234"}

            data: [DONE]

            """));
        var result = doUnifiedCompletionInfer();
        InferenceEventsAssertion.assertThat(result).hasFinishedStream().hasNoErrors().hasEvent("""
            {"id":"12345","choices":[{"delta":{"content":"hello, world","role":"assistant"},"index":0}],""" + """
            "model":"deepseek-chat","object":"chat.completion.chunk"}""");
    }

    public void testDoInfer() throws Exception {
        webServer.enqueue(new MockResponse().setResponseCode(200).setBody("""
            {"choices": [{"message": {"content": "hello, world", "role": "assistant"}, "finish_reason": "stop", "index": 0, \
            "logprobs": null}], "created": 1718345013, "id": "12345", "model": "deepseek-chat", \
            "object": "chat.completion", "system_fingerprint": "fp_1234"}"""));
        var result = doInfer(false);
        assertThat(result, isA(ChatCompletionResults.class));
        var completionResults = (ChatCompletionResults) result;
        assertThat(
            completionResults.results().stream().map(ChatCompletionResults.Result::predictedValue).toList(),
            equalTo(List.of("hello, world"))
        );
    }

    public void testDoInferStream() throws Exception {
        webServer.enqueue(new MockResponse().setResponseCode(200).setBody("""
            data: {"choices": [{"delta": {"content": "hello, world", "role": "assistant"}, "finish_reason": null, "index": 0, \
            "logprobs": null}], "created": 1718345013, "id": "12345", "model": "deepseek-chat", \
            "object": "chat.completion.chunk", "system_fingerprint": "fp_1234"}

            data: [DONE]

            """));
        var result = doInfer(true);
        InferenceEventsAssertion.assertThat(result).hasFinishedStream().hasNoErrors().hasEvent("""
            {"completion":[{"delta":"hello, world"}]}""");
    }

    public void testDoChunkedInferAlwaysFails() throws IOException {
        try (var service = createService()) {
            service.doChunkedInfer(mock(), mock(), Map.of(), InputType.UNSPECIFIED, TIMEOUT, assertNoSuccessListener(e -> {
                assertThat(e, isA(UnsupportedOperationException.class));
                assertThat(e.getMessage(), equalTo("The deepseek service only supports unified completion"));
            }));
        }
    }

    private DeepSeekService createService() {
        return new DeepSeekService(
            HttpRequestSenderTests.createSenderFactory(threadPool, clientManager),
            createWithEmptySettings(threadPool)
        );
    }

    private void parseRequestConfig(String json, ActionListener<Model> listener) throws IOException {
        try (var service = createService()) {
            service.parseRequestConfig("inference-id", TaskType.CHAT_COMPLETION, map(json), listener);
        }
    }

    private Map<String, Object> map(String json) throws IOException {
        try (
            var parser = XContentType.JSON.xContent().createParser(XContentParserConfiguration.EMPTY, json.getBytes(StandardCharsets.UTF_8))
        ) {
            return parser.map();
        }
    }

    private DeepSeekChatCompletionModel parsePersistedConfig(String json) throws IOException {
        try (var service = createService()) {
            var model = service.parsePersistedConfig("inference-id", TaskType.CHAT_COMPLETION, map(json));
            assertThat(model, isA(DeepSeekChatCompletionModel.class));
            return (DeepSeekChatCompletionModel) model;
        }
    }

    private InferenceServiceResults doUnifiedCompletionInfer() throws Exception {
        try (var service = createService()) {
            var model = createModel(service, TaskType.CHAT_COMPLETION);
            PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
            service.unifiedCompletionInfer(
                model,
                UnifiedCompletionRequest.of(
                    List.of(new UnifiedCompletionRequest.Message(new UnifiedCompletionRequest.ContentString("hello"), "user", null, null))
                ),
                TIMEOUT,
                listener
            );
            return listener.get(30, TimeUnit.SECONDS);
        }
    }

    private InferenceServiceResults doInfer(boolean stream) throws Exception {
        try (var service = createService()) {
            var model = createModel(service, TaskType.COMPLETION);
            PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
            service.infer(model, null, List.of("hello"), stream, Map.of(), InputType.UNSPECIFIED, TIMEOUT, listener);
            return listener.get(30, TimeUnit.SECONDS);
        }
    }

    private DeepSeekChatCompletionModel createModel(DeepSeekService service, TaskType taskType) throws URISyntaxException, IOException {
        var model = service.parsePersistedConfig("inference-id", taskType, map(Strings.format("""
            {
              "service_settings": {
                "model_id": "some-cool-model",
                "url": "%s"
              },
              "secret_settings": {
                "api_key": "12345"
              }
            }
            """, webServer.getUri(null).toString())));
        assertThat(model, isA(DeepSeekChatCompletionModel.class));
        return (DeepSeekChatCompletionModel) model;
    }
}
