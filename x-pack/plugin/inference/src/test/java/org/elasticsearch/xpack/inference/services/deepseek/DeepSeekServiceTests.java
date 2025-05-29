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
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentHelper;
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
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.inference.action.InferenceAction;
import org.elasticsearch.xpack.core.inference.results.ChatCompletionResults;
import org.elasticsearch.xpack.core.inference.results.UnifiedChatCompletionException;
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
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.ExceptionsHelper.unwrapCause;
import static org.elasticsearch.action.support.ActionTestUtils.assertNoFailureListener;
import static org.elasticsearch.action.support.ActionTestUtils.assertNoSuccessListener;
import static org.elasticsearch.common.Strings.format;
import static org.elasticsearch.xcontent.ToXContent.EMPTY_PARAMS;
import static org.elasticsearch.xpack.inference.Utils.inferenceUtilityPool;
import static org.elasticsearch.xpack.inference.Utils.mockClusterServiceEmpty;
import static org.elasticsearch.xpack.inference.external.http.Utils.getUrl;
import static org.elasticsearch.xpack.inference.services.ServiceComponentsTests.createWithEmptySettings;
import static org.hamcrest.CoreMatchers.is;
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
                assertThat(deepSeekModel.apiKey().get().getChars(), equalTo("12345".toCharArray()));
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
              }
            }
            """);
        assertThat(deepSeekModel.apiKey(), equalTo(Optional.empty()));
        assertThat(deepSeekModel.model(), equalTo("some-cool-model"));
    }

    public void testParsePersistedConfigWithUrl() throws IOException {
        var deepSeekModel = parsePersistedConfig("""
            {
              "service_settings": {
                "model_id": "some-cool-model",
                "url": "http://localhost:989"
              }
            }
            """);
        assertThat(deepSeekModel.apiKey(), equalTo(Optional.empty()));
        assertThat(deepSeekModel.model(), equalTo("some-cool-model"));
        assertThat(deepSeekModel.uri(), equalTo(URI.create("http://localhost:989")));
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
        doUnifiedCompletionInfer().hasNoErrors().hasEvent("""
            {"id":"12345","choices":[{"delta":{"content":"hello, world","role":"assistant"},"index":0}],""" + """
            "model":"deepseek-chat","object":"chat.completion.chunk"}""");
    }

    public void testDoInfer() throws Exception {
        webServer.enqueue(new MockResponse().setResponseCode(200).setBody("""
            {"choices": [{"message": {"content": "hello, world", "role": "assistant"}, "finish_reason": "stop", "index": 0, \
            "logprobs": null}], "created": 1718345013, "id": "12345", "model": "deepseek-chat", \
            "object": "chat.completion", "system_fingerprint": "fp_1234"}"""));
        try (var service = createService()) {
            var model = createModel(service, TaskType.COMPLETION);
            PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
            service.infer(model, null, null, null, List.of("hello"), false, Map.of(), InputType.UNSPECIFIED, TIMEOUT, listener);
            var result = listener.actionGet(TIMEOUT);
            assertThat(result, isA(ChatCompletionResults.class));
            var completionResults = (ChatCompletionResults) result;
            assertThat(
                completionResults.results().stream().map(ChatCompletionResults.Result::predictedValue).toList(),
                equalTo(List.of("hello, world"))
            );
        }
    }

    public void testDoInferStream() throws Exception {
        webServer.enqueue(new MockResponse().setResponseCode(200).setBody("""
            data: {"choices": [{"delta": {"content": "hello, world", "role": "assistant"}, "finish_reason": null, "index": 0, \
            "logprobs": null}], "created": 1718345013, "id": "12345", "model": "deepseek-chat", \
            "object": "chat.completion.chunk", "system_fingerprint": "fp_1234"}

            data: [DONE]

            """));
        try (var service = createService()) {
            var model = createModel(service, TaskType.COMPLETION);
            PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
            service.infer(model, null, null, null, List.of("hello"), true, Map.of(), InputType.UNSPECIFIED, TIMEOUT, listener);
            InferenceEventsAssertion.assertThat(listener.actionGet(TIMEOUT)).hasFinishedStream().hasNoErrors().hasEvent("""
                {"completion":[{"delta":"hello, world"}]}""");
        }
    }

    public void testUnifiedCompletionError() {
        String responseJson = """
            {
                "error": {
                    "message": "The model `deepseek-not-chat` does not exist or you do not have access to it.",
                    "type": "invalid_request_error",
                    "param": null,
                    "code": "model_not_found"
                }
            }""";
        webServer.enqueue(new MockResponse().setResponseCode(404).setBody(responseJson));
        var e = assertThrows(UnifiedChatCompletionException.class, this::doUnifiedCompletionInfer);
        assertThat(
            e.getMessage(),
            equalTo(
                "Resource not found at ["
                    + getUrl(webServer)
                    + "] for request from inference entity id [inference-id]"
                    + " status [404]. Error message: [The model `deepseek-not-chat` does not exist or you do not have access to it.]"
            )
        );
    }

    private void testStreamError(String expectedResponse) throws Exception {
        try (var service = createService()) {
            var model = createModel(service, TaskType.CHAT_COMPLETION);
            PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
            service.unifiedCompletionInfer(
                model,
                UnifiedCompletionRequest.of(
                    List.of(new UnifiedCompletionRequest.Message(new UnifiedCompletionRequest.ContentString("hello"), "user", null, null))
                ),
                InferenceAction.Request.DEFAULT_TIMEOUT,
                listener
            );

            var result = listener.actionGet(TIMEOUT);

            InferenceEventsAssertion.assertThat(result).hasFinishedStream().hasNoEvents().hasErrorMatching(e -> {
                e = unwrapCause(e);
                assertThat(e, isA(UnifiedChatCompletionException.class));
                try (var builder = XContentFactory.jsonBuilder()) {
                    ((UnifiedChatCompletionException) e).toXContentChunked(EMPTY_PARAMS).forEachRemaining(xContent -> {
                        try {
                            xContent.toXContent(builder, EMPTY_PARAMS);
                        } catch (IOException ex) {
                            throw new RuntimeException(ex);
                        }
                    });
                    var json = XContentHelper.convertToJson(BytesReference.bytes(builder), false, builder.contentType());

                    assertThat(json, is(expectedResponse));
                }
            });
        }
    }

    public void testMidStreamUnifiedCompletionError() throws Exception {
        String responseJson = """
            event: error
            data: { "error": { "message": "Timed out waiting for more data", "type": "timeout" } }

            """;
        webServer.enqueue(new MockResponse().setResponseCode(200).setBody(responseJson));
        testStreamError("""
            {\
            "error":{\
            "message":"Received an error response for request from inference entity id [inference-id]. Error message: \
            [Timed out waiting for more data]",\
            "type":"timeout"\
            }}""");
    }

    public void testUnifiedCompletionMalformedError() throws Exception {
        String responseJson = """
            data: { invalid json }

            """;
        webServer.enqueue(new MockResponse().setResponseCode(200).setBody(responseJson));
        testStreamError("""
            {\
            "error":{\
            "code":"bad_request",\
            "message":"[1:3] Unexpected character ('i' (code 105)): was expecting double-quote to start field name\\n\
             at [Source: (String)\\"{ invalid json }\\"; line: 1, column: 3]",\
            "type":"x_content_parse_exception"\
            }}""");
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

    private InferenceEventsAssertion doUnifiedCompletionInfer() throws Exception {
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
            return InferenceEventsAssertion.assertThat(listener.actionGet(TIMEOUT)).hasFinishedStream();
        }
    }

    private DeepSeekChatCompletionModel createModel(DeepSeekService service, TaskType taskType) throws URISyntaxException, IOException {
        var model = service.parsePersistedConfigWithSecrets("inference-id", taskType, map(Strings.format("""
            {
              "service_settings": {
                "model_id": "some-cool-model",
                "url": "%s"
              }
            }
            """, webServer.getUri(null).toString())), map("""
            {
              "secret_settings": {
                "api_key": "12345"
              }
            }
            """));
        assertThat(model, isA(DeepSeekChatCompletionModel.class));
        return (DeepSeekChatCompletionModel) model;
    }
}
