/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.ai21;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionTestUtils;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.inference.EmptyTaskSettings;
import org.elasticsearch.inference.InferenceServiceConfiguration;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.inference.InputType;
import org.elasticsearch.inference.Model;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.inference.UnifiedCompletionRequest;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.http.MockResponse;
import org.elasticsearch.test.http.MockWebServer;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.inference.action.InferenceAction;
import org.elasticsearch.xpack.core.inference.results.UnifiedChatCompletionException;
import org.elasticsearch.xpack.inference.external.http.HttpClientManager;
import org.elasticsearch.xpack.inference.external.http.sender.HttpRequestSender;
import org.elasticsearch.xpack.inference.external.http.sender.HttpRequestSenderTests;
import org.elasticsearch.xpack.inference.logging.ThrottlerManager;
import org.elasticsearch.xpack.inference.services.AbstractInferenceServiceTests;
import org.elasticsearch.xpack.inference.services.InferenceEventsAssertion;
import org.elasticsearch.xpack.inference.services.SenderService;
import org.elasticsearch.xpack.inference.services.ServiceFields;
import org.elasticsearch.xpack.inference.services.ai21.completion.Ai21ChatCompletionModel;
import org.elasticsearch.xpack.inference.services.ai21.completion.Ai21ChatCompletionModelTests;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.ExceptionsHelper.unwrapCause;
import static org.elasticsearch.common.xcontent.XContentHelper.toXContent;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertToXContentEquivalent;
import static org.elasticsearch.xcontent.ToXContent.EMPTY_PARAMS;
import static org.elasticsearch.xpack.inference.Utils.inferenceUtilityExecutors;
import static org.elasticsearch.xpack.inference.Utils.mockClusterServiceEmpty;
import static org.elasticsearch.xpack.inference.external.http.Utils.getUrl;
import static org.elasticsearch.xpack.inference.services.ServiceComponentsTests.createWithEmptySettings;
import static org.elasticsearch.xpack.inference.services.ai21.completion.Ai21ChatCompletionModelTests.createChatCompletionModel;
import static org.elasticsearch.xpack.inference.services.ai21.completion.Ai21ChatCompletionServiceSettingsTests.getServiceSettingsMap;
import static org.elasticsearch.xpack.inference.services.settings.DefaultSecretSettingsTests.getSecretSettingsMap;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.isA;
import static org.mockito.Mockito.mock;

public class Ai21ServiceTests extends AbstractInferenceServiceTests {
    private static final TimeValue TIMEOUT = new TimeValue(30, TimeUnit.SECONDS);
    private final MockWebServer webServer = new MockWebServer();
    private ThreadPool threadPool;
    private HttpClientManager clientManager;

    public Ai21ServiceTests() {
        super(createTestConfiguration());
    }

    public static AbstractInferenceServiceTests.TestConfiguration createTestConfiguration() {
        return new AbstractInferenceServiceTests.TestConfiguration.Builder(
            new AbstractInferenceServiceTests.CommonConfig(
                TaskType.COMPLETION,
                TaskType.TEXT_EMBEDDING,
                EnumSet.of(TaskType.COMPLETION, TaskType.CHAT_COMPLETION)
            ) {

                @Override
                protected SenderService createService(ThreadPool threadPool, HttpClientManager clientManager) {
                    return Ai21ServiceTests.createService(threadPool, clientManager);
                }

                @Override
                protected Map<String, Object> createServiceSettingsMap(TaskType taskType) {
                    return Ai21ServiceTests.createServiceSettingsMap();
                }

                @Override
                protected Map<String, Object> createTaskSettingsMap() {
                    return new HashMap<>();
                }

                @Override
                protected Map<String, Object> createSecretSettingsMap() {
                    return Ai21ServiceTests.createSecretSettingsMap();
                }

                @Override
                protected void assertModel(Model model, TaskType taskType, boolean modelIncludesSecrets) {
                    Ai21ServiceTests.assertModel(model, taskType, modelIncludesSecrets);
                }

                @Override
                protected EnumSet<TaskType> supportedStreamingTasks() {
                    return EnumSet.of(TaskType.CHAT_COMPLETION, TaskType.COMPLETION);
                }
            }
        ).build();
    }

    private static void assertModel(Model model, TaskType taskType, boolean modelIncludesSecrets) {
        switch (taskType) {
            case COMPLETION -> assertCompletionModel(model, modelIncludesSecrets);
            case CHAT_COMPLETION -> assertChatCompletionModel(model, modelIncludesSecrets);
            default -> fail("unexpected task type [" + taskType + "]");
        }
    }

    private static Ai21Model assertCommonModelFields(Model model, boolean modelIncludesSecrets) {
        assertThat(model, instanceOf(Ai21Model.class));

        var customModel = (Ai21Model) model;
        assertThat(customModel.uri.toString(), Matchers.is("https://api.ai21.com/studio/v1/chat/completions"));
        assertThat(customModel.getTaskSettings(), Matchers.is(EmptyTaskSettings.INSTANCE));

        if (modelIncludesSecrets) {
            assertThat(customModel.getSecretSettings().apiKey(), Matchers.is(new SecureString("secret".toCharArray())));
        }

        return customModel;
    }

    private static void assertCompletionModel(Model model, boolean modelIncludesSecrets) {
        var customModel = assertCommonModelFields(model, modelIncludesSecrets);
        assertThat(customModel.getTaskType(), Matchers.is(TaskType.COMPLETION));
    }

    private static void assertChatCompletionModel(Model model, boolean modelIncludesSecrets) {
        var customModel = assertCommonModelFields(model, modelIncludesSecrets);
        assertThat(customModel.getTaskType(), Matchers.is(TaskType.CHAT_COMPLETION));
    }

    public static SenderService createService(ThreadPool threadPool, HttpClientManager clientManager) {
        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);
        return new Ai21Service(senderFactory, createWithEmptySettings(threadPool), mockClusterServiceEmpty());
    }

    private static Map<String, Object> createServiceSettingsMap() {
        return new HashMap<>(Map.of(ServiceFields.MODEL_ID, "model_id"));
    }

    private static Map<String, Object> createSecretSettingsMap() {
        return new HashMap<>(Map.of("api_key", "secret"));
    }

    @Before
    public void init() throws Exception {
        webServer.start();
        threadPool = createThreadPool(inferenceUtilityExecutors());
        clientManager = HttpClientManager.create(Settings.EMPTY, threadPool, mockClusterServiceEmpty(), mock(ThrottlerManager.class));
    }

    @After
    public void shutdown() throws IOException {
        clientManager.close();
        terminate(threadPool);
        webServer.close();
    }

    public void testParseRequestConfig_CreatesChatCompletionsModel() throws IOException {
        var url = "url";
        var model = "model";
        var secret = "secret";

        try (var service = createService()) {
            ActionListener<Model> modelVerificationListener = ActionListener.wrap(m -> {
                assertThat(m, instanceOf(Ai21ChatCompletionModel.class));

                var chatCompletionModel = (Ai21ChatCompletionModel) m;

                chatCompletionModel.setURI(url);
                assertThat(chatCompletionModel.uri().toString(), is("url"));
                assertThat(chatCompletionModel.getServiceSettings().modelId(), is(model));
                assertThat((chatCompletionModel.getSecretSettings()).apiKey().toString(), is("secret"));

            }, exception -> fail("Unexpected exception: " + exception));

            service.parseRequestConfig(
                "id",
                TaskType.CHAT_COMPLETION,
                getRequestConfigMap(getServiceSettingsMap(model), getSecretSettingsMap(secret)),
                modelVerificationListener
            );
        }
    }

    public void testParseRequestConfig_ThrowsException_WithoutModelId() throws IOException {
        var url = "url";
        var secret = "secret";

        try (var service = createService()) {
            ActionListener<Model> modelVerificationListener = ActionListener.wrap(m -> {
                assertThat(m, instanceOf(Ai21ChatCompletionModel.class));

                var chatCompletionModel = (Ai21ChatCompletionModel) m;

                assertThat(chatCompletionModel.uri().toString(), is(url));
                assertNull(chatCompletionModel.getServiceSettings().modelId());
                assertThat(chatCompletionModel.getSecretSettings().apiKey().toString(), is("secret"));

            }, exception -> {
                assertThat(exception, instanceOf(ValidationException.class));
                assertThat(
                    exception.getMessage(),
                    is("Validation Failed: 1: [service_settings] does not contain the required setting [model_id];")
                );
            });

            service.parseRequestConfig(
                "id",
                TaskType.CHAT_COMPLETION,
                getRequestConfigMap(getServiceSettingsMap(null), getSecretSettingsMap(secret)),
                modelVerificationListener
            );
        }
    }

    public void testUnifiedCompletionInfer() throws Exception {
        // The escapes are because the streaming response must be on a single line
        String responseJson = """
            data: {\
                 "id": "chatcmpl-09454f6e-ef80-ccaa-2d87-5fad309560f6",\
                 "choices": [\
                     {\
                         "index": 0,\
                         "delta": {\
                             "content": "Hello there"\
                         },\
                         "logprobs": null,\
                         "finish_reason": null\
                     }\
                 ]\
             }

            """;
        webServer.enqueue(new MockResponse().setResponseCode(200).setBody(responseJson));

        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);
        try (var service = new Ai21Service(senderFactory, createWithEmptySettings(threadPool), mockClusterServiceEmpty())) {
            var model = createChatCompletionModel(getUrl(webServer), "secret", "model");
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
            InferenceEventsAssertion.assertThat(result).hasFinishedStream().hasNoErrors().hasEvent(XContentHelper.stripWhitespace("""
                    {
                         "id": "chatcmpl-09454f6e-ef80-ccaa-2d87-5fad309560f6",
                         "choices": [{
                                 "delta": {
                                     "content": "Hello there"
                                 },
                                 "index": 0
                             }
                         ],
                         "model": null,
                         "object": null
                     }

                """));
        }
    }

    public void testUnifiedCompletionNonStreamingNotFoundError() throws Exception {
        String responseJson = """
            {
                "detail": "Not Found"
            }
            """;
        webServer.enqueue(new MockResponse().setResponseCode(404).setBody(responseJson));

        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);
        try (var service = new Ai21Service(senderFactory, createWithEmptySettings(threadPool), mockClusterServiceEmpty())) {
            var model = createChatCompletionModel(getUrl(webServer), "secret", "model");
            var latch = new CountDownLatch(1);
            service.unifiedCompletionInfer(
                model,
                UnifiedCompletionRequest.of(
                    List.of(new UnifiedCompletionRequest.Message(new UnifiedCompletionRequest.ContentString("hello"), "user", null, null))
                ),
                InferenceAction.Request.DEFAULT_TIMEOUT,
                ActionListener.runAfter(ActionTestUtils.assertNoSuccessListener(e -> {
                    try (var builder = XContentFactory.jsonBuilder()) {
                        var t = unwrapCause(e);
                        assertThat(t, isA(UnifiedChatCompletionException.class));
                        ((UnifiedChatCompletionException) t).toXContentChunked(EMPTY_PARAMS).forEachRemaining(xContent -> {
                            try {
                                xContent.toXContent(builder, EMPTY_PARAMS);
                            } catch (IOException ex) {
                                throw new RuntimeException(ex);
                            }
                        });
                        var json = XContentHelper.convertToJson(BytesReference.bytes(builder), false, builder.contentType());
                        assertThat(json, is(String.format(Locale.ROOT, XContentHelper.stripWhitespace("""
                            {
                              "error" : {
                                "code" : "not_found",
                                "message" : "Resource not found at [%s] for request from inference entity id [id] status \
                            [404]. Error message: [{\\n    \\"detail\\": \\"Not Found\\"\\n}\\n]",
                                "type" : "ai21_error"
                              }
                            }"""), getUrl(webServer))));
                    } catch (IOException ex) {
                        throw new RuntimeException(ex);
                    }
                }), latch::countDown)
            );
            assertTrue(latch.await(30, TimeUnit.SECONDS));
        }
    }

    public void testMidStreamUnifiedCompletionError() throws Exception {
        String responseJson = """
            data: {"error": {"message": "400: Invalid value: Model 'ai213.12:3b' not found"}}

            """;
        webServer.enqueue(new MockResponse().setResponseCode(200).setBody(responseJson));
        testStreamError(XContentHelper.stripWhitespace("""
            {
                  "error": {
                      "message": "Received an error response for request from inference entity id [id].\
             Error message: [{\\"error\\": {\\"message\\": \\"400: Invalid value: Model 'ai213.12:3b' not found\\"}}]",
                      "type": "ai21_error"
                  }
              }
            """));
    }

    public void testInfer_StreamRequest() throws Exception {
        String responseJson = """
            data: {\
                "id": "chatcmpl-8425dd3d-78f3-4143-93cb-dd576ab8ae26",\
                "choices": [{\
                        "delta": {\
                            "content": "Hello there"\
                        },\
                        "index": 0,\
                        "logprobs": null,\
                        "finish_reason": null\
                    }\
                ]\
            }

            """;
        webServer.enqueue(new MockResponse().setResponseCode(200).setBody(responseJson));

        streamCompletion().hasNoErrors().hasEvent("""
            {"completion":[{"delta":"Hello there"}]}""");
    }

    private void testStreamError(String expectedResponse) throws Exception {
        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);
        try (var service = new Ai21Service(senderFactory, createWithEmptySettings(threadPool), mockClusterServiceEmpty())) {
            var model = createChatCompletionModel(getUrl(webServer), "secret", "model");
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

    public void testInfer_StreamRequest_ErrorResponse() {
        String responseJson = """
            {
                "detail": "Not Found"
            }""";
        webServer.enqueue(new MockResponse().setResponseCode(404).setBody(responseJson));

        var e = assertThrows(ElasticsearchStatusException.class, this::streamCompletion);
        assertThat(e.status(), equalTo(RestStatus.NOT_FOUND));
        assertThat(e.getMessage(), equalTo(String.format(Locale.ROOT, """
            Resource not found at [%s] for request from inference entity id [id] status [404]. Error message: [{
                "detail": "Not Found"
            }]""", getUrl(webServer))));
    }

    public void testInfer_StreamRequestRetry() throws Exception {
        webServer.enqueue(new MockResponse().setResponseCode(503).setBody("""
            {
              "error": {
                "message": "server busy"
              }
            }"""));
        webServer.enqueue(new MockResponse().setResponseCode(200).setBody("""
            data: {\
                "id": "chatcmpl-8425dd3d-78f3-4143-93cb-dd576ab8ae26",\
                "choices": [{\
                        "delta": {\
                            "content": "Hello there",\
                            "role": "assistant"\
                        },\
                        "index": 0,\
                        "logprobs": null,\
                        "finish_reason": null\
                    }\
                ]\
            }

            """));

        streamCompletion().hasNoErrors().hasEvent("""
            {"completion":[{"delta":"Hello there"}]}""");
    }

    public void testSupportsStreaming() throws IOException {
        try (var service = new Ai21Service(mock(), createWithEmptySettings(mock()), mockClusterServiceEmpty())) {
            assertThat(service.supportedStreamingTasks(), is(EnumSet.of(TaskType.COMPLETION, TaskType.CHAT_COMPLETION)));
            assertFalse(service.canStream(TaskType.ANY));
        }
    }

    public void testGetConfiguration() throws Exception {
        try (var service = createService()) {
            String content = XContentHelper.stripWhitespace("""
                {
                       "service": "ai21",
                       "name": "AI21",
                       "task_types": ["completion", "chat_completion"],
                       "configurations": {
                           "api_key": {
                               "description": "API Key for the provider you're connecting to.",
                               "label": "API Key",
                               "required": true,
                               "sensitive": true,
                               "updatable": true,
                               "type": "str",
                               "supported_task_types": ["completion", "chat_completion"]
                           },
                           "model_id": {
                               "description": "Refer to the AI21 models documentation for the list of available inference models.",
                               "label": "Model",
                               "required": true,
                               "sensitive": false,
                               "updatable": false,
                               "type": "str",
                               "supported_task_types": ["completion", "chat_completion"]
                           },
                           "rate_limit.requests_per_minute": {
                               "description": "Minimize the number of rate limit errors.",
                               "label": "Rate Limit",
                               "required": false,
                               "sensitive": false,
                               "updatable": false,
                               "type": "int",
                               "supported_task_types": ["completion", "chat_completion"]
                           }
                       }
                   }
                """);
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

    private InferenceEventsAssertion streamCompletion() throws Exception {
        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);
        try (var service = new Ai21Service(senderFactory, createWithEmptySettings(threadPool), mockClusterServiceEmpty())) {
            var model = Ai21ChatCompletionModelTests.createCompletionModel(getUrl(webServer), "secret", "model");
            PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
            service.infer(
                model,
                null,
                null,
                null,
                List.of("abc"),
                true,
                new HashMap<>(),
                InputType.INGEST,
                InferenceAction.Request.DEFAULT_TIMEOUT,
                listener
            );

            return InferenceEventsAssertion.assertThat(listener.actionGet(TIMEOUT)).hasFinishedStream();
        }
    }

    private Ai21Service createService() {
        return new Ai21Service(mock(HttpRequestSender.Factory.class), createWithEmptySettings(threadPool), mockClusterServiceEmpty());
    }

    private Map<String, Object> getRequestConfigMap(Map<String, Object> serviceSettings, Map<String, Object> secretSettings) {
        var builtServiceSettings = new HashMap<>();
        builtServiceSettings.putAll(serviceSettings);
        builtServiceSettings.putAll(secretSettings);

        return new HashMap<>(Map.of(ModelConfigurations.SERVICE_SETTINGS, builtServiceSettings));
    }
}
