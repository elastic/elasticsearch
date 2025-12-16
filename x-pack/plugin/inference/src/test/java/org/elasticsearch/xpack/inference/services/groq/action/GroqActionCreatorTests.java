/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.groq.action;

import org.apache.http.HttpHeaders;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.http.MockRequest;
import org.elasticsearch.test.http.MockResponse;
import org.elasticsearch.test.http.MockWebServer;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.inference.action.InferenceAction;
import org.elasticsearch.xpack.inference.Utils;
import org.elasticsearch.xpack.inference.external.action.ExecutableAction;
import org.elasticsearch.xpack.inference.external.http.HttpClientManager;
import org.elasticsearch.xpack.inference.external.http.sender.ChatCompletionInput;
import org.elasticsearch.xpack.inference.external.http.sender.HttpRequestSenderTests;
import org.elasticsearch.xpack.inference.external.http.sender.Sender;
import org.elasticsearch.xpack.inference.logging.ThrottlerManager;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.ServiceComponentsTests;
import org.elasticsearch.xpack.inference.services.ServiceFields;
import org.elasticsearch.xpack.inference.services.groq.GroqService;
import org.elasticsearch.xpack.inference.services.groq.GroqUtils;
import org.elasticsearch.xpack.inference.services.groq.completion.GroqChatCompletionModel;
import org.elasticsearch.xpack.inference.services.openai.OpenAiServiceFields;
import org.elasticsearch.xpack.inference.services.settings.DefaultSecretSettings;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.xpack.core.inference.results.ChatCompletionResultsTests.buildExpectationCompletion;
import static org.elasticsearch.xpack.inference.external.http.Utils.entityAsMap;
import static org.elasticsearch.xpack.inference.external.http.Utils.getUrl;
import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.startsWith;
import static org.mockito.Mockito.mock;

public class GroqActionCreatorTests extends ESTestCase {

    private static final TimeValue TIMEOUT = new TimeValue(30, TimeUnit.SECONDS);
    private static final String API_KEY = "api-key";
    private static final String MODEL_ID = "llama-3.3-70b-versatile";
    private static final String ORG_ID = "org-1";
    private static final String USER_ID = "user-123";

    private final MockWebServer webServer = new MockWebServer();
    private ThreadPool threadPool;
    private HttpClientManager clientManager;

    @Before
    public void init() throws Exception {
        webServer.start();
        threadPool = createThreadPool(Utils.inferenceUtilityExecutors());
        clientManager = HttpClientManager.create(Settings.EMPTY, threadPool, Utils.mockClusterServiceEmpty(), mock(ThrottlerManager.class));
    }

    @After
    public void shutdown() throws IOException {
        clientManager.close();
        terminate(threadPool);
        webServer.close();
    }

    public void testExecute_IncludesHeadersAndUserFields() throws IOException {
        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);

        try (Sender sender = HttpRequestSenderTests.createSender(senderFactory)) {
            sender.startSynchronously();

            String responseJson = """
                {
                  "id": "chatcmpl-123",
                  "object": "chat.completion",
                  "created": 1731632140,
                  "model": "llama-3.3-70b-versatile",
                  "choices": [
                    {
                      "index": 0,
                      "message": {
                        "role": "assistant",
                        "content": "Hello there"
                      },
                      "logprobs": null,
                      "finish_reason": "stop"
                    }
                  ],
                  "usage": {
                    "prompt_tokens": 10,
                    "completion_tokens": 2,
                    "total_tokens": 12
                  },
                  "system_fingerprint": "fp_test"
                }
                """;

            webServer.enqueue(new MockResponse().setResponseCode(200).setBody(responseJson));

            var model = createModel(getUrl(webServer));
            var action = createAction(sender, model);

            PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
            action.execute(new ChatCompletionInput(List.of("hello")), InferenceAction.Request.DEFAULT_TIMEOUT, listener);

            var result = listener.actionGet(TIMEOUT);
            assertThat(result.asMap(), is(buildExpectationCompletion(List.of("Hello there"))));

            assertThat(webServer.requests(), hasSize(1));
            assertRequest(webServer.requests().getFirst());
        }
    }

    private ExecutableAction createAction(Sender sender, GroqChatCompletionModel model) {
        var actionCreator = new GroqActionCreator(sender, ServiceComponentsTests.createWithEmptySettings(threadPool));
        return actionCreator.create(model, Map.of());
    }

    private GroqChatCompletionModel createModel(String url) {
        Map<String, Object> serviceSettings = new HashMap<>();
        serviceSettings.put(ServiceFields.MODEL_ID, MODEL_ID);
        serviceSettings.put(ServiceFields.URL, url);
        serviceSettings.put(OpenAiServiceFields.ORGANIZATION, ORG_ID);

        Map<String, Object> taskSettings = new HashMap<>();
        taskSettings.put(OpenAiServiceFields.USER, USER_ID);
        taskSettings.put(OpenAiServiceFields.HEADERS, Map.of("X-Test", "1"));

        Map<String, Object> secretSettings = new HashMap<>();
        secretSettings.put(DefaultSecretSettings.API_KEY, API_KEY);

        return new GroqChatCompletionModel(
            "groq-test-inference",
            TaskType.CHAT_COMPLETION,
            GroqService.NAME,
            serviceSettings,
            taskSettings,
            secretSettings,
            ConfigurationParseContext.REQUEST
        );
    }

    private void assertRequest(MockRequest request) throws IOException {
        assertThat(request.getUri().getQuery(), is((String) null));
        assertThat(request.getHeader(HttpHeaders.CONTENT_TYPE), startsWith(XContentType.JSON.mediaTypeWithoutParameters()));
        assertThat(request.getHeader(HttpHeaders.AUTHORIZATION), equalTo(Strings.format("Bearer %s", API_KEY)));
        assertThat(request.getHeader(GroqUtils.createOrgHeader(ORG_ID).getName()), equalTo(ORG_ID));
        assertThat(request.getHeader("X-Test"), equalTo("1"));

        var payload = entityAsMap(request.getBody());
        assertThat(payload, aMapWithSize(5));
        assertThat(payload.get("model"), is(MODEL_ID));
        assertThat(payload.get("user"), is(USER_ID));
        assertThat(payload.get("n"), is(1));
        assertThat(payload.get("stream"), is(false));
        assertNull(payload.get("stream_options"));
        assertThat(payload.get("messages"), is(List.of(Map.of("role", "user", "content", "hello"))));
    }
}
