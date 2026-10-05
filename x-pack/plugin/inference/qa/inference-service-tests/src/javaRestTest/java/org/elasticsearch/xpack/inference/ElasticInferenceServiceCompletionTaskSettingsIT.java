/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference;

import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.ResponseListener;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.inference.completion.UnifiedCompletionUtils;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.http.MockResponse;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xpack.inference.services.elastic.ElasticInferenceServiceSettings;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.xpack.inference.InferenceBaseRestTest.deleteModel;
import static org.elasticsearch.xpack.inference.InferenceBaseRestTest.getModel;
import static org.elasticsearch.xpack.inference.InferenceBaseRestTest.putModel;
import static org.elasticsearch.xpack.inference.InferenceBaseRestTest.updateEndpoint;
import static org.elasticsearch.xpack.inference.services.elastic.ccm.CCMSettings.CCM_SUPPORTED_ENVIRONMENT;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

/**
 * Integration tests for the {@code reasoning} task setting on the Elastic Inference Service
 * {@code chat_completion} endpoint. The cluster is wired to a local
 * {@link MockElasticInferenceServiceAuthorizationServer} via {@code ELASTIC_INFERENCE_SERVICE_URL}.
 * Authorization is fully disabled and {@code xpack.inference.skip_validate_and_start=true} suppresses the
 * EIS validation call on PUT, so the mock only receives requests that tests explicitly trigger via
 * {@code _stream} or the update-validation path.
 *
 * <p>Note: {@code skip_validate_and_start} applies only to PUT, not to {@code _update}. Updating a
 * {@code chat_completion} endpoint always triggers a live {@code unifiedCompletionInfer} call to the mock,
 * so tests that update a {@code chat_completion} endpoint must pre-enqueue an {@link #SSE_RESPONSE}.
 *
 * <p>Scenarios:
 * <ol>
 *   <li>PUT {@code chat_completion} with {@code task_settings.reasoning} → 200/201; GET round-trips effort
 *       and summary.</li>
 *   <li>PUT {@code completion} (wrong task type) with {@code task_settings.reasoning} → 400.</li>
 *   <li>{@code _stream} without body reasoning applies the stored reasoning in the outgoing EIS request.</li>
 *   <li>{@code _stream} with body reasoning overrides the stored reasoning (body wins).</li>
 *   <li>PUT {@code chat_completion} with invalid reasoning ({@code enabled=false} without {@code effort})
 *       → 400.</li>
 *   <li>UPDATE {@code chat_completion} from empty task settings to containing reasoning → GET confirms
 *       reasoning is stored.</li>
 *   <li>UPDATE {@code chat_completion} from reasoning to {@code "reasoning": null} (explicit clear) → GET
 *       confirms reasoning is removed.</li>
 *   <li>UPDATE {@code completion} with reasoning → 400 (unknown settings).</li>
 * </ol>
 */
public class ElasticInferenceServiceCompletionTaskSettingsIT extends ESRestTestCase {

    private static final String SERVICE = "elastic";
    private static final String MODEL_ID = "eis-test-model";
    private static final String EFFORT_MEDIUM = "medium";
    private static final String EFFORT_LOW = "low";
    private static final String EFFORT_HIGH = "high";
    private static final String SUMMARY_DETAILED = "detailed";

    /**
     * Minimal single-chunk SSE streaming response body. Content does not affect request-inspection
     * assertions; it only needs to be a valid SSE stream so the {@link AsyncInferenceResponseConsumer}
     * completes normally and the outgoing EIS request is captured by the mock.
     */
    private static final String SSE_RESPONSE = """
        data: {\
            "id":"1",\
            "object":"completion",\
            "created":1677858242,\
            "model":"eis-test-model",\
            "choices":[\
                {\
                    "finish_reason":"stop",\
                    "index":0,\
                    "delta":{\
                        "role":"assistant",\
                        "content":"Hello world!"\
                    }\
                }\
            ]\
        }

        data: [DONE]

        """;

    private static final MockElasticInferenceServiceAuthorizationServer mockEISServer =
        new MockElasticInferenceServiceAuthorizationServer();

    private static final ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .distribution(DistributionType.DEFAULT)
        .setting("xpack.security.enabled", "false")
        .setting("xpack.license.self_generated.type", "trial")
        .setting(CCM_SUPPORTED_ENVIRONMENT.getKey(), "false")
        .setting(ElasticInferenceServiceSettings.ELASTIC_INFERENCE_SERVICE_URL.getKey(), mockEISServer::getUrl)
        .setting(ElasticInferenceServiceSettings.AUTHORIZATION_ENABLED.getKey(), "false")
        .setting(ElasticInferenceServiceSettings.PERIODIC_AUTHORIZATION_ENABLED.getKey(), "false")
        .setting("xpack.inference.skip_validate_and_start", "true")
        .build();

    // The mock server must start before the cluster so its URL is bound when the cluster reads the setting.
    @ClassRule
    public static final TestRule ruleChain = RuleChain.outerRule(mockEISServer).around(cluster);

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    @SuppressWarnings("unchecked")
    public void testCreateChatCompletionEndpoint_WithReasoning_RoundTripsOnGet() throws Exception {
        var inferenceId = "test-reasoning-roundtrip";
        try {
            putModel(inferenceId, chatCompletionConfig(EFFORT_MEDIUM, SUMMARY_DETAILED), TaskType.CHAT_COMPLETION);

            var endpoint = getModel(inferenceId);
            var taskSettings = (Map<String, Object>) endpoint.get(ModelConfigurations.TASK_SETTINGS);
            var reasoning = (Map<String, Object>) taskSettings.get(UnifiedCompletionUtils.REASONING_FIELD);
            assertThat(reasoning.get(UnifiedCompletionUtils.EFFORT_FIELD), is(EFFORT_MEDIUM));
            assertThat(reasoning.get(UnifiedCompletionUtils.SUMMARY_FIELD), is(SUMMARY_DETAILED));
        } finally {
            deleteModel(inferenceId);
        }
    }

    public void testCreateCompletionEndpoint_WithReasoning_IsRejected() throws IOException {
        var request = new Request("PUT", "_inference/completion/test-completion-reasoning-rejected");
        request.setJsonEntity(chatCompletionConfig(EFFORT_MEDIUM, null));

        var e = expectThrows(ResponseException.class, () -> client().performRequest(request));
        assertThat(e.getResponse().getStatusLine().getStatusCode(), is(400));
        assertThat(
            e.getMessage(),
            containsString(
                Strings.format(
                    "[%s] Configuration contains unknown settings [%s]",
                    ModelConfigurations.TASK_SETTINGS,
                    UnifiedCompletionUtils.REASONING_FIELD
                )
            )
        );
    }

    @SuppressWarnings("unchecked")
    public void testStoredReasoning_IsAppliedToOutgoingRequest_WhenBodyOmitsIt() throws Exception {
        var inferenceId = "test-stored-reasoning-applied";
        try {
            putModel(inferenceId, chatCompletionConfig(EFFORT_MEDIUM, null), TaskType.CHAT_COMPLETION);
            mockEISServer.getWebServer().enqueue(new MockResponse().setResponseCode(200).setBody(SSE_RESPONSE));

            executeStreamRequest(inferenceId, XContentHelper.stripWhitespace("""
                {
                  "messages": [ { "role": "user", "content": "hi" } ]
                }
                """));

            var capturedBody = mockEISServer.getWebServer().requests().getLast().getBody();
            var requestMap = entityAsMap(new StringEntity(capturedBody, ContentType.APPLICATION_JSON));
            var reasoning = (Map<String, Object>) requestMap.get(UnifiedCompletionUtils.REASONING_FIELD);
            assertThat(reasoning.get(UnifiedCompletionUtils.EFFORT_FIELD), is(EFFORT_MEDIUM));
        } finally {
            deleteModel(inferenceId);
        }
    }

    @SuppressWarnings("unchecked")
    public void testRequestBodyReasoning_WinsOverStoredReasoning() throws Exception {
        var inferenceId = "test-body-reasoning-wins";
        try {
            putModel(inferenceId, chatCompletionConfig(EFFORT_LOW, null), TaskType.CHAT_COMPLETION);
            mockEISServer.getWebServer().enqueue(new MockResponse().setResponseCode(200).setBody(SSE_RESPONSE));

            executeStreamRequest(inferenceId, XContentHelper.stripWhitespace(Strings.format("""
                {
                  "messages": [ { "role": "user", "content": "hi" } ],
                  "reasoning": { "effort": "%s" }
                }
                """, EFFORT_HIGH)));

            var capturedBody = mockEISServer.getWebServer().requests().getLast().getBody();
            var requestMap = entityAsMap(new StringEntity(capturedBody, ContentType.APPLICATION_JSON));
            var reasoning = (Map<String, Object>) requestMap.get(UnifiedCompletionUtils.REASONING_FIELD);
            assertThat(reasoning.get(UnifiedCompletionUtils.EFFORT_FIELD), is(EFFORT_HIGH));
        } finally {
            deleteModel(inferenceId);
        }
    }

    public void testCreateChatCompletionEndpoint_WithInvalidReasoning_IsRejected() {
        // enabled=false without effort is invalid per Reasoning.validateFields
        var config = Strings.format("""
            {
              "service": "%s",
              "service_settings": { "model_id": "%s" },
              "task_settings": { "reasoning": { "enabled": false } }
            }
            """, SERVICE, MODEL_ID);
        var request = new Request("PUT", "_inference/chat_completion/test-invalid-reasoning-rejected");
        request.setJsonEntity(config);

        var e = expectThrows(ResponseException.class, () -> client().performRequest(request));
        assertThat(e.getResponse().getStatusLine().getStatusCode(), is(400));
        assertThat(
            e.getMessage(),
            containsString(
                Strings.format(
                    "When [%s] is false, [%s] must be specified.",
                    UnifiedCompletionUtils.ENABLED_FIELD,
                    UnifiedCompletionUtils.EFFORT_FIELD
                )
            )
        );
    }

    @SuppressWarnings("unchecked")
    public void testUpdateChatCompletionEndpoint_AddsReasoning_WhenStartingEmpty() throws Exception {
        var inferenceId = "test-update-adds-reasoning";
        try {
            putModel(inferenceId, configWithoutReasoning(), TaskType.CHAT_COMPLETION);

            var initialEndpoint = getModel(inferenceId);
            // A chat_completion endpoint created without task settings does not serialize a task_settings field on
            // GET (ModelConfigurations omits empty task settings from responses), so the map is null here. This
            // mirrors testUpdateChatCompletionEndpoint_ClearsReasoning_WhenSetToNull.
            assertNull(initialEndpoint.get(ModelConfigurations.TASK_SETTINGS));

            mockEISServer.getWebServer().enqueue(new MockResponse().setResponseCode(200).setBody(SSE_RESPONSE));
            updateEndpoint(inferenceId, reasoningTaskSettingsUpdate(EFFORT_MEDIUM), TaskType.CHAT_COMPLETION);

            var updatedEndpoint = getModel(inferenceId);
            var updatedTaskSettings = (Map<String, Object>) updatedEndpoint.get(ModelConfigurations.TASK_SETTINGS);
            var reasoning = (Map<String, Object>) updatedTaskSettings.get(UnifiedCompletionUtils.REASONING_FIELD);
            assertThat(reasoning.get(UnifiedCompletionUtils.EFFORT_FIELD), is(EFFORT_MEDIUM));
        } finally {
            deleteModel(inferenceId);
        }
    }

    @SuppressWarnings("unchecked")
    public void testUpdateChatCompletionEndpoint_ClearsReasoning_WhenSetToNull() throws Exception {
        var inferenceId = "test-update-clears-reasoning";
        try {
            putModel(inferenceId, chatCompletionConfig(EFFORT_MEDIUM, SUMMARY_DETAILED), TaskType.CHAT_COMPLETION);

            var initialEndpoint = getModel(inferenceId);
            var initialTaskSettings = (Map<String, Object>) initialEndpoint.get(ModelConfigurations.TASK_SETTINGS);
            var initialReasoning = (Map<String, Object>) initialTaskSettings.get(UnifiedCompletionUtils.REASONING_FIELD);
            assertThat(initialReasoning.get(UnifiedCompletionUtils.EFFORT_FIELD), is(EFFORT_MEDIUM));

            mockEISServer.getWebServer().enqueue(new MockResponse().setResponseCode(200).setBody(SSE_RESPONSE));
            updateEndpoint(inferenceId, clearReasoningTaskSettingsUpdate(), TaskType.CHAT_COMPLETION);

            var updatedEndpoint = getModel(inferenceId);
            var updatedTaskSettings = (Map<String, Object>) updatedEndpoint.get(ModelConfigurations.TASK_SETTINGS);
            // When the reasoning field is cleared there won't be any other task settings so ModelConfigurations won't serialize them
            // because the map is empty. So we should get null here instead of an empty map.
            assertNull(updatedTaskSettings);
        } finally {
            deleteModel(inferenceId);
        }
    }

    public void testUpdateCompletionEndpoint_WithReasoning_IsRejected() throws IOException {
        var inferenceId = "test-update-completion-reasoning-rejected";
        try {
            putModel(inferenceId, configWithoutReasoning(), TaskType.COMPLETION);

            var e = expectThrows(
                ResponseException.class,
                () -> updateEndpoint(inferenceId, reasoningTaskSettingsUpdate(EFFORT_MEDIUM), TaskType.COMPLETION)
            );
            assertThat(e.getResponse().getStatusLine().getStatusCode(), is(400));
            assertThat(
                e.getMessage(),
                containsString(
                    Strings.format(
                        "[%s] Configuration contains unknown settings [%s]",
                        ModelConfigurations.TASK_SETTINGS,
                        UnifiedCompletionUtils.REASONING_FIELD
                    )
                )
            );
        } finally {
            deleteModel(inferenceId);
        }
    }

    public static String chatCompletionConfig(String effort, String summary) throws IOException {
        String reasoning;
        if (summary != null) {
            reasoning = Strings.format("""
                {
                  "effort": "%s",
                  "summary": "%s"
                }
                """, effort, summary);
        } else {
            reasoning = Strings.format("""
                {
                  "effort": "%s"
                }
                """, effort);
        }

        return XContentHelper.stripWhitespace(Strings.format("""
            {
              "service": "%s",
              "service_settings": { "model_id": "%s" },
              "task_settings": { "reasoning": %s }
            }
            """, SERVICE, MODEL_ID, reasoning));
    }

    /**
     * Builds a PUT body with no {@code task_settings}, producing an endpoint with empty task settings.
     * The task type is supplied via the URL by the caller.
     */
    public static String configWithoutReasoning() throws IOException {
        return XContentHelper.stripWhitespace(Strings.format("""
            {
              "service": "%s",
              "service_settings": { "model_id": "%s" }
            }
            """, SERVICE, MODEL_ID));
    }

    /**
     * Builds an update body that sets {@code task_settings.reasoning} to the given effort value.
     */
    public static String reasoningTaskSettingsUpdate(String effort) throws IOException {
        return XContentHelper.stripWhitespace(Strings.format("""
            {
              "task_settings": { "reasoning": { "effort": "%s" } }
            }
            """, effort));
    }

    /**
     * Builds an update body that explicitly clears {@code task_settings.reasoning} via {@code null}.
     * The tri-state merge semantics in {@code updatedTaskSettings} treat an explicit {@code null}
     * as "clear the stored value", unlike an omitted field which keeps it.
     */
    private static String clearReasoningTaskSettingsUpdate() throws IOException {
        return XContentHelper.stripWhitespace("""
            {
              "task_settings": { "reasoning": null }
            }
            """);
    }

    /**
     * Fires a {@code POST _inference/chat_completion/{inferenceId}/_stream} request asynchronously and
     * blocks until the response is fully consumed. The purpose is to trigger the node to forward the
     * outgoing request to the EIS mock; SSE events are not asserted.
     */
    private void executeStreamRequest(String inferenceId, String body) throws Exception {
        var request = new Request("POST", "_inference/chat_completion/" + inferenceId + "/_stream");
        request.setJsonEntity(body);

        var responseConsumer = new AsyncInferenceResponseConsumer();
        request.setOptions(RequestOptions.DEFAULT.toBuilder().setHttpAsyncResponseConsumerFactory(() -> responseConsumer).build());
        var latch = new CountDownLatch(1);
        var failure = new AtomicReference<Exception>();
        client().performRequestAsync(request, new ResponseListener() {
            @Override
            public void onSuccess(Response response) {
                latch.countDown();
            }

            @Override
            public void onFailure(Exception exception) {
                failure.set(exception);
                latch.countDown();
            }
        });
        assertTrue(latch.await(ESRestTestCase.TEST_REQUEST_TIMEOUT.getSeconds(), TimeUnit.SECONDS));
        assertNull(failure.get());
    }
}
