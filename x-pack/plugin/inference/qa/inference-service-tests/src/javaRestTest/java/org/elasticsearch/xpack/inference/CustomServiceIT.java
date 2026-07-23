/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference;

import org.apache.http.HttpHeaders;
import org.elasticsearch.common.Strings;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.test.http.MockResponse;
import org.elasticsearch.test.http.MockWebServer;
import org.elasticsearch.xpack.inference.common.ValidatingSubstitutor;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.util.List;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

/**
 * End-to-end REST integration test for the Custom inference service.
 *
 * <p>Starts an in-process {@link MockWebServer} standing in for the upstream API.
 * Creates a {@code text_embedding} inference endpoint pointing at the mock server,
 * then runs inference and asserts that the request was built correctly.
 *
 * <p>Running inside a real default-distribution cluster means the inference plugin is loaded
 * through {@code PluginsLoader.createModuleLayer} — the same JPMS module-layer path used in
 * production. A missing {@code requires} directive in {@code module-info.java} (e.g. for
 * {@code org.apache.commons.lang3}, which is a transitive runtime dependency of
 * {@code org.apache.commons.text.StringSubstitutor} used by the Custom service's request
 * template-substitution) would surface here as a {@code NoClassDefFoundError} at the point the
 * first inference request is built, NOT in classpath-based unit tests where JPMS boundaries are
 * not enforced.
 */
public class CustomServiceIT extends InferenceBaseRestTest {

    private static final String ENDPOINT_ID = "custom-text-embedding";

    /**
     * OpenAI-compatible embedding response whose float array is extracted by the
     * {@code json_parser} path {@code "$.data[*].embedding"} configured in the model below.
     */
    private static final String EMBEDDINGS_RESPONSE = """
        {
          "object": "list",
          "data": [
            {
              "object": "embedding",
              "index": 0,
              "embedding": [0.1, 0.2, 0.3]
            }
          ],
          "model": "text-embedding-3-small",
          "usage": {"prompt_tokens": 1, "total_tokens": 1}
        }
        """;

    private static final String INFERENCE_INPUT = "hello";

    private static MockWebServer webServer;

    @BeforeClass
    public static void startWebServer() throws Exception {
        webServer = new MockWebServer();
        webServer.start();
    }

    @AfterClass
    public static void stopWebServer() {
        webServer.close();
    }

    @After
    public void clearMockRequests() {
        webServer.clearRequests();
    }

    /**
     * Verifies that both the validation call during endpoint creation and the explicit inference
     * call reach the mock upstream, and that the Custom service's template-substitution ran
     * correctly (the {@code ${api_key}} placeholder in the Authorization header is replaced
     * with the configured secret value before the outbound request is sent).
     *
     * <p>Custom endpoint creation triggers a validation inference call to the configured
     * {@code url}, so two responses must be enqueued before calling {@code putModel}.
     */
    public void testCreate_AndInfer_SubstitutesTemplateVariables() throws Exception {
        var url = Strings.format("http://localhost:%d/v1/embeddings", webServer.getPort());
        // One response for the validation call fired during PUT, one for the explicit inference.
        webServer.enqueue(new MockResponse().setResponseCode(200).setBody(EMBEDDINGS_RESPONSE));
        webServer.enqueue(new MockResponse().setResponseCode(200).setBody(EMBEDDINGS_RESPONSE));

        var apiKey = "test-key";

        putModel(ENDPOINT_ID, customModelConfig(url, apiKey), TaskType.TEXT_EMBEDDING);
        var result = infer(ENDPOINT_ID, TaskType.TEXT_EMBEDDING, List.of(INFERENCE_INPUT));

        assertNonEmptyInferenceResults(result, 1, TaskType.TEXT_EMBEDDING);

        // Confirm the upstream received exactly two calls: the validation call on PUT and the
        // explicit inference call.
        assertThat(webServer.requests(), hasSize(2));

        // The Authorization header proves that ValidatingSubstitutor.replace() ran and resolved
        // ${api_key} to "test-key" — this is the StringSubstitutor code path that depends on
        // org.apache.commons.lang3 via org.apache.commons.text at runtime.
        webServer.requests()
            .forEach(req -> assertThat(req.getHeader(HttpHeaders.AUTHORIZATION), equalTo(Strings.format("Bearer %s", apiKey))));

        // The request body must contain the substituted input.
        assertThat(webServer.requests().get(1).getBody(), containsString(INFERENCE_INPUT));

        deleteModel(ENDPOINT_ID, TaskType.TEXT_EMBEDDING);
    }

    /**
     * Builds the PUT /_inference request body for a Custom text_embedding endpoint.
     *
     * <p>The {@code ${api_key}} placeholder in the Authorization header and the
     * {@code ${input}} placeholder in the request body are substituted at inference time by
     * {@code CustomRequest} via {@link ValidatingSubstitutor} (backed by
     * {@code org.apache.commons.text.StringSubstitutor}, which uses
     * {@code org.apache.commons.lang3} internally).
     */
    private static String customModelConfig(String url, String apiKey) {
        return Strings.format("""
            {
              "service": "custom",
              "service_settings": {
                "secret_parameters": {
                  "api_key": "%s"
                },
                "url": "%s",
                "headers": {
                  "Authorization": "Bearer ${api_key}",
                  "Content-Type": "application/json;charset=utf-8"
                },
                "request": "{\\"input\\": ${input}, \\"model\\": \\"text-embedding-3-small\\"}",
                "response": {
                  "json_parser": {
                    "text_embeddings": "$.data[*].embedding"
                  }
                }
              }
            }
            """, apiKey, url);
    }
}
