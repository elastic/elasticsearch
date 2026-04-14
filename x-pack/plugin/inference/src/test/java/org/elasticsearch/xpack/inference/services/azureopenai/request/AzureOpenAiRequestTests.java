/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.azureopenai.request;

import org.apache.http.client.methods.HttpPost;
import org.elasticsearch.inference.InputType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.inference.common.Truncator;
import org.elasticsearch.xpack.inference.common.TruncatorTests;
import org.elasticsearch.xpack.inference.common.parser.Headers;
import org.elasticsearch.xpack.inference.external.request.HttpRequest;
import org.elasticsearch.xpack.inference.external.request.RequestTests;
import org.elasticsearch.xpack.inference.services.azureopenai.embeddings.AzureOpenAiEmbeddingsModel;
import org.elasticsearch.xpack.inference.services.azureopenai.embeddings.AzureOpenAiEmbeddingsModelTests;
import org.elasticsearch.xpack.inference.services.azureopenai.request.embeddings.AzureOpenAiEmbeddingsRequestTests;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.inference.Utils.inferenceUtilityExecutors;
import static org.elasticsearch.xpack.inference.external.http.Utils.entityAsMap;
import static org.elasticsearch.xpack.inference.services.azureopenai.request.AzureOpenAiUtils.API_KEY_HEADER;
import static org.hamcrest.Matchers.is;

public class AzureOpenAiRequestTests extends ESTestCase {

    private static final String TEST_INFERENCE_ID_VALUE = "test-inference-id";
    private static final String RESOURCE_NAME_VALUE = "some_resource";
    private static final String DEPLOYMENT_ID_VALUE = "some_deployment";
    private static final String API_VERSION_VALUE = "2024";
    private static final String API_KEY_VALUE = "secret";
    private static final String INPUT_TEXT_VALUE = "some_input";
    private static final String TEST_INPUT_TEXT_VALUE = "test input text";
    private static final String CUSTOM_HEADER_NAME = "X-Custom-Header";
    private static final String CUSTOM_HEADER_VALUE = "custom-value";

    private ThreadPool threadPool;

    @Before
    public void init() throws Exception {
        threadPool = createThreadPool(inferenceUtilityExecutors());
    }

    @After
    public void shutdown() throws IOException {
        terminate(threadPool);
    }

    public void testCreateHttpRequest_SetsRequestBodyToRequestEntity() throws IOException {
        var request = AzureOpenAiEmbeddingsRequestTests.createRequest(
            TEST_INFERENCE_ID_VALUE,
            RESOURCE_NAME_VALUE,
            DEPLOYMENT_ID_VALUE,
            API_VERSION_VALUE,
            API_KEY_VALUE,
            null,
            TEST_INPUT_TEXT_VALUE,
            null,
            null,
            threadPool
        );
        HttpRequest httpRequest = RequestTests.getHttpRequestSync(request);

        var httpPost = (HttpPost) httpRequest.httpRequestBase();
        var requestMap = entityAsMap(httpPost.getEntity().getContent());
        assertThat(requestMap.get("input"), is(List.of(TEST_INPUT_TEXT_VALUE)));
    }

    public void testCreateHttpRequest_ReturnsHttpRequestWithInferenceEntityId() {
        var request = AzureOpenAiEmbeddingsRequestTests.createRequest(
            TEST_INFERENCE_ID_VALUE,
            RESOURCE_NAME_VALUE,
            DEPLOYMENT_ID_VALUE,
            API_VERSION_VALUE,
            API_KEY_VALUE,
            null,
            INPUT_TEXT_VALUE,
            null,
            null,
            threadPool
        );
        HttpRequest httpRequest = RequestTests.getHttpRequestSync(request);

        assertThat(httpRequest.inferenceEntityId(), is(TEST_INFERENCE_ID_VALUE));
    }

    public void testCreateHttpRequest_AppliesTaskSettingsHeadersWhenPresent() {
        var baseModel = AzureOpenAiEmbeddingsModelTests.createModel(
            RESOURCE_NAME_VALUE,
            DEPLOYMENT_ID_VALUE,
            API_VERSION_VALUE,
            null,
            API_KEY_VALUE,
            null,
            TEST_INFERENCE_ID_VALUE,
            threadPool
        );
        var modelWithHeaders = AzureOpenAiEmbeddingsModel.of(
            baseModel,
            Map.of(Headers.HEADERS_FIELD, Map.of(CUSTOM_HEADER_NAME, CUSTOM_HEADER_VALUE))
        );
        var request = new AzureOpenAiEmbeddingsRequest(
            TruncatorTests.createTruncator(),
            new Truncator.TruncationResult(List.of(INPUT_TEXT_VALUE), new boolean[] { false }),
            InputType.INGEST,
            modelWithHeaders
        );

        HttpRequest httpRequest = RequestTests.getHttpRequestSync(request);
        var httpPost = (HttpPost) httpRequest.httpRequestBase();

        assertThat(httpPost.getLastHeader(CUSTOM_HEADER_NAME).getValue(), is(CUSTOM_HEADER_VALUE));
    }

    // This test ensures that if the task settings include an auth header, it does not overwrite the header that the request logic adds
    // when a user specifies an API key in the service settings.
    public void testCreateHttpRequest_AppliesTaskSettingsHeadersWhenPresent_DoesNotOverwriteAuthHeader_WhenAuthHeaderIsAdded() {
        var baseModel = AzureOpenAiEmbeddingsModelTests.createModel(
            RESOURCE_NAME_VALUE,
            DEPLOYMENT_ID_VALUE,
            API_VERSION_VALUE,
            null,
            API_KEY_VALUE,
            null,
            TEST_INFERENCE_ID_VALUE,
            threadPool
        );
        var modelWithHeaders = AzureOpenAiEmbeddingsModel.of(
            baseModel,
            Map.of(Headers.HEADERS_FIELD, Map.of(API_KEY_HEADER, CUSTOM_HEADER_VALUE))
        );
        var request = new AzureOpenAiEmbeddingsRequest(
            TruncatorTests.createTruncator(),
            new Truncator.TruncationResult(List.of(INPUT_TEXT_VALUE), new boolean[] { false }),
            InputType.INGEST,
            modelWithHeaders
        );

        HttpRequest httpRequest = RequestTests.getHttpRequestSync(request);
        var httpPost = (HttpPost) httpRequest.httpRequestBase();

        assertThat(httpPost.getLastHeader(API_KEY_HEADER).getValue(), is(API_KEY_VALUE));
    }

    public void testCreateHttpRequest_DoesNotAddTaskSettingsHeadersWhenAbsent() {
        var request = AzureOpenAiEmbeddingsRequestTests.createRequest(
            TEST_INFERENCE_ID_VALUE,
            RESOURCE_NAME_VALUE,
            DEPLOYMENT_ID_VALUE,
            API_VERSION_VALUE,
            API_KEY_VALUE,
            null,
            INPUT_TEXT_VALUE,
            null,
            null,
            threadPool
        );
        HttpRequest httpRequest = RequestTests.getHttpRequestSync(request);

        var httpPost = (HttpPost) httpRequest.httpRequestBase();
        assertNull(httpPost.getFirstHeader(CUSTOM_HEADER_NAME));
    }
}
