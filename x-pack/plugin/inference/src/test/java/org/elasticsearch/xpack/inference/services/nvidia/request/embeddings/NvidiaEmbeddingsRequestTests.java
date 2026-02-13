/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.nvidia.request.embeddings;

import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.HttpPost;
import org.elasticsearch.common.Strings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.InputType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.inference.common.Truncator;
import org.elasticsearch.xpack.inference.common.TruncatorTests;
import org.elasticsearch.xpack.inference.common.model.Truncation;
import org.elasticsearch.xpack.inference.external.request.HttpRequest;
import org.elasticsearch.xpack.inference.services.nvidia.embeddings.NvidiaEmbeddingsModelTests;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.xpack.inference.external.http.Utils.entityAsMap;
import static org.elasticsearch.xpack.inference.services.nvidia.request.NvidiaRequestFields.INPUT_FIELD_NAME;
import static org.elasticsearch.xpack.inference.services.nvidia.request.NvidiaRequestFields.INPUT_TYPE_FIELD_NAME;
import static org.elasticsearch.xpack.inference.services.nvidia.request.NvidiaRequestFields.MODEL_FIELD_NAME;
import static org.elasticsearch.xpack.inference.services.nvidia.request.NvidiaRequestFields.TRUNCATE_FIELD_NAME;
import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

public class NvidiaEmbeddingsRequestTests extends ESTestCase {

    // Test values
    private static final String MODEL_VALUE = "some_model";
    private static final String INPUT_VALUE = "ABCD";
    private static final String URL_VALUE = "http://www.abc.com";
    private static final String URL_DEFAULT_VALUE = "https://integrate.api.nvidia.com/v1/embeddings";
    private static final String API_KEY_VALUE = "test_api_key";
    private static final InputType INPUT_TYPE_ELASTIC_INITIAL_VALUE = InputType.INGEST;
    private static final InputType INPUT_TYPE_ELASTIC_IGNORED_VALUE = InputType.SEARCH;
    private static final Truncation TRUNCATE_ELASTIC_VALUE = Truncation.START;
    private static final String INPUT_TYPE_NVIDIA_VALUE = "passage";
    private static final String INPUT_TYPE_NVIDIA_DEFAULT_VALUE = "query";
    private static final String TRUNCATE_NVIDIA_VALUE = "start";

    public void testCreateRequest_InputTypeFromTaskSettings_Success() throws IOException {
        var request = createRequest(URL_VALUE, INPUT_TYPE_ELASTIC_INITIAL_VALUE, TRUNCATE_ELASTIC_VALUE, null);
        assertCreateHttpRequest(request, INPUT_TYPE_NVIDIA_VALUE, TRUNCATE_NVIDIA_VALUE, URL_VALUE);
    }

    public void testCreateRequest_InputTypeFromRequest_Success() throws IOException {
        var request = createRequest(URL_VALUE, null, TRUNCATE_ELASTIC_VALUE, INPUT_TYPE_ELASTIC_INITIAL_VALUE);
        assertCreateHttpRequest(request, INPUT_TYPE_NVIDIA_VALUE, TRUNCATE_NVIDIA_VALUE, URL_VALUE);
    }

    public void testCreateRequest_InputTypeFromRequestPrioritized_Success() throws IOException {
        var request = createRequest(URL_VALUE, INPUT_TYPE_ELASTIC_IGNORED_VALUE, TRUNCATE_ELASTIC_VALUE, INPUT_TYPE_ELASTIC_INITIAL_VALUE);
        assertCreateHttpRequest(request, INPUT_TYPE_NVIDIA_VALUE, TRUNCATE_NVIDIA_VALUE, URL_VALUE);
    }

    public void testCreateRequest_OnlyMandatoryAndDefaultFields_Success() throws IOException {
        var request = createRequest(null, null, null, null);
        assertCreateHttpRequest(request, INPUT_TYPE_NVIDIA_DEFAULT_VALUE, null, URL_DEFAULT_VALUE);
    }

    private void assertCreateHttpRequest(
        NvidiaEmbeddingsRequest request,
        String expectedInputType,
        String expectedTruncation,
        String expectedUrl
    ) throws IOException {
        var httpRequest = request.createHttpRequest();
        var httpPost = validateRequestUrlAndContentType(httpRequest, expectedUrl);

        var requestMap = entityAsMap(httpPost.getEntity().getContent());
        int size = 3;
        assertThat(requestMap.get(INPUT_FIELD_NAME), is(List.of(INPUT_VALUE)));
        assertThat(requestMap.get(MODEL_FIELD_NAME), is(MODEL_VALUE));
        assertThat(requestMap.get(INPUT_TYPE_FIELD_NAME), is(expectedInputType));
        if (expectedTruncation != null) {
            size++;
            assertThat(requestMap.get(TRUNCATE_FIELD_NAME), is(expectedTruncation));
        }
        assertThat(requestMap, aMapWithSize(size));
        assertThat(httpPost.getFirstHeader(HttpHeaders.AUTHORIZATION).getValue(), is(Strings.format("Bearer %s", API_KEY_VALUE)));
    }

    public void testTruncate_ReducesInputTextSizeByHalf() throws IOException {
        var request = createRequest(URL_VALUE, null, null, null);
        var truncatedRequest = request.truncate();

        var httpRequest = truncatedRequest.createHttpRequest();
        assertThat(httpRequest.httpRequestBase(), instanceOf(HttpPost.class));

        var httpPost = (HttpPost) httpRequest.httpRequestBase();
        var requestMap = entityAsMap(httpPost.getEntity().getContent());
        assertThat(requestMap, aMapWithSize(3));
        assertThat(requestMap.get(INPUT_FIELD_NAME), is(List.of(INPUT_VALUE.substring(0, INPUT_VALUE.length() / 2))));
        assertThat(requestMap.get(MODEL_FIELD_NAME), is(MODEL_VALUE));
        assertThat(requestMap.get(INPUT_TYPE_FIELD_NAME), is(INPUT_TYPE_NVIDIA_DEFAULT_VALUE));
    }

    public void testGetTruncationInfo() {
        var request = createRequest(URL_VALUE, null, null, null);
        assertThat(request.getTruncationInfo()[0], is(false));

        var truncatedRequest = request.truncate();
        assertThat(truncatedRequest.getTruncationInfo()[0], is(true));
    }

    private HttpPost validateRequestUrlAndContentType(HttpRequest request, String expectedUrl) {
        assertThat(request.httpRequestBase(), instanceOf(HttpPost.class));
        var httpPost = (HttpPost) request.httpRequestBase();
        assertThat(httpPost.getURI().toString(), is(expectedUrl));
        assertThat(httpPost.getLastHeader(HttpHeaders.CONTENT_TYPE).getValue(), is(XContentType.JSON.mediaTypeWithoutParameters()));
        return httpPost;
    }

    private static NvidiaEmbeddingsRequest createRequest(
        @Nullable String url,
        @Nullable InputType inputType,
        @Nullable Truncation truncation,
        @Nullable InputType requestInputType
    ) {
        var embeddingsModel = NvidiaEmbeddingsModelTests.createEmbeddingsModel(
            url,
            API_KEY_VALUE,
            MODEL_VALUE,
            null,
            null,
            inputType,
            truncation,
            null
        );
        return new NvidiaEmbeddingsRequest(
            TruncatorTests.createTruncator(),
            new Truncator.TruncationResult(List.of(INPUT_VALUE), new boolean[] { false }),
            embeddingsModel,
            requestInputType
        );
    }

}
