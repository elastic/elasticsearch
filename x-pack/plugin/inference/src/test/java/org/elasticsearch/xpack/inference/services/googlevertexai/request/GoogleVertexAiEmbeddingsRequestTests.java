/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.googlevertexai.request;

import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.HttpPost;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.InputType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.inference.InputTypeTests;
import org.elasticsearch.xpack.inference.common.Truncator;
import org.elasticsearch.xpack.inference.common.TruncatorTests;
import org.elasticsearch.xpack.inference.external.request.Request;
import org.elasticsearch.xpack.inference.services.googlevertexai.embeddings.GoogleVertexAiEmbeddingsModel;
import org.elasticsearch.xpack.inference.services.googlevertexai.embeddings.GoogleVertexAiEmbeddingsModelTests;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.inference.external.http.Utils.entityAsMap;
import static org.elasticsearch.xpack.inference.services.googlevertexai.request.GoogleVertexAiEmbeddingsRequestEntity.convertToString;
import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

public class GoogleVertexAiEmbeddingsRequestTests extends ESTestCase {

    private static final String AUTH_HEADER_VALUE = "foo";

    public void testCreateRequest_WithoutDimensionsSet_And_WithoutAutoTruncateSet_And_WithoutInputTypeSet() throws IOException {
        var model = "model";
        var input = "input";
        var inputType = InputTypeTests.randomWithNull();

        var request = createRequest(model, input, null, null, inputType);
        var httpRequest = request.createHttpRequest();

        assertThat(httpRequest.httpRequestBase(), instanceOf(HttpPost.class));
        var httpPost = (HttpPost) httpRequest.httpRequestBase();

        assertThat(httpPost.getLastHeader(HttpHeaders.CONTENT_TYPE).getValue(), is(XContentType.JSON.mediaType()));
        assertThat(httpPost.getLastHeader(HttpHeaders.AUTHORIZATION).getValue(), is(AUTH_HEADER_VALUE));

        var requestMap = entityAsMap(httpPost.getEntity().getContent());
        assertThat(requestMap, aMapWithSize(1));
        if (InputType.isSpecified(inputType)) {
            var convertedInputType = convertToString(inputType);
            assertThat(requestMap, is(Map.of("instances", List.of(Map.of("content", "input", "task_type", convertedInputType)))));
        } else {
            assertThat(requestMap, is(Map.of("instances", List.of(Map.of("content", "input")))));
        }
    }

    public void testCreateRequest_WithAutoTruncateSet() throws IOException {
        var model = "model";
        var input = "input";
        var autoTruncate = true;
        var inputType = InputTypeTests.randomWithNull();

        var request = createRequest(model, input, autoTruncate, null, inputType);
        var httpRequest = request.createHttpRequest();

        assertThat(httpRequest.httpRequestBase(), instanceOf(HttpPost.class));
        var httpPost = (HttpPost) httpRequest.httpRequestBase();

        assertThat(httpPost.getLastHeader(HttpHeaders.CONTENT_TYPE).getValue(), is(XContentType.JSON.mediaType()));
        assertThat(httpPost.getLastHeader(HttpHeaders.AUTHORIZATION).getValue(), is(AUTH_HEADER_VALUE));

        var requestMap = entityAsMap(httpPost.getEntity().getContent());
        assertThat(requestMap, aMapWithSize(2));
        if (InputType.isSpecified(inputType)) {
            var convertedInputType = convertToString(inputType);
            assertThat(
                requestMap,
                is(
                    Map.of(
                        "instances",
                        List.of(Map.of("content", "input", "task_type", convertedInputType)),
                        "parameters",
                        Map.of("autoTruncate", true)
                    )
                )
            );
        } else {
            assertThat(
                requestMap,
                is(Map.of("instances", List.of(Map.of("content", "input")), "parameters", Map.of("autoTruncate", true)))
            );
        }
    }

    public void testCreateRequest_WithTaskSettingsInputTypeSet() throws IOException {
        var model = "model";
        var input = "input";
        var inputType = InputTypeTests.randomWithoutUnspecified();

        var request = createRequest(model, input, null, inputType, null);
        var httpRequest = request.createHttpRequest();

        assertThat(httpRequest.httpRequestBase(), instanceOf(HttpPost.class));
        var httpPost = (HttpPost) httpRequest.httpRequestBase();

        assertThat(httpPost.getLastHeader(HttpHeaders.CONTENT_TYPE).getValue(), is(XContentType.JSON.mediaType()));
        assertThat(httpPost.getLastHeader(HttpHeaders.AUTHORIZATION).getValue(), is(AUTH_HEADER_VALUE));

        var requestMap = entityAsMap(httpPost.getEntity().getContent());
        assertThat(requestMap, aMapWithSize(1));
        if (InputType.isSpecified(inputType)) {
            var convertedInputType = convertToString(inputType);
            assertThat(requestMap, is(Map.of("instances", List.of(Map.of("content", "input", "task_type", convertedInputType)))));
        } else {
            assertThat(requestMap, is(Map.of("instances", List.of(Map.of("content", "input")))));
        }
    }

    public void testCreateRequest_RequestInputTypeTakesPrecedence() throws IOException {
        var model = "model";
        var input = "input";
        var requestInputType = InputTypeTests.randomWithNull();
        var taskSettingsInputType = InputTypeTests.randomWithoutUnspecified();

        var request = createRequest(model, input, null, taskSettingsInputType, requestInputType);
        var httpRequest = request.createHttpRequest();

        assertThat(httpRequest.httpRequestBase(), instanceOf(HttpPost.class));
        var httpPost = (HttpPost) httpRequest.httpRequestBase();

        assertThat(httpPost.getLastHeader(HttpHeaders.CONTENT_TYPE).getValue(), is(XContentType.JSON.mediaType()));
        assertThat(httpPost.getLastHeader(HttpHeaders.AUTHORIZATION).getValue(), is(AUTH_HEADER_VALUE));

        var requestMap = entityAsMap(httpPost.getEntity().getContent());
        assertThat(requestMap, aMapWithSize(1));
        if (InputType.isSpecified(requestInputType)) {
            var convertedInputType = convertToString(requestInputType);
            assertThat(requestMap, is(Map.of("instances", List.of(Map.of("content", "input", "task_type", convertedInputType)))));
        } else if (InputType.isSpecified(taskSettingsInputType)) {
            var convertedInputType = convertToString(taskSettingsInputType);
            assertThat(requestMap, is(Map.of("instances", List.of(Map.of("content", "input", "task_type", convertedInputType)))));
        } else {
            assertThat(requestMap, is(Map.of("instances", List.of(Map.of("content", "input")))));
        }
    }

    public void testTruncate_ReducesInputTextSizeByHalf() throws IOException {
        var model = "model";
        var input = "abcd";
        var inputType = InputTypeTests.randomWithNull();

        var request = createRequest(model, input, null, null, inputType);
        var truncatedRequest = request.truncate();
        var httpRequest = truncatedRequest.createHttpRequest();

        assertThat(httpRequest.httpRequestBase(), instanceOf(HttpPost.class));
        var httpPost = (HttpPost) httpRequest.httpRequestBase();

        assertThat(httpPost.getLastHeader(HttpHeaders.CONTENT_TYPE).getValue(), is(XContentType.JSON.mediaType()));
        assertThat(httpPost.getLastHeader(HttpHeaders.AUTHORIZATION).getValue(), is(AUTH_HEADER_VALUE));

        var requestMap = entityAsMap(httpPost.getEntity().getContent());
        assertThat(requestMap, aMapWithSize(1));

        if (InputType.isSpecified(inputType)) {
            var convertedInputType = convertToString(inputType);
            assertThat(requestMap, is(Map.of("instances", List.of(Map.of("content", "ab", "task_type", convertedInputType)))));
        } else {
            assertThat(requestMap, is(Map.of("instances", List.of(Map.of("content", "ab")))));
        }
    }

    private static GoogleVertexAiEmbeddingsRequest createRequest(
        String modelId,
        String input,
        @Nullable Boolean autoTruncate,
        @Nullable InputType taskSettingsInputType,
        @Nullable InputType requestInputType
    ) {
        var embeddingsModel = GoogleVertexAiEmbeddingsModelTests.createModel(modelId, autoTruncate, taskSettingsInputType);

        return new GoogleVertexAiEmbeddingsWithoutAuthRequest(
            TruncatorTests.createTruncator(),
            new Truncator.TruncationResult(List.of(input), new boolean[] { false }),
            requestInputType,
            embeddingsModel
        );
    }

    /**
     * We use this class to fake the auth implementation to avoid static mocking of {@link GoogleVertexAiRequest}
     */
    private static class GoogleVertexAiEmbeddingsWithoutAuthRequest extends GoogleVertexAiEmbeddingsRequest {

        private final InputType inputType;

        GoogleVertexAiEmbeddingsWithoutAuthRequest(
            Truncator truncator,
            Truncator.TruncationResult input,
            InputType inputType,
            GoogleVertexAiEmbeddingsModel model
        ) {
            super(truncator, input, inputType, model);
            this.inputType = inputType;
        }

        @Override
        public void decorateWithAuth(HttpPost httpPost) {
            httpPost.setHeader(HttpHeaders.AUTHORIZATION, AUTH_HEADER_VALUE);
        }

        @Override
        public Request truncate() {
            GoogleVertexAiEmbeddingsRequest embeddingsRequest = (GoogleVertexAiEmbeddingsRequest) super.truncate();
            return new GoogleVertexAiEmbeddingsWithoutAuthRequest(
                embeddingsRequest.truncator(),
                embeddingsRequest.truncationResult(),
                inputType,
                embeddingsRequest.model()
            );
        }
    }

}
