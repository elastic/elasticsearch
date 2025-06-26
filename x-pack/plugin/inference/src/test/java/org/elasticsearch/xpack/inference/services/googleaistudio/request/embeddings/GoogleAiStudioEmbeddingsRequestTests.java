/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.googleaistudio.request.embeddings;

import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.HttpPost;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Strings;
import org.elasticsearch.inference.InputType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.inference.InputTypeTests;
import org.elasticsearch.xpack.inference.common.Truncator;
import org.elasticsearch.xpack.inference.common.TruncatorTests;
import org.elasticsearch.xpack.inference.services.googleaistudio.embeddings.GoogleAiStudioEmbeddingsModelTests;
import org.elasticsearch.xpack.inference.services.googleaistudio.request.GoogleAiStudioEmbeddingsRequest;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.inference.external.http.Utils.entityAsMap;
import static org.elasticsearch.xpack.inference.services.googleaistudio.request.GoogleAiStudioEmbeddingsRequestEntity.convertToString;
import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

public class GoogleAiStudioEmbeddingsRequestTests extends ESTestCase {

    public void testCreateRequest_WithoutDimensionsSet() throws IOException {
        var model = "model";
        var apiKey = "api_key";
        var input = "input";
        var inputType = InputTypeTests.randomWithNull();

        var request = createRequest(model, apiKey, input, null, null, inputType);
        var httpRequest = request.createHttpRequest();

        assertThat(httpRequest.httpRequestBase(), instanceOf(HttpPost.class));
        var httpPost = (HttpPost) httpRequest.httpRequestBase();

        assertThat(httpPost.getURI().toString(), endsWith(Strings.format("%s=%s", "key", apiKey)));
        assertThat(httpPost.getLastHeader(HttpHeaders.CONTENT_TYPE).getValue(), is(XContentType.JSON.mediaType()));

        var requestMap = entityAsMap(httpPost.getEntity().getContent());
        assertThat(requestMap, aMapWithSize(1));

        if (InputType.isSpecified(inputType)) {
            var convertedInputType = convertToString(inputType);
            assertThat(
                requestMap.get("requests"),
                is(
                    List.of(
                        Map.of(
                            "model",
                            Strings.format("%s/%s", "models", model),
                            "content",
                            Map.of("parts", List.of(Map.of("text", input))),
                            "taskType",
                            convertedInputType
                        )
                    )
                )
            );
        } else {
            assertThat(
                requestMap.get("requests"),
                is(
                    List.of(
                        Map.of(
                            "model",
                            Strings.format("%s/%s", "models", model),
                            "content",
                            Map.of("parts", List.of(Map.of("text", input)))
                        )
                    )
                )
            );
        }

    }

    public void testCreateRequest_WithDimensionsSet() throws IOException {
        var model = "model";
        var apiKey = "api_key";
        var input = "input";
        var dimensions = 8;
        var inputType = InputTypeTests.randomWithNull();

        var request = createRequest(model, apiKey, input, null, dimensions, inputType);
        var httpRequest = request.createHttpRequest();

        assertThat(httpRequest.httpRequestBase(), instanceOf(HttpPost.class));
        var httpPost = (HttpPost) httpRequest.httpRequestBase();

        assertThat(httpPost.getURI().toString(), endsWith(Strings.format("%s=%s", "key", apiKey)));
        assertThat(httpPost.getLastHeader(HttpHeaders.CONTENT_TYPE).getValue(), is(XContentType.JSON.mediaType()));

        var requestMap = entityAsMap(httpPost.getEntity().getContent());
        assertThat(requestMap, aMapWithSize(1));
        if (InputType.isSpecified(inputType)) {
            var convertedInputType = convertToString(inputType);
            assertThat(
                requestMap.get("requests"),
                is(
                    List.of(
                        Map.of(
                            "model",
                            Strings.format("%s/%s", "models", model),
                            "content",
                            Map.of("parts", List.of(Map.of("text", input))),
                            "outputDimensionality",
                            dimensions,
                            "taskType",
                            convertedInputType
                        )
                    )
                )
            );
        } else {
            assertThat(
                requestMap.get("requests"),
                is(
                    List.of(
                        Map.of(
                            "model",
                            Strings.format("%s/%s", "models", model),
                            "content",
                            Map.of("parts", List.of(Map.of("text", input))),
                            "outputDimensionality",
                            dimensions
                        )
                    )
                )
            );
        }

    }

    public void testTruncate_ReducesInputTextSizeByHalf() throws IOException {
        var model = "model";
        var apiKey = "api_key";
        var input = "abcd";
        var dimensions = 8;
        var inputType = InputTypeTests.randomWithNull();

        var request = createRequest(model, apiKey, input, null, dimensions, inputType);
        var truncatedRequest = request.truncate();
        var httpRequest = truncatedRequest.createHttpRequest();

        assertThat(httpRequest.httpRequestBase(), instanceOf(HttpPost.class));
        var httpPost = (HttpPost) httpRequest.httpRequestBase();

        assertThat(httpPost.getURI().toString(), endsWith(Strings.format("%s=%s", "key", apiKey)));
        assertThat(httpPost.getLastHeader(HttpHeaders.CONTENT_TYPE).getValue(), is(XContentType.JSON.mediaType()));

        var requestMap = entityAsMap(httpPost.getEntity().getContent());
        assertThat(requestMap, aMapWithSize(1));

        if (InputType.isSpecified(inputType)) {
            var convertedInputType = convertToString(inputType);
            assertThat(
                requestMap.get("requests"),
                is(
                    List.of(
                        Map.of(
                            "model",
                            Strings.format("%s/%s", "models", model),
                            "content",
                            // "abcd" reduced by half -> "ab"
                            Map.of("parts", List.of(Map.of("text", "ab"))),
                            "outputDimensionality",
                            dimensions,
                            "taskType",
                            convertedInputType
                        )
                    )
                )
            );
        } else {
            assertThat(
                requestMap.get("requests"),
                is(
                    List.of(
                        Map.of(
                            "model",
                            Strings.format("%s/%s", "models", model),
                            "content",
                            // "abcd" reduced by half -> "ab"
                            Map.of("parts", List.of(Map.of("text", "ab"))),
                            "outputDimensionality",
                            dimensions
                        )
                    )
                )
            );
        }
    }

    public void testIsTruncated_ReturnsTrue() {
        var request = createRequest("model", "api key", "input", null, null, null);
        assertFalse(request.getTruncationInfo()[0]);

        var truncatedRequest = request.truncate();
        assertTrue(truncatedRequest.getTruncationInfo()[0]);
    }

    public static GoogleAiStudioEmbeddingsRequest createRequest(
        String model,
        String apiKey,
        String input,
        @Nullable Integer maxTokens,
        @Nullable Integer dimensions,
        @Nullable InputType inputType
    ) {
        var embeddingsModel = GoogleAiStudioEmbeddingsModelTests.createModel(model, apiKey, maxTokens, dimensions);

        return new GoogleAiStudioEmbeddingsRequest(
            TruncatorTests.createTruncator(),
            new Truncator.TruncationResult(List.of(input), new boolean[] { false }),
            inputType,
            embeddingsModel
        );
    }
}
