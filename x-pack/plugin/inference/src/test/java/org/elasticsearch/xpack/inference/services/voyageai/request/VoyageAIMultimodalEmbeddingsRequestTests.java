/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.voyageai.request;

import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.HttpPost;
import org.elasticsearch.inference.InferenceStringGroup;
import org.elasticsearch.inference.InputType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.inference.InputTypeTests;
import org.elasticsearch.xpack.inference.services.voyageai.embeddings.multimodal.VoyageAIMultimodalEmbeddingsModel;
import org.elasticsearch.xpack.inference.services.voyageai.embeddings.multimodal.VoyageAIMultimodalEmbeddingsModelTests;
import org.elasticsearch.xpack.inference.services.voyageai.embeddings.multimodal.VoyageAIMultimodalEmbeddingsTaskSettings;
import org.hamcrest.MatcherAssert;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.inference.external.http.Utils.entityAsMap;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

public class VoyageAIMultimodalEmbeddingsRequestTests extends ESTestCase {

    private static final String DEFAULT_INPUT = "abc";
    private static final String DEFAULT_MODEL = "voyage-multimodal-3";

    /**
     * Helper method to create InferenceStringGroups from text strings.
     */
    private static List<InferenceStringGroup> toInferenceStringGroups(String... texts) {
        return java.util.Arrays.stream(texts).map(InferenceStringGroup::new).toList();
    }

    private static Map<String, Object> createExpectedRequestMap(String input, String model, InputType inputType) {
        var contentItem = Map.of("type", "text", "text", input);
        var contentList = List.of(contentItem);
        var inputItem = Map.of("content", contentList);

        var expectedMap = Map.of("inputs", List.of(inputItem), "model", model);
        if (InputType.isSpecified(inputType)) {
            var convertedInputType = VoyageAIMultimodalEmbeddingsRequestEntity.convertToString(inputType);
            if (convertedInputType != null) {
                expectedMap = Map.of("inputs", List.of(inputItem), "model", model, "input_type", convertedInputType);
            }
        }
        return expectedMap;
    }

    public void testCreateRequest_UrlDefined() throws IOException {
        var inputType = InputTypeTests.randomSearchAndIngestWithNullWithoutUnspecified();
        var request = createRequest(
            toInferenceStringGroups("abc"),
            inputType,
            VoyageAIMultimodalEmbeddingsModelTests.createModel(
                "url",
                "secret",
                VoyageAIMultimodalEmbeddingsTaskSettings.EMPTY_SETTINGS,
                null,
                null,
                "voyage-multimodal-3"
            )
        );

        var httpRequest = request.createHttpRequest();
        MatcherAssert.assertThat(httpRequest.httpRequestBase(), instanceOf(HttpPost.class));

        var httpPost = (HttpPost) httpRequest.httpRequestBase();

        MatcherAssert.assertThat(httpPost.getURI().toString(), is("url"));
        MatcherAssert.assertThat(httpPost.getLastHeader(HttpHeaders.CONTENT_TYPE).getValue(), is(XContentType.JSON.mediaType()));
        MatcherAssert.assertThat(httpPost.getLastHeader(HttpHeaders.AUTHORIZATION).getValue(), is("Bearer secret"));
        MatcherAssert.assertThat(
            httpPost.getLastHeader(VoyageAIUtils.REQUEST_SOURCE_HEADER).getValue(),
            is(VoyageAIUtils.ELASTIC_REQUEST_SOURCE)
        );

        var requestMap = entityAsMap(httpPost.getEntity().getContent());
        var expectedMap = createExpectedRequestMap("abc", "voyage-multimodal-3", inputType);
        MatcherAssert.assertThat(requestMap, is(expectedMap));
    }

    public void testCreateRequest_AllOptionsDefined() throws IOException {
        var inputType = InputTypeTests.randomSearchAndIngestWithNullWithoutUnspecified();
        var request = createRequest(
            toInferenceStringGroups("abc"),
            inputType,
            VoyageAIMultimodalEmbeddingsModelTests.createModel(
                "url",
                "secret",
                new VoyageAIMultimodalEmbeddingsTaskSettings(null, null),
                null,
                null,
                "voyage-multimodal-3"
            )
        );

        var httpRequest = request.createHttpRequest();
        MatcherAssert.assertThat(httpRequest.httpRequestBase(), instanceOf(HttpPost.class));

        var httpPost = (HttpPost) httpRequest.httpRequestBase();

        MatcherAssert.assertThat(httpPost.getURI().toString(), is("url"));
        MatcherAssert.assertThat(httpPost.getLastHeader(HttpHeaders.CONTENT_TYPE).getValue(), is(XContentType.JSON.mediaType()));
        MatcherAssert.assertThat(httpPost.getLastHeader(HttpHeaders.AUTHORIZATION).getValue(), is("Bearer secret"));
        MatcherAssert.assertThat(
            httpPost.getLastHeader(VoyageAIUtils.REQUEST_SOURCE_HEADER).getValue(),
            is(VoyageAIUtils.ELASTIC_REQUEST_SOURCE)
        );

        var requestMap = entityAsMap(httpPost.getEntity().getContent());
        var expectedMap = createExpectedRequestMap("abc", "voyage-multimodal-3", inputType);
        MatcherAssert.assertThat(requestMap, is(expectedMap));
    }

    public void testCreateRequest_TaskSettingsInputType() throws IOException {
        var inputType = InputTypeTests.randomSearchAndIngestWithNullWithoutUnspecified();
        var request = createRequest(
            toInferenceStringGroups("abc"),
            null,
            VoyageAIMultimodalEmbeddingsModelTests.createModel(
                "url",
                "secret",
                new VoyageAIMultimodalEmbeddingsTaskSettings(inputType, null),
                null,
                null,
                "voyage-multimodal-3"
            )
        );

        var httpRequest = request.createHttpRequest();
        MatcherAssert.assertThat(httpRequest.httpRequestBase(), instanceOf(HttpPost.class));

        var httpPost = (HttpPost) httpRequest.httpRequestBase();

        MatcherAssert.assertThat(httpPost.getURI().toString(), is("url"));
        MatcherAssert.assertThat(httpPost.getLastHeader(HttpHeaders.CONTENT_TYPE).getValue(), is(XContentType.JSON.mediaType()));
        MatcherAssert.assertThat(httpPost.getLastHeader(HttpHeaders.AUTHORIZATION).getValue(), is("Bearer secret"));
        MatcherAssert.assertThat(
            httpPost.getLastHeader(VoyageAIUtils.REQUEST_SOURCE_HEADER).getValue(),
            is(VoyageAIUtils.ELASTIC_REQUEST_SOURCE)
        );

        var requestMap = entityAsMap(httpPost.getEntity().getContent());
        var expectedMap = createExpectedRequestMap("abc", "voyage-multimodal-3", inputType);
        MatcherAssert.assertThat(requestMap, is(expectedMap));
    }

    public void testCreateRequest_RequestInputTypeTakesPrecedence() throws IOException {
        var requestInputType = InputTypeTests.randomSearchAndIngestWithNullWithoutUnspecified();
        var taskSettingsInputType = InputTypeTests.randomSearchAndIngestWithNullWithoutUnspecified();
        var request = createRequest(
            toInferenceStringGroups("abc"),
            requestInputType,
            VoyageAIMultimodalEmbeddingsModelTests.createModel(
                "url",
                "secret",
                new VoyageAIMultimodalEmbeddingsTaskSettings(taskSettingsInputType, null),
                null,
                null,
                "voyage-multimodal-3"
            )
        );

        var httpRequest = request.createHttpRequest();
        MatcherAssert.assertThat(httpRequest.httpRequestBase(), instanceOf(HttpPost.class));

        var httpPost = (HttpPost) httpRequest.httpRequestBase();

        MatcherAssert.assertThat(httpPost.getURI().toString(), is("url"));
        MatcherAssert.assertThat(httpPost.getLastHeader(HttpHeaders.CONTENT_TYPE).getValue(), is(XContentType.JSON.mediaType()));
        MatcherAssert.assertThat(httpPost.getLastHeader(HttpHeaders.AUTHORIZATION).getValue(), is("Bearer secret"));
        MatcherAssert.assertThat(
            httpPost.getLastHeader(VoyageAIUtils.REQUEST_SOURCE_HEADER).getValue(),
            is(VoyageAIUtils.ELASTIC_REQUEST_SOURCE)
        );

        var requestMap = entityAsMap(httpPost.getEntity().getContent());
        if (InputType.isSpecified(requestInputType)) {
            var convertedInputType = VoyageAIMultimodalEmbeddingsRequestEntity.convertToString(requestInputType);
            if (convertedInputType != null) {
                var expectedMap = Map.of(
                    "inputs",
                    List.of(Map.of("content", List.of(Map.of("type", "text", "text", DEFAULT_INPUT)))),
                    "model",
                    DEFAULT_MODEL,
                    "input_type",
                    convertedInputType
                );
                MatcherAssert.assertThat(requestMap, is(expectedMap));
            }
        } else if (InputType.isSpecified(taskSettingsInputType)) {
            var convertedInputType = VoyageAIMultimodalEmbeddingsRequestEntity.convertToString(taskSettingsInputType);
            if (convertedInputType != null) {
                var expectedMap = Map.of(
                    "inputs",
                    List.of(Map.of("content", List.of(Map.of("type", "text", "text", DEFAULT_INPUT)))),
                    "model",
                    DEFAULT_MODEL,
                    "input_type",
                    convertedInputType
                );
                MatcherAssert.assertThat(requestMap, is(expectedMap));
            }
        } else {
            var expectedMap = Map.of(
                "inputs",
                List.of(Map.of("content", List.of(Map.of("type", "text", "text", DEFAULT_INPUT)))),
                "model",
                DEFAULT_MODEL
            );
            MatcherAssert.assertThat(requestMap, is(expectedMap));
        }
    }

    public static VoyageAIMultimodalEmbeddingsRequest createRequest(
        List<InferenceStringGroup> input,
        InputType inputType,
        VoyageAIMultimodalEmbeddingsModel model
    ) {
        return new VoyageAIMultimodalEmbeddingsRequest(input, inputType, model);
    }
}
