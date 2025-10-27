/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.voyageai.request;

import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.HttpPost;
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
import static org.elasticsearch.xpack.inference.services.voyageai.request.VoyageAIEmbeddingsRequestEntity.convertToString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

public class VoyageAIMultimodalEmbeddingsRequestTests extends ESTestCase {

    public void testCreateRequest_UrlDefined() throws IOException {
        var inputType = InputTypeTests.randomSearchAndIngestWithNullWithoutUnspecified();
        var request = createRequest(
            List.of("abc"),
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
        if (InputType.isSpecified(inputType)) {
            var convertedInputType = convertToString(inputType);
            // Note: multimodal uses "inputs" (plural) and does NOT include output_dtype
            MatcherAssert.assertThat(
                requestMap,
                is(Map.of("inputs", List.of("abc"), "model", "voyage-multimodal-3", "input_type", convertedInputType))
            );
        } else {
            MatcherAssert.assertThat(requestMap, is(Map.of("inputs", List.of("abc"), "model", "voyage-multimodal-3")));
        }
    }

    public void testCreateRequest_AllOptionsDefined() throws IOException {
        var inputType = InputTypeTests.randomSearchAndIngestWithNullWithoutUnspecified();
        var request = createRequest(
            List.of("abc"),
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
        if (InputType.isSpecified(inputType)) {
            var convertedInputType = convertToString(inputType);
            MatcherAssert.assertThat(
                requestMap,
                is(Map.of("inputs", List.of("abc"), "model", "voyage-multimodal-3", "input_type", convertedInputType))
            );
        } else {
            MatcherAssert.assertThat(requestMap, is(Map.of("inputs", List.of("abc"), "model", "voyage-multimodal-3")));
        }
    }

    public void testCreateRequest_TaskSettingsInputType() throws IOException {
        var inputType = InputTypeTests.randomSearchAndIngestWithNullWithoutUnspecified();
        var request = createRequest(
            List.of("abc"),
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
        if (InputType.isSpecified(inputType)) {
            var convertedInputType = convertToString(inputType);
            MatcherAssert.assertThat(
                requestMap,
                is(Map.of("inputs", List.of("abc"), "model", "voyage-multimodal-3", "input_type", convertedInputType))
            );
        } else {
            MatcherAssert.assertThat(requestMap, is(Map.of("inputs", List.of("abc"), "model", "voyage-multimodal-3")));
        }
    }

    public void testCreateRequest_RequestInputTypeTakesPrecedence() throws IOException {
        var requestInputType = InputTypeTests.randomSearchAndIngestWithNullWithoutUnspecified();
        var taskSettingsInputType = InputTypeTests.randomSearchAndIngestWithNullWithoutUnspecified();
        var request = createRequest(
            List.of("abc"),
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
            var convertedInputType = convertToString(requestInputType);
            MatcherAssert.assertThat(
                requestMap,
                is(Map.of("inputs", List.of("abc"), "model", "voyage-multimodal-3", "input_type", convertedInputType))
            );
        } else if (InputType.isSpecified(taskSettingsInputType)) {
            var convertedInputType = convertToString(taskSettingsInputType);
            MatcherAssert.assertThat(
                requestMap,
                is(Map.of("inputs", List.of("abc"), "model", "voyage-multimodal-3", "input_type", convertedInputType))
            );
        } else {
            MatcherAssert.assertThat(requestMap, is(Map.of("inputs", List.of("abc"), "model", "voyage-multimodal-3")));
        }
    }

    public static VoyageAIMultimodalEmbeddingsRequest createRequest(
        List<String> input,
        InputType inputType,
        VoyageAIMultimodalEmbeddingsModel model
    ) {
        return new VoyageAIMultimodalEmbeddingsRequest(input, inputType, model);
    }
}
