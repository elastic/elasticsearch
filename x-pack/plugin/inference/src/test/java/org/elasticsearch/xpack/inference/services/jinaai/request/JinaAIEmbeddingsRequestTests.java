/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.jinaai.request;

import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.HttpPost;
import org.elasticsearch.inference.InputType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.inference.InputTypeTests;
import org.elasticsearch.xpack.inference.services.jinaai.embeddings.JinaAIEmbeddingType;
import org.elasticsearch.xpack.inference.services.jinaai.embeddings.JinaAIEmbeddingsModel;
import org.elasticsearch.xpack.inference.services.jinaai.embeddings.JinaAIEmbeddingsModelTests;
import org.elasticsearch.xpack.inference.services.jinaai.embeddings.JinaAIEmbeddingsTaskSettings;
import org.hamcrest.MatcherAssert;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.inference.external.http.Utils.entityAsMap;
import static org.elasticsearch.xpack.inference.services.jinaai.request.JinaAIEmbeddingsRequestEntity.convertToString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

public class JinaAIEmbeddingsRequestTests extends ESTestCase {
    public void testCreateRequest_UrlDefined() throws IOException {
        var inputType = InputTypeTests.randomWithNull();
        var request = createRequest(
            List.of("abc"),
            inputType,
            JinaAIEmbeddingsModelTests.createModel(
                "url",
                "secret",
                JinaAIEmbeddingsTaskSettings.EMPTY_SETTINGS,
                null,
                null,
                "model",
                JinaAIEmbeddingType.FLOAT
            )
        );

        var httpRequest = request.createHttpRequest();
        MatcherAssert.assertThat(httpRequest.httpRequestBase(), instanceOf(HttpPost.class));

        var httpPost = (HttpPost) httpRequest.httpRequestBase();

        MatcherAssert.assertThat(httpPost.getURI().toString(), is("url"));
        MatcherAssert.assertThat(httpPost.getLastHeader(HttpHeaders.CONTENT_TYPE).getValue(), is(XContentType.JSON.mediaType()));
        MatcherAssert.assertThat(httpPost.getLastHeader(HttpHeaders.AUTHORIZATION).getValue(), is("Bearer secret"));
        MatcherAssert.assertThat(
            httpPost.getLastHeader(JinaAIUtils.REQUEST_SOURCE_HEADER).getValue(),
            is(JinaAIUtils.ELASTIC_REQUEST_SOURCE)
        );

        var requestMap = entityAsMap(httpPost.getEntity().getContent());
        if (InputType.isSpecified(inputType)) {
            var convertedInputType = convertToString(inputType);
            MatcherAssert.assertThat(
                requestMap,
                is(Map.of("input", List.of("abc"), "model", "model", "embedding_type", "float", "task", convertedInputType))
            );
        } else {
            MatcherAssert.assertThat(requestMap, is(Map.of("input", List.of("abc"), "model", "model", "embedding_type", "float")));
        }
    }

    public void testCreateRequest_AllOptionsDefined() throws IOException {
        var inputType = InputTypeTests.randomWithNull();
        var request = createRequest(
            List.of("abc"),
            inputType,
            JinaAIEmbeddingsModelTests.createModel(
                "url",
                "secret",
                JinaAIEmbeddingsTaskSettings.EMPTY_SETTINGS,
                null,
                null,
                "model",
                JinaAIEmbeddingType.FLOAT
            )
        );

        var httpRequest = request.createHttpRequest();
        MatcherAssert.assertThat(httpRequest.httpRequestBase(), instanceOf(HttpPost.class));

        var httpPost = (HttpPost) httpRequest.httpRequestBase();

        MatcherAssert.assertThat(httpPost.getURI().toString(), is("url"));
        MatcherAssert.assertThat(httpPost.getLastHeader(HttpHeaders.CONTENT_TYPE).getValue(), is(XContentType.JSON.mediaType()));
        MatcherAssert.assertThat(httpPost.getLastHeader(HttpHeaders.AUTHORIZATION).getValue(), is("Bearer secret"));
        MatcherAssert.assertThat(
            httpPost.getLastHeader(JinaAIUtils.REQUEST_SOURCE_HEADER).getValue(),
            is(JinaAIUtils.ELASTIC_REQUEST_SOURCE)
        );

        var requestMap = entityAsMap(httpPost.getEntity().getContent());
        if (InputType.isSpecified(inputType)) {
            var convertedInputType = convertToString(inputType);
            MatcherAssert.assertThat(
                requestMap,
                is(Map.of("input", List.of("abc"), "model", "model", "task", convertedInputType, "embedding_type", "float"))
            );
        } else {
            MatcherAssert.assertThat(requestMap, is(Map.of("input", List.of("abc"), "model", "model", "embedding_type", "float")));
        }
    }

    public void testCreateRequest_TaskSettingsInputType() throws IOException {
        var inputType = InputTypeTests.randomWithoutUnspecified();
        var request = createRequest(
            List.of("abc"),
            null,
            JinaAIEmbeddingsModelTests.createModel(
                "url",
                "secret",
                new JinaAIEmbeddingsTaskSettings(inputType),
                null,
                null,
                "model",
                JinaAIEmbeddingType.FLOAT
            )
        );

        var httpRequest = request.createHttpRequest();
        MatcherAssert.assertThat(httpRequest.httpRequestBase(), instanceOf(HttpPost.class));

        var httpPost = (HttpPost) httpRequest.httpRequestBase();

        MatcherAssert.assertThat(httpPost.getURI().toString(), is("url"));
        MatcherAssert.assertThat(httpPost.getLastHeader(HttpHeaders.CONTENT_TYPE).getValue(), is(XContentType.JSON.mediaType()));
        MatcherAssert.assertThat(httpPost.getLastHeader(HttpHeaders.AUTHORIZATION).getValue(), is("Bearer secret"));
        MatcherAssert.assertThat(
            httpPost.getLastHeader(JinaAIUtils.REQUEST_SOURCE_HEADER).getValue(),
            is(JinaAIUtils.ELASTIC_REQUEST_SOURCE)
        );

        var requestMap = entityAsMap(httpPost.getEntity().getContent());
        if (InputType.isSpecified(inputType)) {
            var convertedInputType = convertToString(inputType);
            MatcherAssert.assertThat(
                requestMap,
                is(Map.of("input", List.of("abc"), "model", "model", "task", convertedInputType, "embedding_type", "float"))
            );
        } else {
            MatcherAssert.assertThat(requestMap, is(Map.of("input", List.of("abc"), "model", "model", "embedding_type", "float")));
        }
    }

    public void testCreateRequest_RequestInputTypeTakesPrecedence() throws IOException {
        var requestInputType = InputTypeTests.randomWithNull();
        var taskSettingsInputType = InputTypeTests.randomWithoutUnspecified();
        var request = createRequest(
            List.of("abc"),
            requestInputType,
            JinaAIEmbeddingsModelTests.createModel(
                "url",
                "secret",
                new JinaAIEmbeddingsTaskSettings(taskSettingsInputType),
                null,
                null,
                "model",
                JinaAIEmbeddingType.FLOAT
            )
        );

        var httpRequest = request.createHttpRequest();
        MatcherAssert.assertThat(httpRequest.httpRequestBase(), instanceOf(HttpPost.class));

        var httpPost = (HttpPost) httpRequest.httpRequestBase();

        MatcherAssert.assertThat(httpPost.getURI().toString(), is("url"));
        MatcherAssert.assertThat(httpPost.getLastHeader(HttpHeaders.CONTENT_TYPE).getValue(), is(XContentType.JSON.mediaType()));
        MatcherAssert.assertThat(httpPost.getLastHeader(HttpHeaders.AUTHORIZATION).getValue(), is("Bearer secret"));
        MatcherAssert.assertThat(
            httpPost.getLastHeader(JinaAIUtils.REQUEST_SOURCE_HEADER).getValue(),
            is(JinaAIUtils.ELASTIC_REQUEST_SOURCE)
        );

        var requestMap = entityAsMap(httpPost.getEntity().getContent());
        if (InputType.isSpecified(requestInputType)) {
            var convertedInputType = convertToString(requestInputType);
            MatcherAssert.assertThat(
                requestMap,
                is(Map.of("input", List.of("abc"), "model", "model", "task", convertedInputType, "embedding_type", "float"))
            );
        } else if (InputType.isSpecified(taskSettingsInputType)) {
            var convertedInputType = convertToString(taskSettingsInputType);
            MatcherAssert.assertThat(
                requestMap,
                is(Map.of("input", List.of("abc"), "model", "model", "task", convertedInputType, "embedding_type", "float"))
            );
        } else {
            MatcherAssert.assertThat(requestMap, is(Map.of("input", List.of("abc"), "model", "model", "embedding_type", "float")));
        }
    }

    public void testCreateRequest_EmbeddingTypeBit() throws IOException {
        var inputType = InputTypeTests.randomWithNull();
        var request = createRequest(
            List.of("abc"),
            inputType,
            JinaAIEmbeddingsModelTests.createModel(
                "url",
                "secret",
                new JinaAIEmbeddingsTaskSettings(InputType.SEARCH),
                null,
                null,
                "model",
                JinaAIEmbeddingType.BIT
            )
        );

        var httpRequest = request.createHttpRequest();
        MatcherAssert.assertThat(httpRequest.httpRequestBase(), instanceOf(HttpPost.class));

        var httpPost = (HttpPost) httpRequest.httpRequestBase();

        MatcherAssert.assertThat(httpPost.getURI().toString(), is("url"));
        MatcherAssert.assertThat(httpPost.getLastHeader(HttpHeaders.CONTENT_TYPE).getValue(), is(XContentType.JSON.mediaType()));
        MatcherAssert.assertThat(httpPost.getLastHeader(HttpHeaders.AUTHORIZATION).getValue(), is("Bearer secret"));
        MatcherAssert.assertThat(
            httpPost.getLastHeader(JinaAIUtils.REQUEST_SOURCE_HEADER).getValue(),
            is(JinaAIUtils.ELASTIC_REQUEST_SOURCE)
        );

        var requestMap = entityAsMap(httpPost.getEntity().getContent());
        if (InputType.isSpecified(inputType)) {
            var convertedInputType = convertToString(inputType);
            MatcherAssert.assertThat(
                requestMap,
                is(Map.of("input", List.of("abc"), "model", "model", "task", convertedInputType, "embedding_type", "binary"))
            );
        } else {
            MatcherAssert.assertThat(
                requestMap,
                is(Map.of("input", List.of("abc"), "model", "model", "task", "retrieval.query", "embedding_type", "binary"))
            );
        }
    }

    public void testCreateRequest_EmbeddingTypeBinary() throws IOException {
        var inputType = InputTypeTests.randomWithNull();
        var request = createRequest(
            List.of("abc"),
            inputType,
            JinaAIEmbeddingsModelTests.createModel(
                "url",
                "secret",
                JinaAIEmbeddingsTaskSettings.EMPTY_SETTINGS,
                null,
                null,
                "model",
                JinaAIEmbeddingType.BINARY
            )
        );

        var httpRequest = request.createHttpRequest();
        MatcherAssert.assertThat(httpRequest.httpRequestBase(), instanceOf(HttpPost.class));

        var httpPost = (HttpPost) httpRequest.httpRequestBase();

        MatcherAssert.assertThat(httpPost.getURI().toString(), is("url"));
        MatcherAssert.assertThat(httpPost.getLastHeader(HttpHeaders.CONTENT_TYPE).getValue(), is(XContentType.JSON.mediaType()));
        MatcherAssert.assertThat(httpPost.getLastHeader(HttpHeaders.AUTHORIZATION).getValue(), is("Bearer secret"));
        MatcherAssert.assertThat(
            httpPost.getLastHeader(JinaAIUtils.REQUEST_SOURCE_HEADER).getValue(),
            is(JinaAIUtils.ELASTIC_REQUEST_SOURCE)
        );

        var requestMap = entityAsMap(httpPost.getEntity().getContent());
        if (InputType.isSpecified(inputType)) {
            var convertedInputType = convertToString(inputType);
            MatcherAssert.assertThat(
                requestMap,
                is(Map.of("input", List.of("abc"), "model", "model", "task", convertedInputType, "embedding_type", "binary"))
            );
        } else {
            MatcherAssert.assertThat(requestMap, is(Map.of("input", List.of("abc"), "model", "model", "embedding_type", "binary")));
        }
    }

    public static JinaAIEmbeddingsRequest createRequest(List<String> input, InputType inputType, JinaAIEmbeddingsModel model) {
        return new JinaAIEmbeddingsRequest(input, inputType, model);
    }
}
