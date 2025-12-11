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

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.inference.external.http.Utils.entityAsMap;
import static org.elasticsearch.xpack.inference.services.jinaai.request.JinaAIEmbeddingsRequestEntity.convertInputType;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

public class JinaAIEmbeddingsRequestTests extends ESTestCase {
    public void testCreateRequest_AllOptionsDefined() throws IOException {
        var inputType = InputTypeTests.randomWithNull();
        boolean lateChunking = randomBoolean();
        var modelName = "modelName";
        var url = "url";
        var embeddingType = randomFrom(JinaAIEmbeddingType.values());
        var input = List.of("abc");
        var apiKey = "api-key";
        var dimensions = 512;
        var request = createRequest(
            input,
            inputType,
            JinaAIEmbeddingsModelTests.createModel(
                url,
                modelName,
                embeddingType,
                new JinaAIEmbeddingsTaskSettings(null, lateChunking),
                apiKey,
                dimensions
            )
        );

        var httpRequest = request.createHttpRequest();
        assertThat(httpRequest.httpRequestBase(), instanceOf(HttpPost.class));

        var httpPost = (HttpPost) httpRequest.httpRequestBase();

        assertThat(httpPost.getURI().toString(), is(url));
        assertThat(httpPost.getLastHeader(HttpHeaders.CONTENT_TYPE).getValue(), is(XContentType.JSON.mediaType()));
        assertThat(httpPost.getLastHeader(HttpHeaders.AUTHORIZATION).getValue(), is("Bearer " + apiKey));
        assertThat(httpPost.getLastHeader(JinaAIUtils.REQUEST_SOURCE_HEADER).getValue(), is(JinaAIUtils.ELASTIC_REQUEST_SOURCE));

        var requestMap = entityAsMap(httpPost.getEntity().getContent());
        if (InputType.isSpecified(inputType)) {
            var convertedInputType = convertInputType(inputType);
            assertThat(
                requestMap,
                is(
                    Map.of(
                        "input",
                        input,
                        "model",
                        modelName,
                        "embedding_type",
                        embeddingType.toRequestString(),
                        "task",
                        convertedInputType,
                        "late_chunking",
                        lateChunking,
                        "dimensions",
                        dimensions
                    )
                )
            );
        } else {
            assertThat(
                requestMap,
                is(
                    Map.of(
                        "input",
                        input,
                        "model",
                        modelName,
                        "embedding_type",
                        embeddingType.toRequestString(),
                        "late_chunking",
                        lateChunking,
                        "dimensions",
                        dimensions
                    )
                )
            );
        }
    }

    public void testCreateRequest_TaskSettingsInputType() throws IOException {
        var inputType = InputTypeTests.randomWithoutUnspecified();
        var modelName = "modelName";
        var url = "url";
        var embeddingType = randomFrom(JinaAIEmbeddingType.values());
        List<String> input = List.of("abc");
        String apiKey = "api-key";
        int dimensions = 512;
        var request = createRequest(
            input,
            null,
            JinaAIEmbeddingsModelTests.createModel(
                url,
                modelName,
                embeddingType,
                new JinaAIEmbeddingsTaskSettings(inputType, null),
                apiKey,
                dimensions
            )
        );

        var httpRequest = request.createHttpRequest();
        assertThat(httpRequest.httpRequestBase(), instanceOf(HttpPost.class));

        var httpPost = (HttpPost) httpRequest.httpRequestBase();

        assertThat(httpPost.getURI().toString(), is(url));
        assertThat(httpPost.getLastHeader(HttpHeaders.CONTENT_TYPE).getValue(), is(XContentType.JSON.mediaType()));
        assertThat(httpPost.getLastHeader(HttpHeaders.AUTHORIZATION).getValue(), is("Bearer " + apiKey));
        assertThat(httpPost.getLastHeader(JinaAIUtils.REQUEST_SOURCE_HEADER).getValue(), is(JinaAIUtils.ELASTIC_REQUEST_SOURCE));

        var requestMap = entityAsMap(httpPost.getEntity().getContent());
        if (InputType.isSpecified(inputType)) {
            var convertedInputType = convertInputType(inputType);
            assertThat(
                requestMap,
                is(
                    Map.of(
                        "input",
                        input,
                        "model",
                        modelName,
                        "embedding_type",
                        embeddingType.toRequestString(),
                        "task",
                        convertedInputType,
                        "dimensions",
                        dimensions
                    )
                )
            );
        } else {
            assertThat(
                requestMap,
                is(Map.of("input", input, "model", modelName, "embedding_type", embeddingType.toRequestString(), "dimensions", dimensions))
            );
        }
    }

    public void testCreateRequest_RequestInputTypeTakesPrecedence() throws IOException {
        var requestInputType = InputTypeTests.randomWithNull();
        var taskSettingsInputType = InputTypeTests.randomWithoutUnspecified();
        var modelName = "modelName";
        var url = "url";
        List<String> input = List.of("abc");
        String apiKey = "api-key";
        var request = createRequest(
            input,
            requestInputType,
            JinaAIEmbeddingsModelTests.createModel(url, modelName, new JinaAIEmbeddingsTaskSettings(taskSettingsInputType, null), apiKey)
        );

        var httpRequest = request.createHttpRequest();
        assertThat(httpRequest.httpRequestBase(), instanceOf(HttpPost.class));

        var httpPost = (HttpPost) httpRequest.httpRequestBase();

        assertThat(httpPost.getURI().toString(), is(url));
        assertThat(httpPost.getLastHeader(HttpHeaders.CONTENT_TYPE).getValue(), is(XContentType.JSON.mediaType()));
        assertThat(httpPost.getLastHeader(HttpHeaders.AUTHORIZATION).getValue(), is("Bearer " + apiKey));
        assertThat(httpPost.getLastHeader(JinaAIUtils.REQUEST_SOURCE_HEADER).getValue(), is(JinaAIUtils.ELASTIC_REQUEST_SOURCE));

        var requestMap = entityAsMap(httpPost.getEntity().getContent());
        if (InputType.isSpecified(requestInputType)) {
            var convertedInputType = convertInputType(requestInputType);
            assertThat(requestMap, is(Map.of("input", input, "model", modelName, "embedding_type", "float", "task", convertedInputType)));
        } else if (InputType.isSpecified(taskSettingsInputType)) {
            var convertedInputType = convertInputType(taskSettingsInputType);
            assertThat(requestMap, is(Map.of("input", input, "model", modelName, "embedding_type", "float", "task", convertedInputType)));
        } else {
            assertThat(requestMap, is(Map.of("input", input, "model", modelName, "embedding_type", "float")));
        }
    }

    public static JinaAIEmbeddingsRequest createRequest(List<String> input, InputType inputType, JinaAIEmbeddingsModel model) {
        return new JinaAIEmbeddingsRequest(input, inputType, model);
    }
}
