/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.jinaai.request;

import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.HttpPost;
import org.elasticsearch.inference.InferenceString;
import org.elasticsearch.inference.InferenceStringGroup;
import org.elasticsearch.inference.InputType;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.inference.InputTypeTests;
import org.elasticsearch.xpack.inference.services.jinaai.embeddings.JinaAIEmbeddingType;
import org.elasticsearch.xpack.inference.services.jinaai.embeddings.JinaAIEmbeddingsModel;
import org.elasticsearch.xpack.inference.services.jinaai.embeddings.JinaAIEmbeddingsModelTests;
import org.elasticsearch.xpack.inference.services.jinaai.embeddings.JinaAIEmbeddingsTaskSettings;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.inference.external.http.Utils.entityAsMap;
import static org.elasticsearch.xpack.inference.services.jinaai.request.JinaAIEmbeddingsRequestEntity.convertInputType;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

public class JinaAIEmbeddingsRequestTests extends ESTestCase {
    public void testCreateRequest_AllOptionsDefined_textEmbedding() throws IOException {
        var inputType = InputTypeTests.randomWithNull();
        boolean lateChunking = randomBoolean();
        var modelName = "modelName";
        var url = "url";
        var embeddingType = randomFrom(JinaAIEmbeddingType.values());
        var input = List.of("abc");
        var apiKey = "api-key";
        var dimensions = 512;
        var request = createTextOnlyRequest(
            input,
            inputType,
            JinaAIEmbeddingsModelTests.createModel(
                url,
                modelName,
                embeddingType,
                new JinaAIEmbeddingsTaskSettings(null, lateChunking),
                apiKey,
                dimensions,
                TaskType.TEXT_EMBEDDING,
                false
            )
        );

        var httpRequest = request.createHttpRequest();
        assertThat(httpRequest.httpRequestBase(), instanceOf(HttpPost.class));

        var httpPost = (HttpPost) httpRequest.httpRequestBase();

        assertThat(httpPost.getURI().toString(), is(url));
        assertThat(httpPost.getLastHeader(HttpHeaders.CONTENT_TYPE).getValue(), is(XContentType.JSON.mediaType()));
        assertThat(httpPost.getLastHeader(HttpHeaders.AUTHORIZATION).getValue(), is("Bearer " + apiKey));
        assertThat(httpPost.getLastHeader(JinaAIUtils.REQUEST_SOURCE_HEADER).getValue(), is(JinaAIUtils.ELASTIC_REQUEST_SOURCE));

        var expectedRequestMap = new HashMap<>(
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
        );
        if (InputType.isSpecified(inputType)) {
            expectedRequestMap.put("task", convertInputType(inputType));
        }

        var requestMap = entityAsMap(httpPost.getEntity().getContent());
        assertThat(requestMap, is(expectedRequestMap));
    }

    public void testCreateRequest_AllOptionsDefined_multimodalEmbedding() throws IOException {
        var inputType = InputTypeTests.randomWithNull();
        boolean lateChunking = randomBoolean();
        var modelName = "modelName";
        var url = "url";
        var embeddingType = randomFrom(JinaAIEmbeddingType.values());
        var input = List.of("abc");
        var apiKey = "api-key";
        var dimensions = 512;
        var request = createMultimodalRequest(
            input,
            inputType,
            JinaAIEmbeddingsModelTests.createModel(
                url,
                modelName,
                embeddingType,
                new JinaAIEmbeddingsTaskSettings(null, lateChunking),
                apiKey,
                dimensions,
                TaskType.EMBEDDING,
                true
            )
        );

        var httpRequest = request.createHttpRequest();
        assertThat(httpRequest.httpRequestBase(), instanceOf(HttpPost.class));

        var httpPost = (HttpPost) httpRequest.httpRequestBase();

        assertThat(httpPost.getURI().toString(), is(url));
        assertThat(httpPost.getLastHeader(HttpHeaders.CONTENT_TYPE).getValue(), is(XContentType.JSON.mediaType()));
        assertThat(httpPost.getLastHeader(HttpHeaders.AUTHORIZATION).getValue(), is("Bearer " + apiKey));
        assertThat(httpPost.getLastHeader(JinaAIUtils.REQUEST_SOURCE_HEADER).getValue(), is(JinaAIUtils.ELASTIC_REQUEST_SOURCE));

        var expectedRequestMap = new HashMap<>(
            Map.of(
                "input",
                List.of(Map.of("image", input.getFirst())),
                "model",
                modelName,
                "embedding_type",
                embeddingType.toRequestString(),
                "late_chunking",
                false,
                "dimensions",
                dimensions
            )
        );
        if (InputType.isSpecified(inputType)) {
            expectedRequestMap.put("task", convertInputType(inputType));
        }

        var requestMap = entityAsMap(httpPost.getEntity().getContent());
        assertThat(requestMap, is(expectedRequestMap));
    }

    public void testCreateRequest_TaskSettingsInputType() throws IOException {
        var inputType = InputTypeTests.randomWithoutUnspecified();
        var modelName = "modelName";
        var url = "url";
        var embeddingType = randomFrom(JinaAIEmbeddingType.values());
        List<String> input = List.of("abc");
        String apiKey = "api-key";
        int dimensions = 512;
        var request = createTextOnlyRequest(
            input,
            null,
            JinaAIEmbeddingsModelTests.createModel(
                url,
                modelName,
                embeddingType,
                new JinaAIEmbeddingsTaskSettings(inputType, null),
                apiKey,
                dimensions,
                TaskType.TEXT_EMBEDDING,
                false
            )
        );

        var httpRequest = request.createHttpRequest();
        assertThat(httpRequest.httpRequestBase(), instanceOf(HttpPost.class));

        var httpPost = (HttpPost) httpRequest.httpRequestBase();

        assertThat(httpPost.getURI().toString(), is(url));
        assertThat(httpPost.getLastHeader(HttpHeaders.CONTENT_TYPE).getValue(), is(XContentType.JSON.mediaType()));
        assertThat(httpPost.getLastHeader(HttpHeaders.AUTHORIZATION).getValue(), is("Bearer " + apiKey));
        assertThat(httpPost.getLastHeader(JinaAIUtils.REQUEST_SOURCE_HEADER).getValue(), is(JinaAIUtils.ELASTIC_REQUEST_SOURCE));

        var expectedRequestMap = new HashMap<>(
            Map.of("input", input, "model", modelName, "embedding_type", embeddingType.toRequestString(), "dimensions", dimensions)
        );
        if (InputType.isSpecified(inputType)) {
            expectedRequestMap.put("task", convertInputType(inputType));
        }

        var requestMap = entityAsMap(httpPost.getEntity().getContent());
        assertThat(requestMap, is(expectedRequestMap));
    }

    public void testCreateRequest_RequestInputTypeTakesPrecedence() throws IOException {
        var requestInputType = InputTypeTests.randomWithNull();
        var taskSettingsInputType = InputTypeTests.randomWithoutUnspecified();
        var modelName = "modelName";
        var url = "url";
        List<String> input = List.of("abc");
        String apiKey = "api-key";
        var request = createTextOnlyRequest(
            input,
            requestInputType,
            JinaAIEmbeddingsModelTests.createModel(
                url,
                modelName,
                new JinaAIEmbeddingsTaskSettings(taskSettingsInputType, null),
                apiKey,
                TaskType.TEXT_EMBEDDING
            )
        );

        var httpRequest = request.createHttpRequest();
        assertThat(httpRequest.httpRequestBase(), instanceOf(HttpPost.class));

        var httpPost = (HttpPost) httpRequest.httpRequestBase();

        assertThat(httpPost.getURI().toString(), is(url));
        assertThat(httpPost.getLastHeader(HttpHeaders.CONTENT_TYPE).getValue(), is(XContentType.JSON.mediaType()));
        assertThat(httpPost.getLastHeader(HttpHeaders.AUTHORIZATION).getValue(), is("Bearer " + apiKey));
        assertThat(httpPost.getLastHeader(JinaAIUtils.REQUEST_SOURCE_HEADER).getValue(), is(JinaAIUtils.ELASTIC_REQUEST_SOURCE));

        var expectedRequestMap = new HashMap<>(Map.of("input", input, "model", modelName, "embedding_type", "float"));
        if (InputType.isSpecified(requestInputType)) {
            expectedRequestMap.put("task", convertInputType(requestInputType));
        } else if (InputType.isSpecified(taskSettingsInputType)) {
            expectedRequestMap.put("task", convertInputType(taskSettingsInputType));
        }

        var requestMap = entityAsMap(httpPost.getEntity().getContent());
        assertThat(requestMap, is(expectedRequestMap));
    }

    public static JinaAIEmbeddingsRequest createMultimodalRequest(List<String> inputs, InputType inputType, JinaAIEmbeddingsModel model) {
        boolean isTextInput = false;
        List<InferenceStringGroup> convertedInput = new ArrayList<>();
        for (String input : inputs) {
            InferenceString inferenceString;
            if (isTextInput) {
                inferenceString = new InferenceString(InferenceString.DataType.TEXT, InferenceString.DataFormat.TEXT, input);
            } else {
                inferenceString = new InferenceString(InferenceString.DataType.IMAGE, InferenceString.DataFormat.BASE64, input);
            }
            isTextInput = isTextInput == false;
            var inferenceStringGroup = new InferenceStringGroup(inferenceString);
            convertedInput.add(inferenceStringGroup);
        }
        return new JinaAIEmbeddingsRequest(convertedInput, inputType, model);
    }

    public static JinaAIEmbeddingsRequest createTextOnlyRequest(List<String> inputs, InputType inputType, JinaAIEmbeddingsModel model) {
        List<InferenceStringGroup> convertedInput = inputs.stream().map(InferenceStringGroup::new).toList();
        return new JinaAIEmbeddingsRequest(convertedInput, inputType, model);
    }
}
