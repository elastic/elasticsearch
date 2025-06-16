/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.sagemaker.schema.openai;

import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.sagemakerruntime.model.InvokeEndpointResponse;

import org.elasticsearch.inference.InputType;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xpack.core.inference.results.TextEmbeddingFloatResults;
import org.elasticsearch.xpack.inference.services.sagemaker.SageMakerInferenceRequest;
import org.elasticsearch.xpack.inference.services.sagemaker.model.SageMakerModel;
import org.elasticsearch.xpack.inference.services.sagemaker.schema.SageMakerSchemaPayloadTestCase;
import org.elasticsearch.xpack.inference.services.sagemaker.schema.SageMakerStoredServiceSchema;
import org.elasticsearch.xpack.inference.services.sagemaker.schema.SageMakerStoredTaskSchema;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Set;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class OpenAiTextEmbeddingPayloadTests extends SageMakerSchemaPayloadTestCase<OpenAiTextEmbeddingPayload> {
    @Override
    protected OpenAiTextEmbeddingPayload payload() {
        return new OpenAiTextEmbeddingPayload();
    }

    @Override
    protected String expectedApi() {
        return "openai";
    }

    @Override
    protected Set<TaskType> expectedSupportedTaskTypes() {
        return Set.of(TaskType.TEXT_EMBEDDING);
    }

    @Override
    protected SageMakerStoredServiceSchema randomApiServiceSettings() {
        return SageMakerOpenAiServiceSettingsTests.randomApiServiceSettings();
    }

    @Override
    protected SageMakerStoredTaskSchema randomApiTaskSettings() {
        return SageMakerOpenAiTaskSettingsTests.randomApiTaskSettings();
    }

    public void testAccept() {
        assertThat(payload.accept(mock()), equalTo("application/json"));
    }

    public void testContentType() {
        assertThat(payload.contentType(mock()), equalTo("application/json"));
    }

    public void testRequestWithSingleInput() throws Exception {
        SageMakerModel model = mock();
        when(model.apiServiceSettings()).thenReturn(new OpenAiTextEmbeddingPayload.ApiServiceSettings(null, false));
        when(model.apiTaskSettings()).thenReturn(new SageMakerOpenAiTaskSettings((String) null));
        var request = new SageMakerInferenceRequest(null, null, null, List.of("hello"), randomBoolean(), randomFrom(InputType.values()));

        var sdkByes = payload.requestBytes(model, request);
        assertSdkBytes(sdkByes, """
            {"input":"hello"}""");
    }

    public void testRequestWithArrayInput() throws Exception {
        SageMakerModel model = mock();
        when(model.apiServiceSettings()).thenReturn(new OpenAiTextEmbeddingPayload.ApiServiceSettings(null, false));
        when(model.apiTaskSettings()).thenReturn(new SageMakerOpenAiTaskSettings((String) null));
        var request = new SageMakerInferenceRequest(
            null,
            null,
            null,
            List.of("hello", "there"),
            randomBoolean(),
            randomFrom(InputType.values())
        );

        var sdkByes = payload.requestBytes(model, request);
        assertSdkBytes(sdkByes, """
            {"input":["hello","there"]}""");
    }

    public void testRequestWithDimensionsNotSetByUserIgnoreDimensions() throws Exception {
        SageMakerModel model = mock();
        when(model.apiServiceSettings()).thenReturn(new OpenAiTextEmbeddingPayload.ApiServiceSettings(123, false));
        when(model.apiTaskSettings()).thenReturn(new SageMakerOpenAiTaskSettings((String) null));
        var request = new SageMakerInferenceRequest(
            null,
            null,
            null,
            List.of("hello", "there"),
            randomBoolean(),
            randomFrom(InputType.values())
        );

        var sdkByes = payload.requestBytes(model, request);
        assertSdkBytes(sdkByes, """
            {"input":["hello","there"]}""");
    }

    public void testRequestWithOptionals() throws Exception {
        SageMakerModel model = mock();
        when(model.apiServiceSettings()).thenReturn(new OpenAiTextEmbeddingPayload.ApiServiceSettings(1234, true));
        when(model.apiTaskSettings()).thenReturn(new SageMakerOpenAiTaskSettings("user"));
        var request = new SageMakerInferenceRequest("query", null, null, List.of("hello"), randomBoolean(), randomFrom(InputType.values()));

        var sdkByes = payload.requestBytes(model, request);
        assertSdkBytes(sdkByes, """
            {"query":"query","input":"hello","user":"user","dimensions":1234}""");
    }

    public void testResponse() throws Exception {
        String responseJson = """
            {
              "object": "list",
              "data": [
                  {
                      "object": "embedding",
                      "index": 0,
                      "embedding": [
                          0.014539449,
                          -0.015288644
                      ]
                  }
              ],
              "model": "text-embedding-ada-002-v2",
              "usage": {
                  "prompt_tokens": 8,
                  "total_tokens": 8
              }
            }
            """;
        var invokeEndpointResponse = InvokeEndpointResponse.builder()
            .body(SdkBytes.fromString(responseJson, StandardCharsets.UTF_8))
            .build();

        var textEmbeddingFloatResults = payload.responseBody(mock(), invokeEndpointResponse);

        assertThat(
            textEmbeddingFloatResults.embeddings(),
            is(List.of(new TextEmbeddingFloatResults.Embedding(new float[] { 0.014539449F, -0.015288644F })))
        );
    }
}
