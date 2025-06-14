/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.sagemaker.schema.elastic;

import org.elasticsearch.common.ValidationException;
import org.elasticsearch.inference.InputType;
import org.elasticsearch.xpack.inference.services.sagemaker.SageMakerInferenceRequest;
import org.elasticsearch.xpack.inference.services.sagemaker.model.SageMakerModel;
import org.elasticsearch.xpack.inference.services.sagemaker.schema.SageMakerSchemaPayloadTestCase;
import org.elasticsearch.xpack.inference.services.sagemaker.schema.SageMakerStoredServiceSchema;
import org.elasticsearch.xpack.inference.services.sagemaker.schema.SageMakerStoredTaskSchema;

import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public abstract class ElasticPayloadTestCase<T extends ElasticPayload> extends SageMakerSchemaPayloadTestCase<T> {

    @Override
    protected String expectedApi() {
        return "elastic";
    }

    @Override
    protected SageMakerStoredServiceSchema randomApiServiceSettings() {
        return SageMakerStoredServiceSchema.NO_OP;
    }

    @Override
    protected SageMakerStoredTaskSchema randomApiTaskSettings() {
        return SageMakerElasticTaskSettingsTests.randomInstance();
    }

    protected SageMakerModel mockModel() {
        return mockModel(SageMakerElasticTaskSettings.empty());
    }

    protected SageMakerModel mockModel(SageMakerElasticTaskSettings taskSettings) {
        SageMakerModel model = mock();
        when(model.apiTaskSettings()).thenReturn(taskSettings);
        return model;
    }

    public void testApiTaskSettings() {
        {
            var validationException = new ValidationException();
            var actualApiTaskSettings = payload.apiTaskSettings(null, validationException);
            assertTrue(actualApiTaskSettings.isEmpty());
            assertTrue(validationException.validationErrors().isEmpty());
        }
        {
            var validationException = new ValidationException();
            var actualApiTaskSettings = payload.apiTaskSettings(Map.of(), validationException);
            assertTrue(actualApiTaskSettings.isEmpty());
            assertTrue(validationException.validationErrors().isEmpty());
        }
        {
            var validationException = new ValidationException();
            var actualApiTaskSettings = payload.apiTaskSettings(Map.of("hello", "world"), validationException);
            assertTrue(actualApiTaskSettings.isEmpty());
            assertFalse(validationException.validationErrors().isEmpty());
            assertThat(
                validationException.validationErrors().get(0),
                is(equalTo("task_settings is only supported during the inference request and cannot be stored in the inference endpoint."))
            );
        }
    }

    public void testRequestWithRequiredFields() throws Exception {
        var request = new SageMakerInferenceRequest(null, null, null, List.of("hello"), false, InputType.UNSPECIFIED);
        var sdkByes = payload.requestBytes(mockModel(), request);
        assertJsonSdkBytes(sdkByes, """
            {
                "input": "hello"
            }""");
    }

    public void testRequestWithInternalFields() throws Exception {
        var request = new SageMakerInferenceRequest(null, null, null, List.of("hello"), false, InputType.INTERNAL_SEARCH);
        var sdkByes = payload.requestBytes(mockModel(), request);
        assertJsonSdkBytes(sdkByes, """
            {
                "input": "hello",
                "input_type": "search"
            }""");
    }

    public void testRequestWithMultipleInput() throws Exception {
        var request = new SageMakerInferenceRequest(null, null, null, List.of("hello", "there"), false, InputType.UNSPECIFIED);
        var sdkByes = payload.requestBytes(mockModel(), request);
        assertJsonSdkBytes(sdkByes, """
            {
                "input": [
                    "hello",
                    "there"
                ]
            }""");
    }

    public void testRequestWithOptionalFields() throws Exception {
        var request = new SageMakerInferenceRequest("test", null, null, List.of("hello"), false, InputType.INGEST);
        var sdkByes = payload.requestBytes(mockModel(new SageMakerElasticTaskSettings(Map.of("more", "args"))), request);
        assertJsonSdkBytes(sdkByes, """
            {
                "input": "hello",
                "input_type": "ingest",
                "query": "test",
                "task_settings": {
                    "more": "args"
                }
            }""");
    }
}
