/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.sagemaker.schema.openai;

import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xpack.inference.services.InferenceSettingsTestCase;

import java.util.Map;

public class SageMakerOpenAIServiceSettingsTests extends InferenceSettingsTestCase<OpenAiTextEmbeddingPayload.ExtraServiceSettings> {
    @Override
    protected OpenAiTextEmbeddingPayload.ExtraServiceSettings fromMutableMap(Map<String, Object> mutableMap) {
        var validationException = new ValidationException();
        var settings = OpenAiTextEmbeddingPayload.ExtraServiceSettings.fromMap(mutableMap, validationException);
        validationException.throwIfValidationErrorsExist();
        return settings;
    }

    @Override
    protected Writeable.Reader<OpenAiTextEmbeddingPayload.ExtraServiceSettings> instanceReader() {
        return OpenAiTextEmbeddingPayload.ExtraServiceSettings::new;
    }

    @Override
    protected OpenAiTextEmbeddingPayload.ExtraServiceSettings createTestInstance() {
        return new OpenAiTextEmbeddingPayload.ExtraServiceSettings(randomIntBetween(1, 100), randomOptionalString());
    }
}
