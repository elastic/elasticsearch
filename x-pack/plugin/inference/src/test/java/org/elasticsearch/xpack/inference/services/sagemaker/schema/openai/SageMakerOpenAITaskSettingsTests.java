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

public class SageMakerOpenAITaskSettingsTests extends InferenceSettingsTestCase<OpenAiTextEmbeddingPayload.ExtraTaskSettings> {
    @Override
    protected OpenAiTextEmbeddingPayload.ExtraTaskSettings fromMutableMap(Map<String, Object> mutableMap) {
        var validationException = new ValidationException();
        var settings = new OpenAiTextEmbeddingPayload.ExtraTaskSettings.ExtraTaskSettingsBuilder().fromMap(mutableMap, validationException)
            .build();
        validationException.throwIfValidationErrorsExist();
        return settings;
    }

    @Override
    protected Writeable.Reader<OpenAiTextEmbeddingPayload.ExtraTaskSettings> instanceReader() {
        return OpenAiTextEmbeddingPayload.ExtraTaskSettings::new;
    }

    @Override
    protected OpenAiTextEmbeddingPayload.ExtraTaskSettings createTestInstance() {
        return new OpenAiTextEmbeddingPayload.ExtraTaskSettings(randomString());
    }
}
