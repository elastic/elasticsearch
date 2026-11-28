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

import java.io.IOException;
import java.util.Map;

public class SageMakerOpenAiTaskSettingsTests extends InferenceSettingsTestCase<SageMakerOpenAiTaskSettings> {
    @Override
    protected SageMakerOpenAiTaskSettings fromMutableMap(Map<String, Object> mutableMap) {
        var validationException = new ValidationException();
        var settings = SageMakerOpenAiTaskSettings.fromMap(mutableMap, validationException);
        validationException.throwIfValidationErrorsExist();
        return settings;
    }

    @Override
    protected Writeable.Reader<SageMakerOpenAiTaskSettings> instanceReader() {
        return SageMakerOpenAiTaskSettings::new;
    }

    @Override
    protected SageMakerOpenAiTaskSettings createTestInstance() {
        return randomApiTaskSettings();
    }

    @Override
    protected SageMakerOpenAiTaskSettings mutateInstance(SageMakerOpenAiTaskSettings instance) throws IOException {
        String user = randomValueOtherThan(instance.user(), InferenceSettingsTestCase::randomOptionalString);
        return new SageMakerOpenAiTaskSettings(user);
    }

    static SageMakerOpenAiTaskSettings randomApiTaskSettings() {
        return new SageMakerOpenAiTaskSettings(randomOptionalString());
    }
}
