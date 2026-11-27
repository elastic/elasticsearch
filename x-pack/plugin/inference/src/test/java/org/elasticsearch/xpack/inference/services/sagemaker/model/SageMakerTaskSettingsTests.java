/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.sagemaker.model;

import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xpack.inference.services.InferenceSettingsTestCase;
import org.elasticsearch.xpack.inference.services.sagemaker.schema.SageMakerStoredTaskSchema;
import org.elasticsearch.xpack.inference.services.sagemaker.schema.openai.SageMakerOpenAiTaskSettings;

import java.io.IOException;
import java.util.Map;

public class SageMakerTaskSettingsTests extends InferenceSettingsTestCase<SageMakerTaskSettings> {
    @Override
    protected Writeable.Reader<SageMakerTaskSettings> instanceReader() {
        return SageMakerTaskSettings::new;
    }

    @Override
    protected SageMakerTaskSettings createTestInstance() {
        return new SageMakerTaskSettings(
            randomOptionalString(),
            randomOptionalString(),
            randomOptionalString(),
            randomOptionalString(),
            randomOptionalString(),
            SageMakerStoredTaskSchema.NO_OP
        );
    }

    @Override
    protected SageMakerTaskSettings mutateInstance(SageMakerTaskSettings instance) throws IOException {
        return switch (between(0, 5)) {
            case 0 -> new SageMakerTaskSettings(
                randomValueOtherThan(instance.customAttributes(), InferenceSettingsTestCase::randomOptionalString),
                instance.enableExplanations(),
                instance.inferenceIdForDataCapture(),
                instance.sessionId(),
                instance.targetVariant(),
                instance.apiTaskSettings()
            );
            case 1 -> new SageMakerTaskSettings(
                instance.customAttributes(),
                randomValueOtherThan(instance.enableExplanations(), InferenceSettingsTestCase::randomOptionalString),
                instance.inferenceIdForDataCapture(),
                instance.sessionId(),
                instance.targetVariant(),
                instance.apiTaskSettings()
            );
            case 2 -> new SageMakerTaskSettings(
                instance.customAttributes(),
                instance.enableExplanations(),
                randomValueOtherThan(instance.inferenceIdForDataCapture(), InferenceSettingsTestCase::randomOptionalString),
                instance.sessionId(),
                instance.targetVariant(),
                instance.apiTaskSettings()
            );
            case 3 -> new SageMakerTaskSettings(
                instance.customAttributes(),
                instance.enableExplanations(),
                instance.inferenceIdForDataCapture(),
                randomValueOtherThan(instance.sessionId(), InferenceSettingsTestCase::randomOptionalString),
                instance.targetVariant(),
                instance.apiTaskSettings()
            );
            case 4 -> new SageMakerTaskSettings(
                instance.customAttributes(),
                instance.enableExplanations(),
                instance.inferenceIdForDataCapture(),
                instance.sessionId(),
                randomValueOtherThan(instance.targetVariant(), InferenceSettingsTestCase::randomOptionalString),
                instance.apiTaskSettings()
            );
            case 5 -> new SageMakerTaskSettings(
                instance.customAttributes(),
                instance.enableExplanations(),
                instance.inferenceIdForDataCapture(),
                instance.sessionId(),
                instance.targetVariant(),
                differentApiTaskSettings(instance.apiTaskSettings())
            );
            default -> throw new AssertionError("Illegal randomisation branch");
        };
    }

    /**
     * {@link SageMakerTaskSettings} is a record, so {@code apiTaskSettings} is an equals component.
     * A non-empty OpenAI task settings object is a different named writeable from {@code NO_OP}.
     */
    private static SageMakerStoredTaskSchema differentApiTaskSettings(SageMakerStoredTaskSchema current) {
        if (SageMakerStoredTaskSchema.NO_OP.equals(current)) {
            return new SageMakerOpenAiTaskSettings(randomAlphaOfLength(8));
        }
        return SageMakerStoredTaskSchema.NO_OP;
    }

    @Override
    protected SageMakerTaskSettings fromMutableMap(Map<String, Object> mutableMap) {
        var validationException = new ValidationException();
        var taskSettings = SageMakerTaskSettings.fromMap(mutableMap, SageMakerStoredTaskSchema.NO_OP, validationException);
        validationException.throwIfValidationErrorsExist();
        return taskSettings;
    }
}
