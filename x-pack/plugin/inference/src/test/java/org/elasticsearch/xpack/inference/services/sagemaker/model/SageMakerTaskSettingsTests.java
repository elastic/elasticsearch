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
        String customAttributes = instance.customAttributes();
        String enableExplanations = instance.enableExplanations();
        String inferenceIdForDataCapture = instance.inferenceIdForDataCapture();
        String sessionId = instance.sessionId();
        String targetVariant = instance.targetVariant();

        switch (between(0, 4)) {
            case 0 -> customAttributes = randomValueOtherThan(customAttributes, InferenceSettingsTestCase::randomOptionalString);
            case 1 -> enableExplanations = randomValueOtherThan(enableExplanations, InferenceSettingsTestCase::randomOptionalString);
            case 2 -> inferenceIdForDataCapture = randomValueOtherThan(
                inferenceIdForDataCapture,
                InferenceSettingsTestCase::randomOptionalString
            );
            case 3 -> sessionId = randomValueOtherThan(sessionId, InferenceSettingsTestCase::randomOptionalString);
            case 4 -> targetVariant = randomValueOtherThan(targetVariant, InferenceSettingsTestCase::randomOptionalString);
            default -> throw new AssertionError("Illegal randomisation branch");
        }

        return new SageMakerTaskSettings(
            customAttributes,
            enableExplanations,
            inferenceIdForDataCapture,
            sessionId,
            targetVariant,
            instance.apiTaskSettings()
        );
    }

    @Override
    protected SageMakerTaskSettings fromMutableMap(Map<String, Object> mutableMap) {
        var validationException = new ValidationException();
        var taskSettings = SageMakerTaskSettings.fromMap(mutableMap, SageMakerStoredTaskSchema.NO_OP, validationException);
        validationException.throwIfValidationErrorsExist();
        return taskSettings;
    }
}
