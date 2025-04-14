/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.sagemaker.model;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.TaskSettings;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.inference.services.sagemaker.schema.SageMakerStoredTaskSchema;

import java.io.IOException;
import java.util.Map;

import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractOptionalString;

record SageMakerTaskSettings(
    @Nullable String customAttributes,
    @Nullable String enableExplanations,
    @Nullable String inferenceIdForDataCapture,
    @Nullable String sessionId,
    @Nullable String targetVariant,
    SageMakerStoredTaskSchema extraTaskSettings
) implements TaskSettings {

    static final String NAME = "sage_maker_task_settings";
    private static final String CUSTOM_ATTRIBUTES = "custom_attributes";
    private static final String ENABLE_EXPLANATIONS = "enable_explanations";
    private static final String INFERENCE_ID = "inference_id";
    private static final String SESSION_ID = "session_id";
    private static final String TARGET_VARIANT = "target_variant";

    SageMakerTaskSettings(StreamInput in) throws IOException {
        this(
            in.readOptionalString(),
            in.readOptionalString(),
            in.readOptionalString(),
            in.readOptionalString(),
            in.readOptionalString(),
            in.readNamedWriteable(SageMakerStoredTaskSchema.class)
        );
    }

    @Override
    public boolean isEmpty() {
        return customAttributes == null
            && enableExplanations == null
            && inferenceIdForDataCapture == null
            && sessionId == null
            && targetVariant == null
            && SageMakerStoredTaskSchema.NO_OP.equals(extraTaskSettings);
    }

    @Override
    public SageMakerTaskSettings updatedTaskSettings(Map<String, Object> newSettings) {
        var validationException = new ValidationException();

        var updateTaskSettings = fromMap(newSettings, extraTaskSettings.update(newSettings, validationException), validationException);

        validationException.throwIfValidationErrorsExist();

        var updatedExtraTaskSettings = updateTaskSettings.extraTaskSettings().equals(SageMakerStoredTaskSchema.NO_OP)
            ? extraTaskSettings
            : updateTaskSettings.extraTaskSettings();

        return new SageMakerTaskSettings(
            firstNotNullOrNull(updateTaskSettings.customAttributes(), customAttributes),
            firstNotNullOrNull(updateTaskSettings.enableExplanations(), enableExplanations),
            firstNotNullOrNull(updateTaskSettings.inferenceIdForDataCapture(), inferenceIdForDataCapture),
            firstNotNullOrNull(updateTaskSettings.sessionId(), sessionId),
            firstNotNullOrNull(updateTaskSettings.targetVariant(), targetVariant),
            updatedExtraTaskSettings
        );
    }

    private static <T> T firstNotNullOrNull(T first, T second) {
        return first != null ? first : second;
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersion.current();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalString(customAttributes);
        out.writeOptionalString(enableExplanations);
        out.writeOptionalString(inferenceIdForDataCapture);
        out.writeOptionalString(sessionId);
        out.writeOptionalString(targetVariant);
        out.writeNamedWriteable(extraTaskSettings);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        optionalField(CUSTOM_ATTRIBUTES, customAttributes, builder);
        optionalField(ENABLE_EXPLANATIONS, enableExplanations, builder);
        optionalField(INFERENCE_ID, inferenceIdForDataCapture, builder);
        optionalField(SESSION_ID, sessionId, builder);
        optionalField(TARGET_VARIANT, targetVariant, builder);
        extraTaskSettings.toXContent(builder, params);

        return builder.endObject();
    }

    private static <T> void optionalField(String name, T value, XContentBuilder builder) throws IOException {
        if (value != null) {
            builder.field(name, value);
        }
    }

    public static SageMakerTaskSettings fromMap(
        Map<String, Object> taskSettingsMap,
        SageMakerStoredTaskSchema extraTaskSettings,
        ValidationException validationException
    ) {
        var customAttributes = extractOptionalString(
            taskSettingsMap,
            CUSTOM_ATTRIBUTES,
            ModelConfigurations.TASK_SETTINGS,
            validationException
        );
        var enableExplanations = extractOptionalString(
            taskSettingsMap,
            ENABLE_EXPLANATIONS,
            ModelConfigurations.TASK_SETTINGS,
            validationException
        );
        var inferenceIdForDataCapture = extractOptionalString(
            taskSettingsMap,
            INFERENCE_ID,
            ModelConfigurations.TASK_SETTINGS,
            validationException
        );
        var sessionId = extractOptionalString(taskSettingsMap, SESSION_ID, ModelConfigurations.TASK_SETTINGS, validationException);
        var targetVariant = extractOptionalString(taskSettingsMap, TARGET_VARIANT, ModelConfigurations.TASK_SETTINGS, validationException);

        return new SageMakerTaskSettings(
            customAttributes,
            enableExplanations,
            inferenceIdForDataCapture,
            sessionId,
            targetVariant,
            extraTaskSettings
        );
    }
}
