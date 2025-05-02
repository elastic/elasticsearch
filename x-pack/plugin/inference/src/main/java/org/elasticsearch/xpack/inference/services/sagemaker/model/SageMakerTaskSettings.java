/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.sagemaker.model;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.SettingsConfiguration;
import org.elasticsearch.inference.TaskSettings;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.inference.configuration.SettingsConfigurationFieldType;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.inference.services.sagemaker.schema.SageMakerStoredTaskSchema;

import java.io.IOException;
import java.util.EnumSet;
import java.util.Map;
import java.util.stream.Stream;

import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractOptionalString;

/**
 * Maintains mutable settings for SageMaker. Model-specific settings are stored in {@link SageMakerStoredTaskSchema}.
 */
record SageMakerTaskSettings(
    @Nullable String customAttributes,
    @Nullable String enableExplanations,
    @Nullable String inferenceIdForDataCapture,
    @Nullable String sessionId,
    @Nullable String targetVariant,
    SageMakerStoredTaskSchema apiTaskSettings
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
            && SageMakerStoredTaskSchema.NO_OP.equals(apiTaskSettings);
    }

    @Override
    public SageMakerTaskSettings updatedTaskSettings(Map<String, Object> newSettings) {
        var validationException = new ValidationException();

        var updateTaskSettings = fromMap(newSettings, apiTaskSettings.updatedTaskSettings(newSettings), validationException);

        validationException.throwIfValidationErrorsExist();

        var updatedExtraTaskSettings = updateTaskSettings.apiTaskSettings().equals(SageMakerStoredTaskSchema.NO_OP)
            ? apiTaskSettings
            : updateTaskSettings.apiTaskSettings();

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
        return TransportVersions.ML_INFERENCE_SAGEMAKER_8_19;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalString(customAttributes);
        out.writeOptionalString(enableExplanations);
        out.writeOptionalString(inferenceIdForDataCapture);
        out.writeOptionalString(sessionId);
        out.writeOptionalString(targetVariant);
        out.writeNamedWriteable(apiTaskSettings);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        optionalField(CUSTOM_ATTRIBUTES, customAttributes, builder);
        optionalField(ENABLE_EXPLANATIONS, enableExplanations, builder);
        optionalField(INFERENCE_ID, inferenceIdForDataCapture, builder);
        optionalField(SESSION_ID, sessionId, builder);
        optionalField(TARGET_VARIANT, targetVariant, builder);
        apiTaskSettings.toXContent(builder, params);

        return builder.endObject();
    }

    private static <T> void optionalField(String name, T value, XContentBuilder builder) throws IOException {
        if (value != null) {
            builder.field(name, value);
        }
    }

    public static SageMakerTaskSettings fromMap(
        Map<String, Object> taskSettingsMap,
        SageMakerStoredTaskSchema apiTaskSettings,
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
            apiTaskSettings
        );
    }

    static Stream<Map.Entry<String, SettingsConfiguration>> configuration(EnumSet<TaskType> supportedTaskTypes) {
        return Stream.of(
            Map.entry(
                CUSTOM_ATTRIBUTES,
                new SettingsConfiguration.Builder(supportedTaskTypes).setDescription(
                    "An opaque informational value forwarded as-is to the model within SageMaker."
                )
                    .setLabel("Custom Attributes")
                    .setRequired(false)
                    .setSensitive(false)
                    .setUpdatable(false)
                    .setType(SettingsConfigurationFieldType.STRING)
                    .build()
            ),
            Map.entry(
                ENABLE_EXPLANATIONS,
                new SettingsConfiguration.Builder(supportedTaskTypes).setDescription(
                    "JMESPath expression overriding the ClarifyingExplainerConfig in the SageMaker Endpoint Configuration."
                )
                    .setLabel("Enable Explanations")
                    .setRequired(false)
                    .setSensitive(false)
                    .setUpdatable(false)
                    .setType(SettingsConfigurationFieldType.STRING)
                    .build()
            ),
            Map.entry(
                INFERENCE_ID,
                new SettingsConfiguration.Builder(supportedTaskTypes).setDescription(
                    "Informational identifying for auditing requests within the SageMaker Endpoint."
                )
                    .setLabel("Inference ID")
                    .setRequired(false)
                    .setSensitive(false)
                    .setUpdatable(false)
                    .setType(SettingsConfigurationFieldType.STRING)
                    .build()
            ),
            Map.entry(
                SESSION_ID,
                new SettingsConfiguration.Builder(supportedTaskTypes).setDescription(
                    "Creates or reuses an existing Session for SageMaker stateful models."
                )
                    .setLabel("Session ID")
                    .setRequired(false)
                    .setSensitive(false)
                    .setUpdatable(false)
                    .setType(SettingsConfigurationFieldType.STRING)
                    .build()
            ),
            Map.entry(
                TARGET_VARIANT,
                new SettingsConfiguration.Builder(supportedTaskTypes).setDescription(
                    "The production variant when calling the SageMaker Endpoint"
                )
                    .setLabel("Target Variant")
                    .setRequired(false)
                    .setSensitive(false)
                    .setUpdatable(false)
                    .setType(SettingsConfigurationFieldType.STRING)
                    .build()
            )
        );
    }
}
