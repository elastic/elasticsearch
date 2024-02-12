/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.openai.embeddings;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.TaskSettings;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.inference.services.ServiceUtils;
import org.elasticsearch.xpack.inference.services.openai.OpenAiParseContext;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractOptionalString;

/**
 * Defines the task settings for the openai service.
 *
 * @param modelId the id of the model to use in the requests to openai
 * @param user an optional unique identifier representing the end-user, which can help OpenAI to monitor and detect abuse
 *             <a href="https://platform.openai.com/docs/api-reference/embeddings/create">see the openai docs for more details</a>
 */
public record OpenAiEmbeddingsTaskSettings(String modelId, @Nullable String user) implements TaskSettings {

    public static final String NAME = "openai_embeddings_task_settings";
    public static final String OLD_MODEL_ID_FIELD = "model";
    public static final String MODEL_ID = "model_id";
    public static final String USER = "user";
    private static final String MODEL_DEPRECATION_MESSAGE =
        "The openai [task_settings.model] field is deprecated. Please use [task_settings.model_id] instead.";
    private static final Logger logger = LogManager.getLogger(OpenAiEmbeddingsTaskSettings.class);

    public static OpenAiEmbeddingsTaskSettings fromMap(Map<String, Object> map, OpenAiParseContext context) {
        ValidationException validationException = new ValidationException();

        String oldModelId = extractOptionalString(map, OLD_MODEL_ID_FIELD, ModelConfigurations.TASK_SETTINGS, validationException);
        logOldModelDeprecation(oldModelId, context, logger);

        String modelId = extractOptionalString(map, MODEL_ID, ModelConfigurations.TASK_SETTINGS, validationException);
        String user = extractOptionalString(map, USER, ModelConfigurations.TASK_SETTINGS, validationException);

        var modelIdToUse = getModelId(oldModelId, modelId, validationException);

        if (validationException.validationErrors().isEmpty() == false) {
            throw validationException;
        }

        return new OpenAiEmbeddingsTaskSettings(modelIdToUse, user);
    }

    // default for testing
    static void logOldModelDeprecation(@Nullable String oldModelId, OpenAiParseContext context, Logger logger) {
        if (OpenAiParseContext.isRequestContext(context) && oldModelId != null) {
            logger.info(MODEL_DEPRECATION_MESSAGE);
        }
    }

    private static String getModelId(@Nullable String oldModelId, @Nullable String modelId, ValidationException validationException) {
        var modelIdToUse = modelId != null ? modelId : oldModelId;

        if (modelIdToUse == null) {
            validationException.addValidationError(ServiceUtils.missingSettingErrorMsg(MODEL_ID, ModelConfigurations.TASK_SETTINGS));
        }

        return modelIdToUse;
    }

    /**
     * Creates a new {@link OpenAiEmbeddingsTaskSettings} object by overriding the values in originalSettings with the ones
     * passed in via requestSettings if the fields are not null.
     * @param originalSettings the original task settings from the inference entity configuration from storage
     * @param requestSettings the task settings from the request
     * @return a new {@link OpenAiEmbeddingsTaskSettings}
     */
    public static OpenAiEmbeddingsTaskSettings of(
        OpenAiEmbeddingsTaskSettings originalSettings,
        OpenAiEmbeddingsRequestTaskSettings requestSettings
    ) {
        var modelToUse = requestSettings.modelId() == null ? originalSettings.modelId : requestSettings.modelId();
        var userToUse = requestSettings.user() == null ? originalSettings.user : requestSettings.user();

        return new OpenAiEmbeddingsTaskSettings(modelToUse, userToUse);
    }

    public OpenAiEmbeddingsTaskSettings {
        Objects.requireNonNull(modelId);
    }

    public OpenAiEmbeddingsTaskSettings(StreamInput in) throws IOException {
        this(in.readString(), in.readOptionalString());
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(MODEL_ID, modelId);
        if (user != null) {
            builder.field(USER, user);
        }
        builder.endObject();
        return builder;
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersions.V_8_12_0;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(modelId);
        out.writeOptionalString(user);
    }
}
