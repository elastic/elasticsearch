/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.openai.embeddings;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.ModelConfigurations;

import java.util.Map;

import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractOptionalString;
import static org.elasticsearch.xpack.inference.services.openai.embeddings.OpenAiEmbeddingsTaskSettings.MODEL_ID;
import static org.elasticsearch.xpack.inference.services.openai.embeddings.OpenAiEmbeddingsTaskSettings.OLD_MODEL_ID_FIELD;
import static org.elasticsearch.xpack.inference.services.openai.embeddings.OpenAiEmbeddingsTaskSettings.USER;

/**
 * This class handles extracting OpenAI task settings from a request. The difference between this class and
 * {@link OpenAiEmbeddingsTaskSettings} is that this class considers all fields as optional. It will not throw an error if a field
 * is missing. This allows overriding persistent task settings.
 * @param modelId the name of the model to use with this request
 * @param user a unique identifier representing your end-user, which can help OpenAI to monitor and detect abuse
 */
public record OpenAiEmbeddingsRequestTaskSettings(@Nullable String modelId, @Nullable String user) {
    private static final Logger logger = LogManager.getLogger(OpenAiEmbeddingsRequestTaskSettings.class);

    public static final OpenAiEmbeddingsRequestTaskSettings EMPTY_SETTINGS = new OpenAiEmbeddingsRequestTaskSettings(null, null);

    /**
     * Extracts the task settings from a map. All settings are considered optional and the absence of a setting
     * does not throw an error.
     * @param map the settings received from a request
     * @return a {@link OpenAiEmbeddingsRequestTaskSettings}
     */
    public static OpenAiEmbeddingsRequestTaskSettings fromMap(Map<String, Object> map) {
        if (map.isEmpty()) {
            return OpenAiEmbeddingsRequestTaskSettings.EMPTY_SETTINGS;
        }

        ValidationException validationException = new ValidationException();

        // I'm intentionally not logging if this is set because it would log on every request
        String model = extractOptionalString(map, OLD_MODEL_ID_FIELD, ModelConfigurations.TASK_SETTINGS, validationException);

        String modelId = extractOptionalString(map, MODEL_ID, ModelConfigurations.TASK_SETTINGS, validationException);
        String user = extractOptionalString(map, USER, ModelConfigurations.TASK_SETTINGS, validationException);

        var modelIdToUse = getModelId(model, modelId);

        if (validationException.validationErrors().isEmpty() == false) {
            throw validationException;
        }

        return new OpenAiEmbeddingsRequestTaskSettings(modelIdToUse, user);
    }

    private static String getModelId(@Nullable String model, @Nullable String modelId) {
        return modelId != null ? modelId : model;
    }
}
