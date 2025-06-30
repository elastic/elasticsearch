/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.mixedbread.embeddings;

import org.elasticsearch.common.ValidationException;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.ModelConfigurations;

import java.util.Map;

import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractOptionalString;
import static org.elasticsearch.xpack.inference.services.azureaistudio.AzureAiStudioConstants.USER_FIELD;

public record MixedbreadEmbeddingsRequestTaskSettings(@Nullable String user) {
    public static final MixedbreadEmbeddingsRequestTaskSettings EMPTY_SETTINGS = new MixedbreadEmbeddingsRequestTaskSettings(null);

    /**
     * Extracts the task settings from a map. All settings are considered optional and the absence of a setting
     * does not throw an error.
     *
     * @param map the settings received from a request
     * @return a {@link MixedbreadEmbeddingsRequestTaskSettings}
     */
    public static MixedbreadEmbeddingsRequestTaskSettings fromMap(Map<String, Object> map) {
        if (map.isEmpty()) {
            return MixedbreadEmbeddingsRequestTaskSettings.EMPTY_SETTINGS;
        }

        ValidationException validationException = new ValidationException();

        String user = extractOptionalString(map, USER_FIELD, ModelConfigurations.TASK_SETTINGS, validationException);

        if (validationException.validationErrors().isEmpty() == false) {
            throw validationException;
        }

        return new MixedbreadEmbeddingsRequestTaskSettings(user);
    }
}
