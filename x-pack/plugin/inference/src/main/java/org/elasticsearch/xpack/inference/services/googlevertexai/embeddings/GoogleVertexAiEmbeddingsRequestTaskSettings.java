/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.googlevertexai.embeddings;

import org.elasticsearch.common.ValidationException;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.InputType;
import org.elasticsearch.inference.ModelConfigurations;

import java.util.Map;

import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractOptionalBoolean;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractOptionalEnum;
import static org.elasticsearch.xpack.inference.services.googlevertexai.GoogleVertexAiService.VALID_INPUT_TYPE_VALUES;
import static org.elasticsearch.xpack.inference.services.googlevertexai.embeddings.GoogleVertexAiEmbeddingsTaskSettings.INPUT_TYPE;

public record GoogleVertexAiEmbeddingsRequestTaskSettings(@Nullable Boolean autoTruncate, @Nullable InputType inputType) {

    public static final GoogleVertexAiEmbeddingsRequestTaskSettings EMPTY_SETTINGS = new GoogleVertexAiEmbeddingsRequestTaskSettings(
        null,
        null
    );

    public static GoogleVertexAiEmbeddingsRequestTaskSettings fromMap(Map<String, Object> map) {
        if (map == null || map.isEmpty()) {
            return EMPTY_SETTINGS;
        }

        ValidationException validationException = new ValidationException();

        InputType inputType = extractOptionalEnum(
            map,
            INPUT_TYPE,
            ModelConfigurations.TASK_SETTINGS,
            InputType::fromString,
            VALID_INPUT_TYPE_VALUES,
            validationException
        );

        Boolean autoTruncate = extractOptionalBoolean(map, GoogleVertexAiEmbeddingsTaskSettings.AUTO_TRUNCATE, validationException);

        if (validationException.validationErrors().isEmpty() == false) {
            throw validationException;
        }

        return new GoogleVertexAiEmbeddingsRequestTaskSettings(autoTruncate, inputType);
    }

}
