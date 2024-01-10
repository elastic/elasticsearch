/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.cohere;

import org.elasticsearch.common.ValidationException;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.ModelConfigurations;

import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractOptionalListOfType;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractOptionalString;
import static org.elasticsearch.xpack.inference.services.cohere.CohereServiceFields.MODEL;

/**
 * Defines the task settings for the cohere text embeddings service.
 *
 * @param model the id of the model to use in the requests to cohere
 * @param inputType Specifies the type of input you're giving to the model
 * @param embeddingTypes Specifies the types of embeddings you want to get back
 * @param truncate Specifies how the API will handle inputs longer than the maximum token length
 */
public record CohereEmbeddingsTaskSettings(
    @Nullable String model,
    @Nullable String inputType,
    @Nullable List<String> embeddingTypes,
    @Nullable CohereTruncation truncate
) {

    public static final String NAME = "cohere_embeddings_task_settings";
    static final String INPUT_TYPE = "input_type";
    static final String EMBEDDING_TYPES = "embedding_types";

    public static CohereEmbeddingsTaskSettings fromMap(Map<String, Object> map) {
        ValidationException validationException = new ValidationException();

        String model = extractOptionalString(map, MODEL, ModelConfigurations.TASK_SETTINGS, validationException);
        String inputType = extractOptionalString(map, INPUT_TYPE, ModelConfigurations.TASK_SETTINGS, validationException);
        List<String> embeddingTypes = extractOptionalListOfType(
            map,
            EMBEDDING_TYPES,
            ModelConfigurations.TASK_SETTINGS,
            String.class,
            validationException
        );
        CohereTruncation truncation = extractOptionalString(map, INPUT_TYPE, ModelConfigurations.TASK_SETTINGS, validationException);

        if (validationException.validationErrors().isEmpty() == false) {
            throw validationException;
        }

        return new CohereEmbeddingsTaskSettings(model, user);
    }
}
