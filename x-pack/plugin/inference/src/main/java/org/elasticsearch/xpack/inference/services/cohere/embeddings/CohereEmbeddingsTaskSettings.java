/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.cohere.embeddings;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.InputType;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.TaskSettings;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.inference.services.cohere.CohereTruncation;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractOptionalEnum;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractOptionalListOfEnums;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractOptionalString;
import static org.elasticsearch.xpack.inference.services.cohere.CohereServiceFields.MODEL;
import static org.elasticsearch.xpack.inference.services.cohere.CohereServiceFields.TRUNCATE;

/**
 * Defines the task settings for the cohere text embeddings service.
 *
 * <p>
 * <a href="https://docs.cohere.com/reference/embed">See api docs for details.</a>
 * </p>
 *
 * @param model the id of the model to use in the requests to cohere
 * @param inputType Specifies the type of input you're giving to the model
 * @param embeddingTypes Specifies the types of embeddings you want to get back
 * @param truncation Specifies how the API will handle inputs longer than the maximum token length
 */
public record CohereEmbeddingsTaskSettings(
    @Nullable String model,
    @Nullable InputType inputType,
    @Nullable List<CohereEmbeddingType> embeddingTypes,
    @Nullable CohereTruncation truncation
) implements TaskSettings {

    public static final String NAME = "cohere_embeddings_task_settings";
    static final CohereEmbeddingsTaskSettings EMPTY_SETTINGS = new CohereEmbeddingsTaskSettings(null, null, null, null);
    static final String INPUT_TYPE = "input_type";
    static final String EMBEDDING_TYPES = "embedding_types";

    public static CohereEmbeddingsTaskSettings fromMap(Map<String, Object> map) {
        if (map.isEmpty()) {
            return EMPTY_SETTINGS;
        }

        ValidationException validationException = new ValidationException();

        String model = extractOptionalString(map, MODEL, ModelConfigurations.TASK_SETTINGS, validationException);
        InputType inputType = extractOptionalEnum(
            map,
            INPUT_TYPE,
            ModelConfigurations.TASK_SETTINGS,
            InputType::fromString,
            InputType.values(),
            validationException
        );
        List<CohereEmbeddingType> embeddingTypes = extractOptionalListOfEnums(
            map,
            EMBEDDING_TYPES,
            ModelConfigurations.TASK_SETTINGS,
            CohereEmbeddingType::fromString,
            CohereEmbeddingType.values(),
            validationException
        );
        CohereTruncation truncation = extractOptionalEnum(
            map,
            TRUNCATE,
            ModelConfigurations.TASK_SETTINGS,
            CohereTruncation::fromString,
            CohereTruncation.values(),
            validationException
        );

        if (validationException.validationErrors().isEmpty() == false) {
            throw validationException;
        }

        return new CohereEmbeddingsTaskSettings(model, inputType, embeddingTypes, truncation);
    }

    public CohereEmbeddingsTaskSettings(StreamInput in) throws IOException {
        this(
            in.readOptionalString(),
            InputType.fromStream(in),
            in.readOptionalCollectionAsList(CohereEmbeddingType::fromStream),
            CohereTruncation.fromStream(in)
        );
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (model != null) {
            builder.field(MODEL, model);
        }

        if (inputType != null) {
            builder.field(INPUT_TYPE, inputType);
        }

        if (embeddingTypes != null) {
            builder.field(EMBEDDING_TYPES, embeddingTypes);
        }

        if (truncation != null) {
            builder.field(TRUNCATE, truncation);
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
        return TransportVersions.ML_INFERENCE_COHERE_EMBEDDINGS_ADDED;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalString(model);
        inputType.writeTo(out);
        out.writeOptionalCollection(embeddingTypes);
        truncation.writeTo(out);
    }

    public CohereEmbeddingsTaskSettings overrideWith(CohereEmbeddingsTaskSettings requestTaskSettings) {
        var modelToUse = requestTaskSettings.model() == null ? model : requestTaskSettings.model();
        var inputTypeToUse = requestTaskSettings.inputType() == null ? inputType : requestTaskSettings.inputType();
        var embeddingTypesToUse = requestTaskSettings.embeddingTypes() == null ? embeddingTypes : requestTaskSettings.embeddingTypes();
        var truncationToUse = requestTaskSettings.truncation() == null ? truncation : requestTaskSettings.truncation();

        return new CohereEmbeddingsTaskSettings(modelToUse, inputTypeToUse, embeddingTypesToUse, truncationToUse);
    }
}
