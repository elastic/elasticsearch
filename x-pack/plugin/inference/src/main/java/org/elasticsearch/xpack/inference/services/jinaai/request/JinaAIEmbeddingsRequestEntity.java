/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.jinaai.request;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.InputType;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.inference.services.jinaai.embeddings.JinaAIEmbeddingType;
import org.elasticsearch.xpack.inference.services.jinaai.embeddings.JinaAIEmbeddingsTaskSettings;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.inference.InputType.INTERNAL_SEARCH;
import static org.elasticsearch.inference.InputType.SEARCH;
import static org.elasticsearch.inference.InputType.invalidInputTypeMessage;

public record JinaAIEmbeddingsRequestEntity(
    List<String> input,
    InputType inputType,
    JinaAIEmbeddingsTaskSettings taskSettings,
    @Nullable String model,
    @Nullable JinaAIEmbeddingType embeddingType,
    Integer dimensions,
    boolean dimensionsSetByUser
) implements ToXContentObject {

    private static final String SEARCH_DOCUMENT = "retrieval.passage";
    private static final String SEARCH_QUERY = "retrieval.query";
    private static final String CLUSTERING = "separation";
    private static final String CLASSIFICATION = "classification";
    private static final String INPUT_FIELD = "input";
    private static final String MODEL_FIELD = "model";
    public static final String TASK_TYPE_FIELD = "task";
    static final String EMBEDDING_TYPE_FIELD = "embedding_type";
    static final String DIMENSIONS_FIELD = "dimensions";
    public static final String JINA_CLIP_V_2_MODEL_NAME = "jina-clip-v2";

    public JinaAIEmbeddingsRequestEntity {
        Objects.requireNonNull(input);
        Objects.requireNonNull(taskSettings);
        Objects.requireNonNull(model);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(INPUT_FIELD, input);
        builder.field(MODEL_FIELD, model);

        if (embeddingType != null) {
            builder.field(EMBEDDING_TYPE_FIELD, embeddingType.toRequestString());
        }

        // Prefer the root level inputType over task settings input type.
        InputType inputTypeToUse = null;
        if (InputType.isSpecified(inputType)) {
            inputTypeToUse = inputType;
        } else {
            var taskSettingsInputType = taskSettings.getInputType();
            if (InputType.isSpecified(taskSettingsInputType)) {
                inputTypeToUse = taskSettingsInputType;
            }
        }

        // Do not specify the "task" field if the provided input type is null or not supported by the model
        if (shouldWriteInputType(model, inputTypeToUse)) {
            builder.field(TASK_TYPE_FIELD, convertToString(inputTypeToUse));
        }

        if (dimensionsSetByUser && dimensions != null) {
            builder.field(DIMENSIONS_FIELD, dimensions);
        }

        builder.endObject();
        return builder;
    }

    // default for testing
    public static String convertToString(InputType inputType) {
        return switch (inputType) {
            case INGEST, INTERNAL_INGEST -> SEARCH_DOCUMENT;
            case SEARCH, INTERNAL_SEARCH -> SEARCH_QUERY;
            case CLASSIFICATION -> CLASSIFICATION;
            case CLUSTERING -> CLUSTERING;
            default -> {
                assert false : invalidInputTypeMessage(inputType);
                yield null;
            }
        };
    }

    private static boolean shouldWriteInputType(String modelName, @Nullable InputType inputType) {
        if (inputType == null) {
            return false;
        }
        if (JINA_CLIP_V_2_MODEL_NAME.equalsIgnoreCase(modelName)) {
            // jina-clip-v2 only accepts "retrieval.query" for the "task" field
            return SEARCH.equals(inputType) || INTERNAL_SEARCH.equals(inputType);
        }
        return true;
    }
}
