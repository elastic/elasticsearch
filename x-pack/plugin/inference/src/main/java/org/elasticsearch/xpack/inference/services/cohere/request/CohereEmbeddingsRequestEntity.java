/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.cohere.request;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.InputType;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.inference.services.cohere.CohereServiceFields;
import org.elasticsearch.xpack.inference.services.cohere.CohereServiceSettings;
import org.elasticsearch.xpack.inference.services.cohere.embeddings.CohereEmbeddingType;
import org.elasticsearch.xpack.inference.services.cohere.embeddings.CohereEmbeddingsTaskSettings;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.inference.InputType.invalidInputTypeMessage;

public record CohereEmbeddingsRequestEntity(
    List<String> input,
    InputType inputType,
    CohereEmbeddingsTaskSettings taskSettings,
    @Nullable String model,
    @Nullable CohereEmbeddingType embeddingType
) implements ToXContentObject {

    private static final String SEARCH_DOCUMENT = "search_document";
    private static final String SEARCH_QUERY = "search_query";
    private static final String CLUSTERING = "clustering";
    private static final String CLASSIFICATION = "classification";
    private static final String TEXTS_FIELD = "texts";
    public static final String INPUT_TYPE_FIELD = "input_type";
    static final String EMBEDDING_TYPES_FIELD = "embedding_types";

    public CohereEmbeddingsRequestEntity {
        Objects.requireNonNull(input);
        Objects.requireNonNull(taskSettings);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(TEXTS_FIELD, input);
        if (model != null) {
            builder.field(CohereServiceSettings.OLD_MODEL_ID_FIELD, model);
        }

        // prefer the root level inputType over task settings input type
        if (InputType.isSpecified(inputType)) {
            builder.field(INPUT_TYPE_FIELD, convertToString(inputType));
        } else if (InputType.isSpecified(taskSettings.getInputType())) {
            builder.field(INPUT_TYPE_FIELD, convertToString(taskSettings.getInputType()));
        }

        if (embeddingType != null) {
            builder.field(EMBEDDING_TYPES_FIELD, List.of(embeddingType.toRequestString()));
        }

        if (taskSettings.getTruncation() != null) {
            builder.field(CohereServiceFields.TRUNCATE, taskSettings.getTruncation());
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
}
