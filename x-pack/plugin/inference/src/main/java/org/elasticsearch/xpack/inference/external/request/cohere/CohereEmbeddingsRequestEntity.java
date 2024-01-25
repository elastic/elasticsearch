/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.request.cohere;

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

public record CohereEmbeddingsRequestEntity(
    List<String> input,
    CohereEmbeddingsTaskSettings taskSettings,
    @Nullable String model,
    @Nullable CohereEmbeddingType embeddingType
) implements ToXContentObject {

    private static final String SEARCH_DOCUMENT = "search_document";
    private static final String SEARCH_QUERY = "search_query";
    /**
     * Maps the {@link InputType} to the expected value for cohere for the input_type field in the request using the enum's ordinal.
     * The order of these entries is important and needs to match the order in the enum
     */
    private static final String[] INPUT_TYPE_MAPPING = { SEARCH_DOCUMENT, SEARCH_QUERY };
    static {
        assert INPUT_TYPE_MAPPING.length == InputType.values().length : "input type mapping was incorrectly defined";
    }

    private static final String TEXTS_FIELD = "texts";

    static final String INPUT_TYPE_FIELD = "input_type";
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
            builder.field(CohereServiceSettings.MODEL, model);
        }

        if (taskSettings.inputType() != null) {
            builder.field(INPUT_TYPE_FIELD, covertToString(taskSettings.inputType()));
        }

        if (embeddingType != null) {
            builder.field(EMBEDDING_TYPES_FIELD, List.of(embeddingType));
        }

        if (taskSettings.truncation() != null) {
            builder.field(CohereServiceFields.TRUNCATE, taskSettings.truncation());
        }

        builder.endObject();
        return builder;
    }

    private static String covertToString(InputType inputType) {
        return INPUT_TYPE_MAPPING[inputType.ordinal()];
    }
}
