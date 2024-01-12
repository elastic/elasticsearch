/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.request.cohere;

import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.inference.services.cohere.CohereServiceFields;
import org.elasticsearch.xpack.inference.services.cohere.embeddings.CohereEmbeddingsTaskSettings;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

public record CohereEmbeddingsRequestEntity(List<String> input, CohereEmbeddingsTaskSettings taskSettings) implements ToXContentObject {

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
        if (taskSettings.model() != null) {
            builder.field(CohereServiceFields.MODEL, taskSettings.model());
        }

        if (taskSettings.inputType() != null) {
            builder.field(INPUT_TYPE_FIELD, taskSettings.inputType());
        }

        if (taskSettings.embeddingTypes() != null) {
            builder.field(EMBEDDING_TYPES_FIELD, taskSettings.embeddingTypes());
        }

        if (taskSettings.truncation() != null) {
            builder.field(EMBEDDING_TYPES_FIELD, taskSettings.truncation());
        }

        builder.endObject();
        return builder;
    }
}
