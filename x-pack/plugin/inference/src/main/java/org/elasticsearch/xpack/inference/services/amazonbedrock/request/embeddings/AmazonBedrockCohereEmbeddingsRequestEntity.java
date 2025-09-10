/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.amazonbedrock.request.embeddings;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.InputType;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.inference.services.amazonbedrock.embeddings.AmazonBedrockEmbeddingsTaskSettings;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.inference.InputType.invalidInputTypeMessage;

public record AmazonBedrockCohereEmbeddingsRequestEntity(
    List<String> input,
    @Nullable InputType inputType,
    AmazonBedrockEmbeddingsTaskSettings taskSettings
) implements ToXContentObject {

    private static final String TEXTS_FIELD = "texts";
    private static final String INPUT_TYPE_FIELD = "input_type";
    private static final String SEARCH_DOCUMENT = "search_document";
    private static final String SEARCH_QUERY = "search_query";
    private static final String CLUSTERING = "clustering";
    private static final String CLASSIFICATION = "classification";
    private static final String TRUNCATE = "truncate";

    public AmazonBedrockCohereEmbeddingsRequestEntity {
        Objects.requireNonNull(input);
        Objects.requireNonNull(taskSettings);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(TEXTS_FIELD, input);

        if (InputType.isSpecified(inputType)) {
            builder.field(INPUT_TYPE_FIELD, convertToString(inputType));
        } else {
            // input_type is required so default to document
            builder.field(INPUT_TYPE_FIELD, SEARCH_DOCUMENT);
        }

        if (taskSettings.cohereTruncation() != null) {
            builder.field(TRUNCATE, taskSettings.cohereTruncation().name());
        }

        builder.endObject();
        return builder;
    }

    static String convertToString(InputType inputType) {
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
