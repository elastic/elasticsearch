/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.voyageai.request;

import org.elasticsearch.inference.InputType;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.inference.services.voyageai.embeddings.VoyageAIEmbeddingsServiceSettings;
import org.elasticsearch.xpack.inference.services.voyageai.embeddings.VoyageAIEmbeddingsTaskSettings;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.inference.InputType.invalidInputTypeMessage;

public record VoyageAIEmbeddingsRequestEntity(
    List<String> input,
    InputType inputType,
    VoyageAIEmbeddingsServiceSettings serviceSettings,
    VoyageAIEmbeddingsTaskSettings taskSettings
) implements ToXContentObject {

    // Accepted input_type values
    private static final String INPUT_TYPE_DOCUMENT_VALUE = "document";
    private static final String INPUT_TYPE_QUERY_VALUE = "query";

    // Field names for request body
    public static final String INPUT_FIELD = "input";
    public static final String MODEL_FIELD = "model";
    public static final String INPUT_TYPE_FIELD = "input_type";
    public static final String TRUNCATION_FIELD = "truncation";
    public static final String OUTPUT_DIMENSION_FIELD = "output_dimension";
    public static final String OUTPUT_DTYPE_FIELD = "output_dtype";

    public VoyageAIEmbeddingsRequestEntity {
        Objects.requireNonNull(input);
        Objects.requireNonNull(taskSettings);
        Objects.requireNonNull(serviceSettings);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(INPUT_FIELD, input);
        builder.field(MODEL_FIELD, serviceSettings.modelId());
        // prefer the root level inputType over task settings input type
        if (InputType.isSpecified(inputType)) {
            builder.field(INPUT_TYPE_FIELD, convertInputTypeToString(inputType));
        } else if (InputType.isSpecified(taskSettings.inputType())) {
            builder.field(INPUT_TYPE_FIELD, convertInputTypeToString(taskSettings.inputType()));
        }
        if (taskSettings.truncation() != null) {
            builder.field(TRUNCATION_FIELD, taskSettings.truncation());
        }
        if (serviceSettings.dimensions() != null) {
            builder.field(OUTPUT_DIMENSION_FIELD, serviceSettings.dimensions());
        }
        builder.field(OUTPUT_DTYPE_FIELD, serviceSettings.embeddingType().toRequestString());
        builder.endObject();
        return builder;
    }

    public static String convertInputTypeToString(InputType inputType) {
        return switch (inputType) {
            case null -> null;
            case INGEST, INTERNAL_INGEST -> INPUT_TYPE_DOCUMENT_VALUE;
            case SEARCH, INTERNAL_SEARCH -> INPUT_TYPE_QUERY_VALUE;
            default -> {
                assert false : invalidInputTypeMessage(inputType);
                yield null;
            }
        };
    }
}
