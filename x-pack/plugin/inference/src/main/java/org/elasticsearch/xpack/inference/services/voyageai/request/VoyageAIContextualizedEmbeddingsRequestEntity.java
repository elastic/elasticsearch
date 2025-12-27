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
import org.elasticsearch.xpack.inference.services.voyageai.embeddings.contextual.VoyageAIContextualEmbeddingsServiceSettings;
import org.elasticsearch.xpack.inference.services.voyageai.embeddings.contextual.VoyageAIContextualEmbeddingsTaskSettings;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.inference.InputType.invalidInputTypeMessage;

/**
 * Request entity for VoyageAI contextualized embeddings API.
 *
 * This differs from regular embeddings in that it accepts nested lists of strings,
 * where each inner list represents chunks from a single document that should be
 * contextualized together.
 */
public record VoyageAIContextualizedEmbeddingsRequestEntity(
    List<List<String>> inputs,
    InputType inputType,
    VoyageAIContextualEmbeddingsServiceSettings serviceSettings,
    VoyageAIContextualEmbeddingsTaskSettings taskSettings,
    String model
) implements ToXContentObject {

    private static final String DOCUMENT = "document";
    private static final String QUERY = "query";
    private static final String INPUTS_FIELD = "inputs";
    private static final String MODEL_FIELD = "model";
    public static final String INPUT_TYPE_FIELD = "input_type";
    public static final String OUTPUT_DIMENSION = "output_dimension";
    static final String OUTPUT_DTYPE_FIELD = "output_dtype";

    public VoyageAIContextualizedEmbeddingsRequestEntity {
        Objects.requireNonNull(inputs);
        Objects.requireNonNull(model);
        Objects.requireNonNull(taskSettings);
        Objects.requireNonNull(serviceSettings);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(INPUTS_FIELD, inputs);
        builder.field(MODEL_FIELD, model);

        // prefer the root level inputType over task settings input type
        if (InputType.isSpecified(inputType)) {
            builder.field(INPUT_TYPE_FIELD, convertToString(inputType));
        } else if (InputType.isSpecified(taskSettings.getInputType())) {
            builder.field(INPUT_TYPE_FIELD, convertToString(taskSettings.getInputType()));
        }

        // Add output_dimension if available in serviceSettings
        if (serviceSettings.dimensions() != null) {
            builder.field(OUTPUT_DIMENSION, serviceSettings.dimensions());
        }

        // Add output_dtype if available in serviceSettings
        if (serviceSettings.getEmbeddingType() != null) {
            builder.field(OUTPUT_DTYPE_FIELD, serviceSettings.getEmbeddingType().toRequestString());
        }

        builder.endObject();
        return builder;
    }

    public static String convertToString(InputType inputType) {
        return switch (inputType) {
            case null -> null;
            case INGEST, INTERNAL_INGEST -> DOCUMENT;
            case SEARCH, INTERNAL_SEARCH -> QUERY;
            default -> {
                assert false : invalidInputTypeMessage(inputType);
                yield null;
            }
        };
    }
}
