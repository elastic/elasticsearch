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
import org.elasticsearch.xpack.inference.services.voyageai.embeddings.multimodal.VoyageAIMultimodalEmbeddingsServiceSettings;
import org.elasticsearch.xpack.inference.services.voyageai.embeddings.multimodal.VoyageAIMultimodalEmbeddingsTaskSettings;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.inference.InputType.invalidInputTypeMessage;

public record VoyageAIMultimodalEmbeddingsRequestEntity(
    List<String> inputs,
    InputType inputType,
    VoyageAIMultimodalEmbeddingsServiceSettings serviceSettings,
    VoyageAIMultimodalEmbeddingsTaskSettings taskSettings,
    String model
) implements ToXContentObject {

    private static final String DOCUMENT = "document";
    private static final String QUERY = "query";
    private static final String INPUTS_FIELD = "inputs";  // Multimodal API uses "inputs" (plural)
    private static final String CONTENT_FIELD = "content";
    private static final String TYPE_FIELD = "type";
    private static final String TEXT_FIELD = "text";
    private static final String MODEL_FIELD = "model";
    public static final String INPUT_TYPE_FIELD = "input_type";
    public static final String TRUNCATION_FIELD = "truncation";

    public VoyageAIMultimodalEmbeddingsRequestEntity {
        Objects.requireNonNull(inputs);
        Objects.requireNonNull(model);
        Objects.requireNonNull(taskSettings);
        Objects.requireNonNull(serviceSettings);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();

        // Build multimodal inputs structure: inputs[{content: [{type: "text", text: "..."}]}]
        builder.startArray(INPUTS_FIELD);
        for (String input : inputs) {
            builder.startObject();
            builder.startArray(CONTENT_FIELD);
            builder.startObject();
            builder.field(TYPE_FIELD, "text");
            builder.field(TEXT_FIELD, input);
            builder.endObject();
            builder.endArray();
            builder.endObject();
        }
        builder.endArray();

        builder.field(MODEL_FIELD, model);

        // prefer the root level inputType over task settings input type
        if (InputType.isSpecified(inputType)) {
            builder.field(INPUT_TYPE_FIELD, convertToString(inputType));
        } else if (InputType.isSpecified(taskSettings.getInputType())) {
            builder.field(INPUT_TYPE_FIELD, convertToString(taskSettings.getInputType()));
        }

        if (taskSettings.getTruncation() != null) {
            builder.field(TRUNCATION_FIELD, taskSettings.getTruncation());
        }

        // Note: multimodal embeddings API does NOT support output_dimension or output_dtype

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
