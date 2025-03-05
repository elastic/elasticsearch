/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.request.voyageai;

import org.elasticsearch.inference.InputType;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.inference.services.voyageai.embeddings.VoyageAIEmbeddingsServiceSettings;
import org.elasticsearch.xpack.inference.services.voyageai.embeddings.VoyageAIEmbeddingsTaskSettings;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.services.voyageai.embeddings.VoyageAIEmbeddingsTaskSettings.invalidInputTypeMessage;

public record VoyageAIEmbeddingsRequestEntity(
    List<String> input,
    VoyageAIEmbeddingsServiceSettings serviceSettings,
    VoyageAIEmbeddingsTaskSettings taskSettings,
    String model
) implements ToXContentObject {

    private static final String DOCUMENT = "document";
    private static final String QUERY = "query";
    private static final String INPUT_FIELD = "input";
    private static final String MODEL_FIELD = "model";
    public static final String INPUT_TYPE_FIELD = "input_type";
    public static final String TRUNCATION_FIELD = "truncation";
    public static final String OUTPUT_DIMENSION = "output_dimension";
    static final String OUTPUT_DTYPE_FIELD = "output_dtype";

    public VoyageAIEmbeddingsRequestEntity {
        Objects.requireNonNull(input);
        Objects.requireNonNull(model);
        Objects.requireNonNull(taskSettings);
        Objects.requireNonNull(serviceSettings);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(INPUT_FIELD, input);
        builder.field(MODEL_FIELD, model);

        var inputType = convertToString(taskSettings.getInputType());
        if (inputType != null) {
            builder.field(INPUT_TYPE_FIELD, inputType);
        }

        if (taskSettings.getTruncation() != null) {
            builder.field(TRUNCATION_FIELD, taskSettings.getTruncation());
        }

        if (serviceSettings.dimensions() != null) {
            builder.field(OUTPUT_DIMENSION, serviceSettings.dimensions());
        }

        if (serviceSettings.getEmbeddingType() != null) {
            builder.field(OUTPUT_DTYPE_FIELD, serviceSettings.getEmbeddingType().toRequestString());
        }

        builder.endObject();
        return builder;
    }

    static String convertToString(InputType inputType) {
        return switch (inputType) {
            case null -> null;
            case INGEST -> DOCUMENT;
            case SEARCH -> QUERY;
            default -> {
                assert false : invalidInputTypeMessage(inputType);
                yield null;
            }
        };
    }
}
