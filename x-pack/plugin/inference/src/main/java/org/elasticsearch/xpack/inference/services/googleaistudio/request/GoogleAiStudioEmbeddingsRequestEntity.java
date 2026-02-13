/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.googleaistudio.request;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.InputType;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.core.Strings.format;
import static org.elasticsearch.inference.InputType.invalidInputTypeMessage;

public record GoogleAiStudioEmbeddingsRequestEntity(List<String> inputs, InputType inputType, String model, @Nullable Integer dimensions)
    implements
        ToXContentObject {

    private static final String REQUESTS_FIELD = "requests";
    private static final String MODEL_FIELD = "model";

    private static final String MODELS_PREFIX = "models";
    private static final String CONTENT_FIELD = "content";
    private static final String PARTS_FIELD = "parts";
    private static final String TEXT_FIELD = "text";

    public static final String TASK_TYPE_FIELD = "taskType";
    private static final String CLASSIFICATION_TASK_TYPE = "CLASSIFICATION";
    private static final String CLUSTERING_TASK_TYPE = "CLUSTERING";
    private static final String RETRIEVAL_DOCUMENT_TASK_TYPE = "RETRIEVAL_DOCUMENT";
    private static final String RETRIEVAL_QUERY_TASK_TYPE = "RETRIEVAL_QUERY";

    private static final String OUTPUT_DIMENSIONALITY_FIELD = "outputDimensionality";

    public GoogleAiStudioEmbeddingsRequestEntity {
        Objects.requireNonNull(inputs);
        Objects.requireNonNull(model);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.startArray(REQUESTS_FIELD);

        for (String input : inputs) {
            builder.startObject();
            builder.field(MODEL_FIELD, format("%s/%s", MODELS_PREFIX, model));

            {
                builder.startObject(CONTENT_FIELD);

                {
                    builder.startArray(PARTS_FIELD);

                    {
                        builder.startObject();
                        builder.field(TEXT_FIELD, input);
                        builder.endObject();
                    }

                    builder.endArray();
                }

                builder.endObject();
            }

            if (dimensions != null) {
                builder.field(OUTPUT_DIMENSIONALITY_FIELD, dimensions);
            }

            if (InputType.isSpecified(inputType)) {
                builder.field(TASK_TYPE_FIELD, convertToString(inputType));
            }

            builder.endObject();
        }

        builder.endArray();

        builder.endObject();

        return builder;
    }

    // default for testing
    public static String convertToString(InputType inputType) {
        return switch (inputType) {
            case INGEST, INTERNAL_INGEST -> RETRIEVAL_DOCUMENT_TASK_TYPE;
            case SEARCH, INTERNAL_SEARCH -> RETRIEVAL_QUERY_TASK_TYPE;
            case CLASSIFICATION -> CLASSIFICATION_TASK_TYPE;
            case CLUSTERING -> CLUSTERING_TASK_TYPE;
            default -> {
                assert false : invalidInputTypeMessage(inputType);
                yield null;
            }
        };
    }
}
