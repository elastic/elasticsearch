/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.googlevertexai.request;

import org.elasticsearch.inference.InputType;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.inference.services.googlevertexai.embeddings.GoogleVertexAiEmbeddingsTaskSettings;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.inference.InputType.invalidInputTypeMessage;

public record GoogleVertexAiEmbeddingsRequestEntity(
    List<String> inputs,
    InputType inputType,
    GoogleVertexAiEmbeddingsTaskSettings taskSettings
) implements ToXContentObject {

    private static final String INSTANCES_FIELD = "instances";
    private static final String CONTENT_FIELD = "content";
    private static final String PARAMETERS_FIELD = "parameters";
    private static final String AUTO_TRUNCATE_FIELD = "autoTruncate";
    private static final String TASK_TYPE_FIELD = "task_type";

    private static final String CLASSIFICATION_TASK_TYPE = "CLASSIFICATION";
    private static final String CLUSTERING_TASK_TYPE = "CLUSTERING";
    private static final String RETRIEVAL_DOCUMENT_TASK_TYPE = "RETRIEVAL_DOCUMENT";
    private static final String RETRIEVAL_QUERY_TASK_TYPE = "RETRIEVAL_QUERY";

    public GoogleVertexAiEmbeddingsRequestEntity {
        Objects.requireNonNull(inputs);
        Objects.requireNonNull(taskSettings);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.startArray(INSTANCES_FIELD);

        for (String input : inputs) {
            builder.startObject();
            {
                builder.field(CONTENT_FIELD, input);

                // prefer the root level inputType over task settings input type
                if (InputType.isSpecified(inputType)) {
                    builder.field(TASK_TYPE_FIELD, convertToString(inputType));
                } else if (InputType.isSpecified(taskSettings.getInputType())) {
                    builder.field(TASK_TYPE_FIELD, convertToString(taskSettings.getInputType()));
                }
            }
            builder.endObject();
        }

        builder.endArray();

        if (taskSettings.autoTruncate() != null) {
            builder.startObject(PARAMETERS_FIELD);
            {
                builder.field(AUTO_TRUNCATE_FIELD, taskSettings.autoTruncate());
            }
            builder.endObject();
        }
        builder.endObject();

        return builder;
    }

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
