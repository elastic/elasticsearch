/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.elastic.request;

import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

/**
 * Adapter that converts COMPLETION task inputs (List of String) into chat message format
 * for the ElasticInferenceService chat endpoint.
 */
public class ElasticInferenceServiceCompletionRequestEntity implements ToXContentObject {

    private static final String USER_ROLE = "user";
    private static final String MESSAGES_FIELD = "messages";
    private static final String ROLE_FIELD = "role";
    private static final String CONTENT_FIELD = "content";
    private static final String MODEL_FIELD = "model";
    private static final String STREAM_FIELD = "stream";
    private static final String NUMBER_OF_RETURNED_CHOICES_FIELD = "n";

    private final List<String> inputs;
    private final String modelId;

    public ElasticInferenceServiceCompletionRequestEntity(List<String> inputs, String modelId) {
        this.inputs = Objects.requireNonNull(inputs);
        this.modelId = Objects.requireNonNull(modelId);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(MODEL_FIELD, modelId);

        // Convert simple text inputs to chat messages format
        builder.startArray(MESSAGES_FIELD);
        for (String input : inputs) {
            builder.startObject();
            builder.field(ROLE_FIELD, USER_ROLE);
            builder.field(CONTENT_FIELD, input);
            builder.endObject();
        }
        builder.endArray();

        // Always use non-streaming for COMPLETION adapter
        builder.field(STREAM_FIELD, false);
        // Request only 1 choice per input
        builder.field(NUMBER_OF_RETURNED_CHOICES_FIELD, 1);

        builder.endObject();
        return builder;
    }
}

