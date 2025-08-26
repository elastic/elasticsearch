/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.ibmwatsonx.request;

import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

public record IbmWatsonxEmbeddingsRequestEntity(List<String> inputs, String modelId, String projectId) implements ToXContentObject {

    private static final String INPUTS_FIELD = "inputs";
    private static final String MODEL_ID_FIELD = "model_id";
    private static final String PROJECT_ID_FIELD = "project_id";

    public IbmWatsonxEmbeddingsRequestEntity {
        Objects.requireNonNull(inputs);
        Objects.requireNonNull(modelId);
        Objects.requireNonNull(projectId);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(INPUTS_FIELD, inputs);
        builder.field(MODEL_ID_FIELD, modelId);
        builder.field(PROJECT_ID_FIELD, projectId);
        builder.endObject();

        return builder;
    }
}
