/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.request.azureaistudio;

import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.inference.services.azureaistudio.embeddings.AzureAiStudioEmbeddingsModel;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.services.azureaistudio.AzureAiStudioConstants.USER_FIELD;

public class AzureAiStudioEmbeddingsRequestEntity implements ToXContentObject {

    private final AzureAiStudioEmbeddingsModel model;
    private final List<String> inputs;

    public AzureAiStudioEmbeddingsRequestEntity(AzureAiStudioEmbeddingsModel model, List<String> inputs) {
        this.model = Objects.requireNonNull(model);
        this.inputs = Objects.requireNonNull(inputs);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();

        if (inputs.size() == 1) {
            builder.field("input", inputs.get(0));
        } else {
            builder.array("input", inputs);
        }

        var taskUser = this.model.getTaskSettings().user();
        if (taskUser != null) {
            builder.field(USER_FIELD, taskUser);
        }

        builder.endObject();

        return builder;
    }
}
