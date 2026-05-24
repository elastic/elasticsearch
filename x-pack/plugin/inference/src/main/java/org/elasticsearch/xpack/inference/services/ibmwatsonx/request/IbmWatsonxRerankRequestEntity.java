/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.ibmwatsonx.request;

import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.inference.services.ibmwatsonx.rerank.IbmWatsonxRerankTaskSettings;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

public record IbmWatsonxRerankRequestEntity(
    String query,
    List<String> inputs,
    IbmWatsonxRerankTaskSettings taskSettings,
    String modelId,
    String projectId
) implements ToXContentObject {

    private static final String INPUTS_FIELD = "inputs";
    private static final String QUERY_FIELD = "query";
    private static final String MODEL_ID_FIELD = "model_id";
    private static final String PROJECT_ID_FIELD = "project_id";

    public IbmWatsonxRerankRequestEntity {
        Objects.requireNonNull(query);
        Objects.requireNonNull(inputs);
        Objects.requireNonNull(modelId);
        Objects.requireNonNull(projectId);
        Objects.requireNonNull(taskSettings);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();

        builder.field(MODEL_ID_FIELD, modelId);
        builder.field(QUERY_FIELD, query);
        builder.startArray(INPUTS_FIELD);
        for (String input : inputs) {
            builder.startObject();
            builder.field("text", input);
            builder.endObject();
        }
        builder.endArray();
        builder.field(PROJECT_ID_FIELD, projectId);

        builder.startObject("parameters");
        {
            if (taskSettings.getTruncateInputTokens() != null) {
                builder.field("truncate_input_tokens", taskSettings.getTruncateInputTokens());
            }

            builder.startObject("return_options");
            {
                if (taskSettings.getDoesReturnDocuments() != null) {
                    builder.field("inputs", taskSettings.getDoesReturnDocuments());
                }
                if (taskSettings.getTopNDocumentsOnly() != null) {
                    builder.field("top_n", taskSettings.getTopNDocumentsOnly());
                }
            }
            builder.endObject();
        }
        builder.endObject();

        builder.endObject();

        return builder;
    }
}
