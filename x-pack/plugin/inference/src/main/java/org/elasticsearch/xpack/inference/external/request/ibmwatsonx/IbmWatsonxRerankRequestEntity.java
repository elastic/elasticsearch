/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.request.ibmwatsonx;

import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.inference.services.ibmwatsonx.rerank.IbmWatsonxRerankTaskSettings;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

public record IbmWatsonxRerankRequestEntity(String model, String query, List<String> documents, IbmWatsonxRerankTaskSettings taskSettings)
    implements ToXContentObject {

    private static final String INPUTS_FIELD = "inputs";
    private static final String QUERY_FIELD = "query";
    private static final String MODEL_ID_FIELD = "model_id";
    private static final String PROJECT_ID_FIELD = "project_id";

    public IbmWatsonxRerankRequestEntity {
        Objects.requireNonNull(query);
        Objects.requireNonNull(documents);
        Objects.requireNonNull(taskSettings);
    }

    public IbmWatsonxRerankRequestEntity(String query, List<String> input, IbmWatsonxRerankTaskSettings taskSettings, String model) {
        this(model, query, input, taskSettings);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();

        builder.field(MODEL_ID_FIELD, model);
        builder.field(QUERY_FIELD, query);
        builder.startArray(INPUTS_FIELD);
        for (String document : documents) {
            builder.startObject();
            builder.field("text", document);
            builder.endObject();
        }
        builder.endArray();
        builder.field(PROJECT_ID_FIELD, "7fb44976-e9fd-4360-822a-1ef62bedd911");

        builder.endObject();

        return builder;
    }
}
