/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.fireworksai.request;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.inference.services.fireworksai.rerank.FireworksAiRerankModel;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

/**
 * Request entity for FireworksAI rerank API.
 * Based on the API documentation at https://docs.fireworks.ai/
 */
public class FireworksAiRerankRequestEntity implements ToXContentObject {

    private static final String MODEL_FIELD = "model";
    private static final String QUERY_FIELD = "query";
    private static final String DOCUMENTS_FIELD = "documents";
    private static final String TOP_N_FIELD = "top_n";
    private static final String RETURN_DOCUMENTS_FIELD = "return_documents";

    private final String query;
    private final List<String> documents;
    private final Integer topN;
    private final Boolean returnDocuments;
    private final FireworksAiRerankModel model;

    public FireworksAiRerankRequestEntity(
        String query,
        List<String> documents,
        @Nullable Integer topN,
        @Nullable Boolean returnDocuments,
        FireworksAiRerankModel model
    ) {
        this.query = Objects.requireNonNull(query);
        this.documents = Objects.requireNonNull(documents);
        this.topN = topN;
        this.returnDocuments = returnDocuments;
        this.model = Objects.requireNonNull(model);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();

        builder.field(MODEL_FIELD, model.modelId());
        builder.field(QUERY_FIELD, query);
        builder.field(DOCUMENTS_FIELD, documents);

        if (topN != null) {
            builder.field(TOP_N_FIELD, topN);
        } else if (model.getTaskSettings() != null && model.getTaskSettings().getTopN() != null) {
            builder.field(TOP_N_FIELD, model.getTaskSettings().getTopN());
        }

        if (returnDocuments != null) {
            builder.field(RETURN_DOCUMENTS_FIELD, returnDocuments);
        } else if (model.getTaskSettings() != null && model.getTaskSettings().getReturnDocuments() != null) {
            builder.field(RETURN_DOCUMENTS_FIELD, model.getTaskSettings().getReturnDocuments());
        }

        builder.endObject();
        return builder;
    }
}
