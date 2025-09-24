/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.contextualai.request;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.inference.services.contextualai.rerank.ContextualAiRerankModel;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

/**
 * Request entity for Contextual AI rerank API.
 * Based on the API documentation at https://docs.contextual.ai/api-reference/rerank/rerank
 */
public class ContextualAiRerankRequestEntity implements ToXContentObject {

    private static final String MODEL_FIELD = "model";
    private static final String QUERY_FIELD = "query";
    private static final String DOCUMENTS_FIELD = "documents";
    private static final String TOP_N_FIELD = "top_n";
    private static final String INSTRUCTION_FIELD = "instruction";

    private final String query;
    private final List<String> documents;
    private final Integer topN;
    private final String instruction;
    private final ContextualAiRerankModel model;

    public ContextualAiRerankRequestEntity(
        String query,
        List<String> documents,
        @Nullable Integer topN,
        @Nullable String instruction,
        ContextualAiRerankModel model
    ) {
        this.query = Objects.requireNonNull(query);
        this.documents = Objects.requireNonNull(documents);
        this.topN = topN;
        this.instruction = instruction;
        this.model = Objects.requireNonNull(model);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();

        // Order fields to match ContextualAI API expectation: query, model, top_n, instruction, documents
        builder.field(QUERY_FIELD, query);
        builder.field(MODEL_FIELD, model.modelId());

        // Add top_n field if specified
        if (topN != null) {
            builder.field(TOP_N_FIELD, topN);
        } else if (model.getTaskSettings() != null && model.getTaskSettings().getTopN() != null) {
            builder.field(TOP_N_FIELD, model.getTaskSettings().getTopN());
        }

        if (instruction != null) {
            builder.field(INSTRUCTION_FIELD, instruction);
        }

        builder.field(DOCUMENTS_FIELD, documents);

        builder.endObject();
        return builder;
    }
}
