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

import java.io.IOException;
import java.util.List;
import java.util.Objects;

/**
 * Request entity for Contextual AI rerank API.
 * Based on the API documentation at <a href="https://docs.contextual.ai/api-reference/rerank/rerank">ContextualAI Rerank API reference</a>
 */
public class ContextualAiRerankRequestEntity implements ToXContentObject {

    private static final String MODEL_FIELD = "model";
    private static final String QUERY_FIELD = "query";
    private static final String DOCUMENTS_FIELD = "documents";
    private static final String TOP_N_FIELD = "top_n";
    private static final String INSTRUCTION_FIELD = "instruction";
    private static final String RETURN_DOCUMENTS_FIELD = "return_documents";

    private final String query;
    private final String modelId;
    private final Integer topN;
    private final String instruction;
    private final List<String> documents;
    private final Boolean returnDocuments;

    public ContextualAiRerankRequestEntity(
        String modelId,
        String query,
        List<String> documents,
        @Nullable Integer topN,
        @Nullable String instruction,
        Boolean returnDocuments
    ) {
        this.query = Objects.requireNonNull(query);
        this.modelId = Objects.requireNonNull(modelId);
        this.documents = Objects.requireNonNull(documents);
        this.topN = topN;
        this.instruction = instruction;
        this.returnDocuments = returnDocuments;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();

        // Order fields to match ContextualAI API expectation: query, model, top_n, instruction, documents
        builder.field(QUERY_FIELD, query);
        builder.field(DOCUMENTS_FIELD, documents);
        builder.field(MODEL_FIELD, modelId);

        if (topN != null) {
            builder.field(TOP_N_FIELD, topN);
        }
        if (instruction != null) {
            builder.field(INSTRUCTION_FIELD, instruction);
        }
        // TODO: check if it is recognized by ContextualAI API. It is absent in their API docs
        if (returnDocuments != null) {
            builder.field(RETURN_DOCUMENTS_FIELD, returnDocuments);
        }
        builder.endObject();
        return builder;
    }
}
