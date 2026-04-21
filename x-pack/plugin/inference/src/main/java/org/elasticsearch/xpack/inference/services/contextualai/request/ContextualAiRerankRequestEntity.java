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

    public static final String MODEL_FIELD = "model";
    public static final String QUERY_FIELD = "query";
    public static final String DOCUMENTS_FIELD = "documents";
    public static final String TOP_N_FIELD = "top_n";
    public static final String INSTRUCTION_FIELD = "instruction";

    private final String query;
    private final String modelId;
    private final Integer topN;
    private final String instruction;
    private final List<String> documents;

    public ContextualAiRerankRequestEntity(
        String modelId,
        String query,
        List<String> documents,
        @Nullable Integer topN,
        @Nullable String instruction
    ) {
        this.query = Objects.requireNonNull(query);
        this.modelId = Objects.requireNonNull(modelId);
        this.documents = Objects.requireNonNull(documents);
        this.topN = topN;
        this.instruction = instruction;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();

        builder.field(QUERY_FIELD, query);
        builder.field(DOCUMENTS_FIELD, documents);
        builder.field(MODEL_FIELD, modelId);
        if (topN != null) {
            builder.field(TOP_N_FIELD, topN);
        }
        if (instruction != null) {
            builder.field(INSTRUCTION_FIELD, instruction);
        }

        builder.endObject();
        return builder;
    }
}
