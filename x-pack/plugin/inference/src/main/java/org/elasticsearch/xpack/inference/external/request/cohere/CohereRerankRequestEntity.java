/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.request.cohere;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.inference.services.cohere.rerank.CohereRerankTaskSettings;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

public record CohereRerankRequestEntity(
    String query,
    List<String> documents,
    CohereRerankTaskSettings taskSettings,
    @Nullable String model,
    @Nullable Integer topNDocumentsOnly,
    @Nullable Integer maxChunksPerDoc
) implements ToXContentObject {

    private static final String DOCUMENTS_FIELD = "documents";
    private static final String QUERY_FIELD = "query";
    private static final String RETURN_DOCUMENTS_FIELD = "return_documents";
    private static final String TOP_N_FIELD = "top_n";
    private static final String MAX_CHUNKS_PER_DOC = "max_chunks_per_doc";
    private static final String COHERE_MODEL_ID_FIELD = "model";

    public CohereRerankRequestEntity {
        Objects.requireNonNull(documents);
        Objects.requireNonNull(query);
        Objects.requireNonNull(taskSettings);
    }

    public CohereRerankRequestEntity(String query, List<String> input, CohereRerankTaskSettings taskSettings, String model) {
        this(query, input, taskSettings, model, null, null);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();

        builder.field(QUERY_FIELD, query);
        builder.field(DOCUMENTS_FIELD, documents);

        if (taskSettings.getDoesReturnDocuments() != null) {
            builder.field(RETURN_DOCUMENTS_FIELD, taskSettings.getDoesReturnDocuments());
        }

        if (model != null) {
            builder.field(COHERE_MODEL_ID_FIELD, model);
        }

        if (topNDocumentsOnly != null) {
            builder.field(TOP_N_FIELD, topNDocumentsOnly);
        }

        if (maxChunksPerDoc != null) {
            builder.field(MAX_CHUNKS_PER_DOC, maxChunksPerDoc);
        }

        builder.endObject();
        return builder;
    }

}
