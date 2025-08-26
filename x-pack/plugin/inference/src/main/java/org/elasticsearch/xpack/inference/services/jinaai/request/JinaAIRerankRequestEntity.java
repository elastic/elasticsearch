/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.jinaai.request;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.inference.services.jinaai.rerank.JinaAIRerankTaskSettings;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

public record JinaAIRerankRequestEntity(
    String model,
    String query,
    List<String> documents,
    @Nullable Boolean returnDocuments,
    @Nullable Integer topN,
    JinaAIRerankTaskSettings taskSettings
) implements ToXContentObject {

    private static final String DOCUMENTS_FIELD = "documents";
    private static final String QUERY_FIELD = "query";
    private static final String MODEL_FIELD = "model";

    public JinaAIRerankRequestEntity {
        Objects.requireNonNull(query);
        Objects.requireNonNull(documents);
        Objects.requireNonNull(model);
        Objects.requireNonNull(taskSettings);
    }

    public JinaAIRerankRequestEntity(
        String query,
        List<String> input,
        @Nullable Boolean returnDocuments,
        @Nullable Integer topN,
        JinaAIRerankTaskSettings taskSettings,
        String model
    ) {
        this(model, query, input, returnDocuments, topN, taskSettings != null ? taskSettings : JinaAIRerankTaskSettings.EMPTY_SETTINGS);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();

        builder.field(MODEL_FIELD, model);
        builder.field(QUERY_FIELD, query);
        builder.field(DOCUMENTS_FIELD, documents);

        // prefer the root level top_n over task settings
        if (topN != null) {
            builder.field(JinaAIRerankTaskSettings.TOP_N_DOCS_ONLY, topN);
        } else if (taskSettings.getTopNDocumentsOnly() != null) {
            builder.field(JinaAIRerankTaskSettings.TOP_N_DOCS_ONLY, taskSettings.getTopNDocumentsOnly());
        }

        // prefer the root level return_documents over task settings
        if (returnDocuments != null) {
            builder.field(JinaAIRerankTaskSettings.RETURN_DOCUMENTS, returnDocuments);
        } else if (taskSettings.getDoesReturnDocuments() != null) {
            builder.field(JinaAIRerankTaskSettings.RETURN_DOCUMENTS, taskSettings.getDoesReturnDocuments());
        }

        builder.endObject();
        return builder;
    }

}
