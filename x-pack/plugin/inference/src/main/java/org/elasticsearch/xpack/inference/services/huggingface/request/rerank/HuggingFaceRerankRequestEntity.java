/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.huggingface.request.rerank;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.inference.services.huggingface.rerank.HuggingFaceRerankTaskSettings;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

public record HuggingFaceRerankRequestEntity(
    String query,
    List<String> documents,
    @Nullable Boolean returnDocuments,
    @Nullable Integer topN,
    HuggingFaceRerankTaskSettings taskSettings
) implements ToXContentObject {

    private static final String RETURN_TEXT = "return_text";
    private static final String DOCUMENTS_FIELD = "texts";
    private static final String QUERY_FIELD = "query";

    public HuggingFaceRerankRequestEntity {
        Objects.requireNonNull(query);
        Objects.requireNonNull(documents);
        Objects.requireNonNull(taskSettings);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();

        builder.field(DOCUMENTS_FIELD, documents);
        builder.field(QUERY_FIELD, query);

        // prefer the root level return_documents over task settings
        if (returnDocuments != null) {
            builder.field(RETURN_TEXT, returnDocuments);
        } else if (taskSettings.getReturnDocuments() != null) {
            builder.field(RETURN_TEXT, taskSettings.getReturnDocuments());
        }

        if (topN != null) {
            builder.field(HuggingFaceRerankTaskSettings.TOP_N_DOCS_ONLY, topN);
        } else if (taskSettings.getTopNDocumentsOnly() != null) {
            builder.field(HuggingFaceRerankTaskSettings.TOP_N_DOCS_ONLY, taskSettings.getTopNDocumentsOnly());
        }

        builder.endObject();
        return builder;
    }
}
