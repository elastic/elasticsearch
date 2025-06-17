/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.azureaistudio.request;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.inference.services.azureaistudio.rerank.AzureAiStudioRerankTaskSettings;
import org.elasticsearch.xpack.inference.services.huggingface.rerank.HuggingFaceRerankTaskSettings;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

public record AzureAiStudioRerankRequestEntity(
    String query,
    List<String> input,
    @Nullable Boolean returnDocuments,
    @Nullable Integer topN,
    AzureAiStudioRerankTaskSettings taskSettings
) implements ToXContentObject {

    private static final String RETURN_TEXT = "return_text";
    private static final String DOCUMENTS_FIELD = "texts";
    private static final String QUERY = "query";

    public AzureAiStudioRerankRequestEntity {
        Objects.requireNonNull(query);
        Objects.requireNonNull(input);
        Objects.requireNonNull(taskSettings);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();

        builder.field(DOCUMENTS_FIELD, input);
        builder.field(QUERY, query);

        if (returnDocuments != null) {
            builder.field(RETURN_TEXT, returnDocuments);
        } else if (taskSettings.returnDocuments() != null) {
            builder.field(RETURN_TEXT, taskSettings.returnDocuments());
        }

        if (topN != null) {
            builder.field(HuggingFaceRerankTaskSettings.TOP_N_DOCS_ONLY, topN);
        } else if (taskSettings.topN() != null) {
            builder.field(HuggingFaceRerankTaskSettings.TOP_N_DOCS_ONLY, taskSettings.topN());
        }
        builder.endObject();
        return builder;
    }
}
