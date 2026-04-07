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

import java.io.IOException;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.services.azureaistudio.AzureAiStudioConstants.DOCUMENTS_FIELD;
import static org.elasticsearch.xpack.inference.services.azureaistudio.AzureAiStudioConstants.QUERY_FIELD;
import static org.elasticsearch.xpack.inference.services.azureaistudio.AzureAiStudioConstants.RETURN_DOCUMENTS_FIELD;
import static org.elasticsearch.xpack.inference.services.azureaistudio.AzureAiStudioConstants.TOP_N_FIELD;

public record AzureAiStudioRerankRequestEntity(
    String query,
    List<String> input,
    @Nullable Boolean returnDocuments,
    @Nullable Integer topN,
    AzureAiStudioRerankTaskSettings taskSettings
) implements ToXContentObject {

    public AzureAiStudioRerankRequestEntity {
        Objects.requireNonNull(query);
        Objects.requireNonNull(input);
        Objects.requireNonNull(taskSettings);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();

        builder.field(DOCUMENTS_FIELD, input);
        builder.field(QUERY_FIELD, query);

        if (returnDocuments != null) {
            builder.field(RETURN_DOCUMENTS_FIELD, returnDocuments);
        } else if (taskSettings.returnDocuments() != null) {
            builder.field(RETURN_DOCUMENTS_FIELD, taskSettings.returnDocuments());
        }

        if (topN != null) {
            builder.field(TOP_N_FIELD, topN);
        } else if (taskSettings.topN() != null) {
            builder.field(TOP_N_FIELD, taskSettings.topN());
        }
        builder.endObject();
        return builder;
    }
}
