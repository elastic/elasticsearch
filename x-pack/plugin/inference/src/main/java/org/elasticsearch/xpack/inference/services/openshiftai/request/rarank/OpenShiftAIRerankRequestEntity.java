/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.openshiftai.request.rarank;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.inference.services.openshiftai.rerank.OpenShiftAiRerankTaskSettings;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

/**
 * Entity representing the request body for OpenShift AI rerank requests.
 * @param modelId the model identifier (optional)
 * @param query the query string
 * @param documents the list of documents to be reranked
 * @param returnDocuments whether to return the documents in the response (optional)
 * @param topN the number of top results to return (optional)
 */
public record OpenShiftAIRerankRequestEntity(
    @Nullable String modelId,
    String query,
    List<String> documents,
    @Nullable Boolean returnDocuments,
    @Nullable Integer topN
) implements ToXContentObject {

    private static final String MODEL_FIELD = "model";
    private static final String DOCUMENTS_FIELD = "documents";
    private static final String QUERY_FIELD = "query";

    public OpenShiftAIRerankRequestEntity {
        Objects.requireNonNull(query);
        Objects.requireNonNull(documents);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();

        // model field is optional for OpenShift AI
        if (modelId != null) {
            builder.field(MODEL_FIELD, modelId);
        }
        builder.field(QUERY_FIELD, query);
        builder.field(DOCUMENTS_FIELD, documents);

        if (topN != null) {
            builder.field(OpenShiftAiRerankTaskSettings.TOP_N, topN);
        }

        if (returnDocuments != null) {
            builder.field(OpenShiftAiRerankTaskSettings.RETURN_DOCUMENTS, returnDocuments);
        }

        builder.endObject();
        return builder;
    }

}
