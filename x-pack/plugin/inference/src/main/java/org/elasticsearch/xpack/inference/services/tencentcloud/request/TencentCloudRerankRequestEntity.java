/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.tencentcloud.request;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.inference.services.tencentcloud.rerank.TencentCloudRerankModel;
import org.elasticsearch.xpack.inference.services.tencentcloud.rerank.TencentCloudRerankTaskSettings;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

/**
 * Request body for TencentCloud AI Gateway {@code POST /v1/rerank}.
 * <pre>
 *   { "model": "bge-reranker-v2-m3", "query": "...", "documents": [...], "top_n": 3, "return_documents": true }
 * </pre>
 */
public record TencentCloudRerankRequestEntity(
    String query,
    List<String> documents,
    @Nullable Boolean returnDocuments,
    @Nullable Integer topN,
    TencentCloudRerankModel model
) implements ToXContentObject {

    public static final String QUERY_FIELD = "query";
    public static final String DOCUMENTS_FIELD = "documents";
    public static final String MODEL_FIELD = "model";

    public TencentCloudRerankRequestEntity {
        Objects.requireNonNull(query);
        Objects.requireNonNull(documents);
        Objects.requireNonNull(model);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(MODEL_FIELD, model.getServiceSettings().modelId());
        builder.field(QUERY_FIELD, query);
        builder.field(DOCUMENTS_FIELD, documents);

        TencentCloudRerankTaskSettings taskSettings = model.getTaskSettings();

        // Prefer the request-level top_n over task settings, then default from task settings.
        if (topN != null) {
            builder.field(TencentCloudRerankTaskSettings.TOP_N, topN);
        } else if (taskSettings.getTopN() != null) {
            builder.field(TencentCloudRerankTaskSettings.TOP_N, taskSettings.getTopN());
        }

        if (returnDocuments != null) {
            builder.field(TencentCloudRerankTaskSettings.RETURN_DOCUMENTS, returnDocuments);
        } else if (taskSettings.getReturnDocuments() != null) {
            builder.field(TencentCloudRerankTaskSettings.RETURN_DOCUMENTS, taskSettings.getReturnDocuments());
        }

        builder.endObject();
        return builder;
    }
}
