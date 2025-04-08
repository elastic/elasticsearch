/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.request.elastic.rerank;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

public record ElasticInferenceServiceRerankRequestEntity(
    String query,
    List<String> documents,
    String modelId,
    @Nullable Integer topNDocumentsOnly
) implements ToXContentObject {

    private static final String QUERY_FIELD = "query";
    private static final String MODEL_FIELD = "model";
    private static final String TOP_N_DOCUMENTS_ONLY_FIELD = "top_n";
    private static final String DOCUMENTS_FIELD = "documents";

    public ElasticInferenceServiceRerankRequestEntity {
        Objects.requireNonNull(query);
        Objects.requireNonNull(documents);
        Objects.requireNonNull(modelId);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();

        builder.field(QUERY_FIELD, query);

        builder.field(MODEL_FIELD, modelId);

        if (Objects.nonNull(topNDocumentsOnly)) {
            builder.field(TOP_N_DOCUMENTS_ONLY_FIELD, topNDocumentsOnly);
        }

        builder.startArray(DOCUMENTS_FIELD);
        for (String document : documents) {
            builder.value(document);
        }

        builder.endArray();

        builder.endObject();

        return builder;
    }
}
