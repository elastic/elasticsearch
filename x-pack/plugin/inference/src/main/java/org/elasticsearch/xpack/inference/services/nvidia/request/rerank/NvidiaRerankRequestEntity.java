/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.nvidia.request.rerank;

import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

/**
 * Entity representing the request body for Nvidia rerank requests.
 *
 * @param modelId the model identifier
 * @param query the query string
 * @param passages the list of passages to be reranked
 */
public record NvidiaRerankRequestEntity(String modelId, String query, List<String> passages) implements ToXContentObject {

    private static final String MODEL_FIELD = "model";
    private static final String QUERY_FIELD = "query";
    private static final String PASSAGES_FIELD = "passages";
    private static final String TEXT_FIELD = "text";

    public NvidiaRerankRequestEntity {
        Objects.requireNonNull(modelId);
        Objects.requireNonNull(query);
        Objects.requireNonNull(passages);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();

        builder.field(MODEL_FIELD, modelId);

        builder.startObject(QUERY_FIELD);
        builder.field(TEXT_FIELD, query);
        builder.endObject();

        builder.startArray(PASSAGES_FIELD);
        for (String passage : passages) {
            builder.startObject();
            builder.field(TEXT_FIELD, passage);
            builder.endObject();
        }
        builder.endArray();

        builder.endObject();
        return builder;
    }

}
