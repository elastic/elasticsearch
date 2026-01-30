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

import static org.elasticsearch.xpack.inference.services.nvidia.request.NvidiaRequestFields.MODEL_FIELD_NAME;
import static org.elasticsearch.xpack.inference.services.nvidia.request.NvidiaRequestFields.PASSAGES_FIELD_NAME;
import static org.elasticsearch.xpack.inference.services.nvidia.request.NvidiaRequestFields.QUERY_FIELD_NAME;
import static org.elasticsearch.xpack.inference.services.nvidia.request.NvidiaRequestFields.TEXT_FIELD_NAME;

/**
 * Entity representing the request body for Nvidia rerank requests.
 *
 * @param modelId the model identifier
 * @param query the query string
 * @param passages the list of passages to be reranked
 */
public record NvidiaRerankRequestEntity(String modelId, String query, List<String> passages) implements ToXContentObject {

    public NvidiaRerankRequestEntity {
        Objects.requireNonNull(modelId);
        Objects.requireNonNull(query);
        Objects.requireNonNull(passages);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();

        builder.field(MODEL_FIELD_NAME, modelId);

        builder.startObject(QUERY_FIELD_NAME);
        builder.field(TEXT_FIELD_NAME, query);
        builder.endObject();

        builder.startArray(PASSAGES_FIELD_NAME);
        for (String passage : passages) {
            builder.startObject();
            builder.field(TEXT_FIELD_NAME, passage);
            builder.endObject();
        }
        builder.endArray();

        builder.endObject();
        return builder;
    }

}
