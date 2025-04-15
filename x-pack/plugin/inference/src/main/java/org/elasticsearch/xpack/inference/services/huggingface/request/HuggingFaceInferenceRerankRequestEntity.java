/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.huggingface.request;

import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

public record HuggingFaceInferenceRerankRequestEntity(List<String> documents, String query) implements ToXContentObject {

    private static final String DOCUMENTS_FIELD = "documents";
    private static final String QUERY_FIELD = "query";

    public HuggingFaceInferenceRerankRequestEntity {
        Objects.requireNonNull(documents);
        Objects.requireNonNull(query);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();

        builder.field(DOCUMENTS_FIELD, documents);
        builder.field(QUERY_FIELD, query);

        builder.endObject();
        return builder;
    }
}
