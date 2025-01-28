/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.request.googlevertexai;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

public record GoogleVertexAiRerankRequestEntity(String query, List<String> inputs, @Nullable String model, @Nullable Integer topN)
    implements
        ToXContentObject {

    private static final String MODEL_FIELD = "model";
    private static final String QUERY_FIELD = "query";
    private static final String RECORDS_FIELD = "records";
    private static final String ID_FIELD = "id";

    private static final String CONTENT_FIELD = "content";
    private static final String TOP_N_FIELD = "topN";

    public GoogleVertexAiRerankRequestEntity {
        Objects.requireNonNull(query);
        Objects.requireNonNull(inputs);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();

        if (model != null) {
            builder.field(MODEL_FIELD, model);
        }

        builder.field(QUERY_FIELD, query);

        builder.startArray(RECORDS_FIELD);

        for (int recordId = 0; recordId < inputs.size(); recordId++) {
            builder.startObject();

            {
                builder.field(ID_FIELD, String.valueOf(recordId));
                builder.field(CONTENT_FIELD, inputs.get(recordId));
            }

            builder.endObject();
        }

        builder.endArray();

        if (topN != null) {
            builder.field(TOP_N_FIELD, topN);
        }

        builder.endObject();

        return builder;
    }
}
