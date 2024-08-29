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

public record GoogleVertexAiEmbeddingsRequestEntity(List<String> inputs, @Nullable Boolean autoTruncation) implements ToXContentObject {

    private static final String INSTANCES_FIELD = "instances";
    private static final String CONTENT_FIELD = "content";
    private static final String PARAMETERS_FIELD = "parameters";
    private static final String AUTO_TRUNCATE_FIELD = "autoTruncate";

    public GoogleVertexAiEmbeddingsRequestEntity {
        Objects.requireNonNull(inputs);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.startArray(INSTANCES_FIELD);

        for (String input : inputs) {
            builder.startObject();
            {
                builder.field(CONTENT_FIELD, input);
            }
            builder.endObject();
        }

        builder.endArray();

        if (autoTruncation != null) {
            builder.startObject(PARAMETERS_FIELD);
            {
                builder.field(AUTO_TRUNCATE_FIELD, autoTruncation);
            }
            builder.endObject();
        }
        builder.endObject();

        return builder;
    }
}
