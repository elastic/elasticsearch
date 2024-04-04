/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.request.azureopenai;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

public record AzureOpenAiEmbeddingsRequestEntity(
    List<String> input,
    @Nullable String user,
    @Nullable Integer dimensions,
    boolean dimensionsSetByUser,
    @Nullable String encodingFormat,
    boolean encodingFormatSetByUser
) implements ToXContentObject {

    private static final String INPUT_FIELD = "input";
    private static final String ENCODING_FORMAT_FIELD = "encoding_format";
    private static final String USER_FIELD = "user";
    private static final String DIMENSIONS_FIELD = "dimensions";

    public AzureOpenAiEmbeddingsRequestEntity {
        Objects.requireNonNull(input);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(INPUT_FIELD, input);

        if (user != null) {
            builder.field(USER_FIELD, user);
        }

        if (dimensionsSetByUser && dimensions != null) {
            builder.field(DIMENSIONS_FIELD, dimensions);
        }

        if (encodingFormatSetByUser && encodingFormat != null) {
            builder.field(ENCODING_FORMAT_FIELD, encodingFormat);
        }

        builder.endObject();
        return builder;
    }
}
