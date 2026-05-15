/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.openai.request;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

public record OpenAiEmbeddingsRequestEntity(
    List<String> input,
    String model,
    @Nullable String user,
    @Nullable Integer dimensions,
    boolean dimensionsSetByUser
) implements ToXContentObject {

    private static final String INPUT_FIELD = "input";
    private static final String MODEL_FIELD = "model";
    private static final String USER_FIELD = "user";
    private static final String DIMENSIONS_FIELD = "dimensions";
    private static final String ENCODING_FORMAT_FIELD = "encoding_format";
    private static final String ENCODING_FORMAT_BASE64 = "base64";

    public OpenAiEmbeddingsRequestEntity {
        Objects.requireNonNull(input);
        Objects.requireNonNull(model);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(INPUT_FIELD, input);
        builder.field(MODEL_FIELD, model);

        if (user != null) {
            builder.field(USER_FIELD, user);
        }

        if (dimensionsSetByUser && dimensions != null) {
            builder.field(DIMENSIONS_FIELD, dimensions);
        }

        // Request the embedding values as a base64-encoded packed little-endian float32
        // string rather than a JSON array of floats. The OpenAI Python SDK has used this
        // as its default since 2023; the wire form is ~3x smaller and parses without
        // autoboxing into List<Float>. The response parser accepts both shapes for
        // forward/backward compatibility.
        builder.field(ENCODING_FORMAT_FIELD, ENCODING_FORMAT_BASE64);

        builder.endObject();
        return builder;
    }
}
