/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.request.openai;

import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;
import java.util.Optional;

public record OpenAiEmbeddingsRequestEntity(String input, String model, String user) implements ToXContentObject {

    private static final String INPUT_FIELD = "input";
    private static final String MODEL_FIELD = "model";
    private static final String USER_FIELD = "user";

    public OpenAiEmbeddingsRequestEntity {
        Objects.requireNonNull(input);
        Objects.requireNonNull(model);
    }

    public Optional<String> getUser() {
        return Optional.ofNullable(user);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(INPUT_FIELD, input);
        builder.field(MODEL_FIELD, model);

        var optionalUser = getUser();
        if (optionalUser.isPresent()) {
            builder.field(USER_FIELD, optionalUser.get());
        }

        builder.endObject();
        return builder;
    }
}
