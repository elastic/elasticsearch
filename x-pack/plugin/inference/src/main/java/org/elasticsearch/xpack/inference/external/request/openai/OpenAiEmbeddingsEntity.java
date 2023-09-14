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

public record OpenAiEmbeddingsEntity(String input, String user) implements ToXContentObject {

    private static final String INPUT_FIELD = "input";
    private static final String USER_FIELD = "user";

    public OpenAiEmbeddingsEntity {
        Objects.requireNonNull(input);
    }

    public OpenAiEmbeddingsEntity(String input) {
        this(input, null);
    }

    public Optional<String> getUser() {
        return Optional.ofNullable(user);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(INPUT_FIELD, input);

        var optionalUser = getUser();
        if (optionalUser.isPresent()) {
            builder.field(USER_FIELD, optionalUser.get());
        }

        builder.endObject();
        return builder;
    }
}
