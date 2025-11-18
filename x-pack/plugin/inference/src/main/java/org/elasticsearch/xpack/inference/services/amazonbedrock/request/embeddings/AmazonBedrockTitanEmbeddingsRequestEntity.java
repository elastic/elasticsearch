/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.amazonbedrock.request.embeddings;

import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

public record AmazonBedrockTitanEmbeddingsRequestEntity(String inputText) implements ToXContentObject {

    private static final String INPUT_TEXT_FIELD = "inputText";

    public AmazonBedrockTitanEmbeddingsRequestEntity {
        Objects.requireNonNull(inputText);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(INPUT_TEXT_FIELD, inputText);
        builder.endObject();
        return builder;
    }
}
