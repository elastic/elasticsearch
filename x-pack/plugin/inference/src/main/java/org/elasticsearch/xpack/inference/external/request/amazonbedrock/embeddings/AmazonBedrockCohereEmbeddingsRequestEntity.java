/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.request.amazonbedrock.embeddings;

import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

public record AmazonBedrockCohereEmbeddingsRequestEntity(List<String> input) implements ToXContentObject {

    private static final String TEXTS_FIELD = "texts";
    private static final String INPUT_TYPE_FIELD = "input_type";
    private static final String INPUT_TYPE_SEARCH_DOCUMENT = "search_document";

    public AmazonBedrockCohereEmbeddingsRequestEntity {
        Objects.requireNonNull(input);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(TEXTS_FIELD, input);
        builder.field(INPUT_TYPE_FIELD, INPUT_TYPE_SEARCH_DOCUMENT);
        builder.endObject();
        return builder;
    }
}
