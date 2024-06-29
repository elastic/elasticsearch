/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.request.amazonbedrock.embeddings;

import com.fasterxml.jackson.core.JsonGenerator;

import org.elasticsearch.xpack.inference.external.request.amazonbedrock.AmazonBedrockJsonWriter;

import java.io.IOException;
import java.util.Objects;

public record AmazonBedrockTitanEmbeddingsRequestEntity(String inputText) implements AmazonBedrockJsonWriter {

    private static final String INPUT_TEXT_FIELD = "inputText";

    public AmazonBedrockTitanEmbeddingsRequestEntity {
        Objects.requireNonNull(inputText);
    }

    @Override
    public JsonGenerator writeJson(JsonGenerator generator) throws IOException {
        generator.writeStartObject();
        generator.writeStringField(INPUT_TEXT_FIELD, inputText);
        generator.writeEndObject();
        return generator;
    }
}
