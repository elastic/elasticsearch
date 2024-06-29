/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.request.amazonbedrock.embeddings;

import com.fasterxml.jackson.core.JsonFactory;

import org.elasticsearch.xpack.inference.external.request.amazonbedrock.AmazonBedrockJsonWriter;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class AmazonBedrockJsonBuilder {

    private final AmazonBedrockJsonWriter jsonWriter;

    public AmazonBedrockJsonBuilder(AmazonBedrockJsonWriter jsonWriter) {
        this.jsonWriter = jsonWriter;
    }

    public String getStringContent() throws IOException {
        var factory = new JsonFactory();
        var outputStream = new ByteArrayOutputStream();
        var generator = jsonWriter.writeJson(factory.createGenerator(outputStream));
        generator.flush();
        generator.close();
        return outputStream.toString();
    }
}
