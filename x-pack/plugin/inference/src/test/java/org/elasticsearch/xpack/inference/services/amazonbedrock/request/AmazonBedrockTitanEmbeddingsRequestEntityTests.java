/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.amazonbedrock.request;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.inference.services.amazonbedrock.AmazonBedrockServiceSettings.AmazonBedrockEmbeddingType;
import org.elasticsearch.xpack.inference.services.amazonbedrock.request.embeddings.AmazonBedrockTitanEmbeddingsRequestEntity;

import java.io.IOException;

import static org.hamcrest.Matchers.is;

public class AmazonBedrockTitanEmbeddingsRequestEntityTests extends ESTestCase {
    public void testRequestEntity_WithFloatType_GeneratesExpectedJsonBody() throws IOException {
        var entity = new AmazonBedrockTitanEmbeddingsRequestEntity("test input", AmazonBedrockEmbeddingType.FLOAT);
        var builder = new AmazonBedrockJsonBuilder(entity);
        var result = builder.getStringContent();
        assertThat(result, is("{\"inputText\":\"test input\"}"));
    }

    public void testRequestEntity_WithBinaryType_GeneratesExpectedJsonBody() throws IOException {
        var entity = new AmazonBedrockTitanEmbeddingsRequestEntity("test input", AmazonBedrockEmbeddingType.BINARY);
        var builder = new AmazonBedrockJsonBuilder(entity);
        var result = builder.getStringContent();
        assertThat(result, is("{\"inputText\":\"test input\",\"embedding_type\":\"binary\"}"));
    }
}
