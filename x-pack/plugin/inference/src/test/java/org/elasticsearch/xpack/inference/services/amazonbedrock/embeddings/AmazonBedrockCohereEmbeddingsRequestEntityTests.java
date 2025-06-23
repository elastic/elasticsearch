/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.amazonbedrock.embeddings;

import org.elasticsearch.inference.InputType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.inference.services.amazonbedrock.request.AmazonBedrockJsonBuilder;
import org.elasticsearch.xpack.inference.services.amazonbedrock.request.embeddings.AmazonBedrockCohereEmbeddingsRequestEntity;
import org.elasticsearch.xpack.inference.services.cohere.CohereTruncation;

import java.io.IOException;
import java.util.List;

import static org.hamcrest.Matchers.is;

public class AmazonBedrockCohereEmbeddingsRequestEntityTests extends ESTestCase {
    public void testRequestEntity_GeneratesExpectedJsonBody() throws IOException {
        var entity = new AmazonBedrockCohereEmbeddingsRequestEntity(
            List.of("test input"),
            InputType.CLASSIFICATION,
            AmazonBedrockEmbeddingsTaskSettingsTests.emptyTaskSettings()
        );
        var builder = new AmazonBedrockJsonBuilder(entity);
        var result = builder.getStringContent();
        assertThat(result, is("{\"texts\":[\"test input\"],\"input_type\":\"classification\"}"));
    }

    public void testRequestEntity_GeneratesExpectedJsonBody_WithInternalInputType() throws IOException {
        var entity = new AmazonBedrockCohereEmbeddingsRequestEntity(
            List.of("test input"),
            InputType.INTERNAL_SEARCH,
            AmazonBedrockEmbeddingsTaskSettingsTests.emptyTaskSettings()
        );
        var builder = new AmazonBedrockJsonBuilder(entity);
        var result = builder.getStringContent();
        assertThat(result, is("{\"texts\":[\"test input\"],\"input_type\":\"search_query\"}"));
    }

    public void testRequestEntity_GeneratesExpectedJsonBody_WithoutInputType() throws IOException {
        var entity = new AmazonBedrockCohereEmbeddingsRequestEntity(
            List.of("test input"),
            null,
            AmazonBedrockEmbeddingsTaskSettingsTests.emptyTaskSettings()
        );
        var builder = new AmazonBedrockJsonBuilder(entity);
        var result = builder.getStringContent();
        assertThat(result, is("{\"texts\":[\"test input\"],\"input_type\":\"search_document\"}"));
    }

    public void testRequestEntity_GeneratesExpectedJsonBody_WithCohereTruncation() throws IOException {
        var entity = new AmazonBedrockCohereEmbeddingsRequestEntity(
            List.of("test input"),
            null,
            new AmazonBedrockEmbeddingsTaskSettings(CohereTruncation.START)
        );
        var builder = new AmazonBedrockJsonBuilder(entity);
        var result = builder.getStringContent();
        assertThat(result, is("{\"texts\":[\"test input\"],\"input_type\":\"search_document\",\"truncate\":\"START\"}"));
    }
}
