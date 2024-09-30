/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.request.amazonbedrock.embeddings;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.inference.external.request.amazonbedrock.AmazonBedrockJsonBuilder;

import java.io.IOException;
import java.util.List;

import static org.hamcrest.Matchers.is;

public class AmazonBedrockCohereEmbeddingsRequestEntityTests extends ESTestCase {
    public void testRequestEntity_GeneratesExpectedJsonBody() throws IOException {
        var entity = new AmazonBedrockCohereEmbeddingsRequestEntity(List.of("test input"));
        var builder = new AmazonBedrockJsonBuilder(entity);
        var result = builder.getStringContent();
        assertThat(result, is("{\"texts\":[\"test input\"],\"input_type\":\"search_document\"}"));
    }
}
