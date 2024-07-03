/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.amazonbedrock;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.inference.services.amazonbedrock.AmazonBedrockProvider;
import org.elasticsearch.xpack.inference.services.amazonbedrock.embeddings.AmazonBedrockEmbeddingsModelTests;

import java.io.IOException;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;

public class AmazonBedrockInferenceClientCacheTests extends ESTestCase {
    public void testCache_ReturnsSameObject() throws IOException {
        AmazonBedrockInferenceClientCache cacheInstance;
        try (var cache = new AmazonBedrockInferenceClientCache(AmazonBedrockMockInferenceClient::create)) {
            cacheInstance = cache;
            var model = AmazonBedrockEmbeddingsModelTests.createModel(
                "inferenceId",
                "testregion",
                "model",
                AmazonBedrockProvider.AMAZONTITAN,
                "access_key",
                "secret_key"
            );

            var client = cache.getOrCreateClient(model, null);

            var secondModel = AmazonBedrockEmbeddingsModelTests.createModel(
                "inferenceId_two",
                "testregion",
                "a_different_model",
                AmazonBedrockProvider.COHERE,
                "access_key",
                "secret_key"
            );

            var secondClient = cache.getOrCreateClient(secondModel, null);
            assertThat(client, sameInstance(secondClient));

            assertThat(cache.clientCount(), is(1));
        }
        assertThat(cacheInstance.clientCount(), is(0));
    }
}
