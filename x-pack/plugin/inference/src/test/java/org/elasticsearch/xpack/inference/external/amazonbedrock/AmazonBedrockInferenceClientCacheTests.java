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
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;

import static org.elasticsearch.xpack.inference.external.amazonbedrock.AmazonBedrockInferenceClient.CLIENT_CACHE_EXPIRY_MINUTES;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.sameInstance;

public class AmazonBedrockInferenceClientCacheTests extends ESTestCase {
    public void testCache_ReturnsSameObject() throws IOException {
        AmazonBedrockInferenceClientCache cacheInstance;
        try (var cache = new AmazonBedrockInferenceClientCache(AmazonBedrockMockInferenceClient::create, Clock.systemUTC())) {
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

            var thirdClient = cache.getOrCreateClient(model, null);
            assertThat(client, sameInstance(thirdClient));

            assertThat(cache.clientCount(), is(1));
        }
        assertThat(cacheInstance.clientCount(), is(0));
    }

    public void testCache_ItEvictsExpiredClients() throws IOException {
        var clock = Clock.fixed(Instant.now(), ZoneId.systemDefault());
        AmazonBedrockInferenceClientCache cacheInstance;
        try (var cache = new AmazonBedrockInferenceClientCache(AmazonBedrockMockInferenceClient::create, clock)) {
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
                "some_other_region",
                "a_different_model",
                AmazonBedrockProvider.COHERE,
                "other_access_key",
                "other_secret_key"
            );

            assertThat(cache.clientCount(), is(1));

            var secondClient = cache.getOrCreateClient(secondModel, null);
            assertThat(client, not(sameInstance(secondClient)));

            assertThat(cache.clientCount(), is(2));

            // set clock to after expiry
            cache.setClock(Clock.fixed(clock.instant().plus(Duration.ofMinutes(CLIENT_CACHE_EXPIRY_MINUTES + 1)), ZoneId.systemDefault()));

            // get another client, this will ensure flushExpiredClients is called
            var regetSecondClient = cache.getOrCreateClient(secondModel, null);
            assertThat(secondClient, sameInstance(regetSecondClient));

            var regetFirstClient = cache.getOrCreateClient(model, null);
            assertThat(client, not(sameInstance(regetFirstClient)));
        }
        assertThat(cacheInstance.clientCount(), is(0));
    }
}
