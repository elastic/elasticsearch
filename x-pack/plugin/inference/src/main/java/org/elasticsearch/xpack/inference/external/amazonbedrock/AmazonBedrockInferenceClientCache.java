/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.amazonbedrock;

import com.amazonaws.http.IdleConnectionReaper;

import org.elasticsearch.common.util.Maps;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xpack.inference.services.amazonbedrock.AmazonBedrockModel;

import java.io.IOException;
import java.util.Map;
import java.util.function.BiFunction;

import static java.util.Collections.emptyMap;

public final class AmazonBedrockInferenceClientCache implements AmazonBedrockClientCache {

    private final BiFunction<AmazonBedrockModel, TimeValue, AmazonBedrockBaseClient> creator;
    private volatile Map<Integer, AmazonBedrockBaseClient> clientsCache = emptyMap();

    public AmazonBedrockInferenceClientCache(BiFunction<AmazonBedrockModel, TimeValue, AmazonBedrockBaseClient> creator) {
        this.creator = creator;
    }

    public AmazonBedrockBaseClient getOrCreateClient(AmazonBedrockModel model, @Nullable TimeValue timeout) throws IOException {
        var returnClient = internalGetOrCreateClient(model, timeout);
        flushExpiredClients();
        return returnClient;
    }

    private AmazonBedrockBaseClient internalGetOrCreateClient(AmazonBedrockModel model, @Nullable TimeValue timeout) throws IOException {
        final Integer modelHash = AmazonBedrockInferenceClient.getModelKeysAndRegionHashcode(model, timeout);
        {
            final AmazonBedrockBaseClient client = clientsCache.get(modelHash);
            if (client != null && client.tryToIncreaseReference()) {
                return client;
            }
        }

        synchronized (this) {
            final AmazonBedrockBaseClient existing = clientsCache.get(modelHash);
            if (existing != null && existing.tryIncRef()) {
                return existing;
            }

            final AmazonBedrockBaseClient builtClient = creator.apply(model, timeout);

            builtClient.mustIncRef();
            clientsCache = Maps.copyMapWithAddedEntry(clientsCache, builtClient.hashCode(), builtClient);

            return builtClient;
        }
    }

    private void flushExpiredClients() {
        var currentTimestampMs = System.currentTimeMillis();
        synchronized (this) {
            for (final AmazonBedrockBaseClient client : clientsCache.values()) {
                if (client.isExpired(currentTimestampMs)) {
                    client.decRef();
                }
            }
        }
    }

    @Override
    public void close() throws IOException {
        releaseCachedClients();
    }

    private void releaseCachedClients() {
        // the clients will shutdown when they will not be used anymore
        for (final AmazonBedrockBaseClient client : clientsCache.values()) {
            client.decRef();
        }

        // clear previously cached clients, they will be build lazily
        clientsCache = emptyMap();

        // shutdown IdleConnectionReaper background thread
        // it will be restarted on new client usage
        IdleConnectionReaper.shutdown();
    }

    // used for testing
    public int clientCount() {
        return clientsCache.size();
    }
}
