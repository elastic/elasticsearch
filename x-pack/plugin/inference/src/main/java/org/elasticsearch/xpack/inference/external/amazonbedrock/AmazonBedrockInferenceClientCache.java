/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.amazonbedrock;

import com.amazonaws.http.IdleConnectionReaper;

import org.elasticsearch.common.util.Maps;
import org.elasticsearch.xpack.inference.services.amazonbedrock.AmazonBedrockModel;

import java.io.Closeable;
import java.io.IOException;
import java.util.Map;

import static java.util.Collections.emptyMap;

public final class AmazonBedrockInferenceClientCache implements Closeable {

    private volatile Map<Integer, AmazonBedrockInferenceClient> clientsCache = emptyMap();

    public AmazonBedrockInferenceClient getOrCreateClient(AmazonBedrockModel model) throws IOException {
        var returnClient = internalGetOrCreateClient(model);
        flushExpiredClients();
        return returnClient;
    }

    private AmazonBedrockInferenceClient internalGetOrCreateClient(AmazonBedrockModel model) throws IOException {
        final Integer modelHash = AmazonBedrockInferenceClient.getModelKeysAndRegionHashcode(model);
        {
            final AmazonBedrockInferenceClient client = clientsCache.get(modelHash);
            if (client != null && client.tryToIncreaseReference()) {
                return client;
            }
        }

        synchronized (this) {
            final AmazonBedrockInferenceClient existing = clientsCache.get(modelHash);
            if (existing != null && existing.tryIncRef()) {
                return existing;
            }

            final AmazonBedrockInferenceClient builtClient = new AmazonBedrockInferenceClient(model);

            builtClient.mustIncRef();
            clientsCache = Maps.copyMapWithAddedEntry(clientsCache, builtClient.hashCode(), builtClient);

            return builtClient;
        }
    }

    private void flushExpiredClients() {
        var currentTimestampMs = System.currentTimeMillis();
        synchronized (this) {
            for (final AmazonBedrockInferenceClient client : clientsCache.values()) {
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
        for (final AmazonBedrockInferenceClient client : clientsCache.values()) {
            client.decRef();
        }

        // clear previously cached clients, they will be build lazily
        clientsCache = emptyMap();

        // shutdown IdleConnectionReaper background thread
        // it will be restarted on new client usage
        IdleConnectionReaper.shutdown();
    }
}
