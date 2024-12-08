/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.amazonbedrock;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xpack.inference.services.amazonbedrock.AmazonBedrockModel;

import java.io.IOException;
import java.time.Clock;
import java.util.ArrayList;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.BiFunction;

public final class AmazonBedrockInferenceClientCache implements AmazonBedrockClientCache {

    private final BiFunction<AmazonBedrockModel, TimeValue, AmazonBedrockBaseClient> creator;
    private final Map<Integer, AmazonBedrockBaseClient> clientsCache = new ConcurrentHashMap<>();
    private final ReentrantReadWriteLock cacheLock = new ReentrantReadWriteLock();

    // not final for testing
    private Clock clock;

    public AmazonBedrockInferenceClientCache(BiFunction<AmazonBedrockModel, TimeValue, AmazonBedrockBaseClient> creator, Clock clock) {
        this.creator = Objects.requireNonNull(creator);
        this.clock = Objects.requireNonNull(clock);
    }

    public AmazonBedrockBaseClient getOrCreateClient(AmazonBedrockModel model, @Nullable TimeValue timeout) {
        var returnClient = internalGetOrCreateClient(model, timeout);
        flushExpiredClients();
        return returnClient;
    }

    private AmazonBedrockBaseClient internalGetOrCreateClient(AmazonBedrockModel model, @Nullable TimeValue timeout) {
        final Integer modelHash = AmazonBedrockInferenceClient.getModelKeysAndRegionHashcode(model, timeout);
        cacheLock.readLock().lock();
        try {
            return clientsCache.computeIfAbsent(modelHash, hashKey -> {
                final AmazonBedrockBaseClient builtClient = creator.apply(model, timeout);
                builtClient.setClock(clock);
                builtClient.resetExpiration();
                return builtClient;
            });
        } finally {
            cacheLock.readLock().unlock();
        }
    }

    private void flushExpiredClients() {
        var currentTimestampMs = clock.instant();
        var expiredClients = new ArrayList<Map.Entry<Integer, AmazonBedrockBaseClient>>();

        cacheLock.readLock().lock();
        try {
            for (final Map.Entry<Integer, AmazonBedrockBaseClient> client : clientsCache.entrySet()) {
                if (client.getValue().isExpired(currentTimestampMs)) {
                    expiredClients.add(client);
                }
            }

            if (expiredClients.isEmpty()) {
                return;
            }

            cacheLock.readLock().unlock();
            cacheLock.writeLock().lock();
            try {
                for (final Map.Entry<Integer, AmazonBedrockBaseClient> client : expiredClients) {
                    var removed = clientsCache.remove(client.getKey());
                    if (removed != null) {
                        removed.close();
                    }
                }
            } finally {
                cacheLock.readLock().lock();
                cacheLock.writeLock().unlock();
            }
        } finally {
            cacheLock.readLock().unlock();
        }
    }

    @Override
    public void close() throws IOException {
        releaseCachedClients();
    }

    private void releaseCachedClients() {
        // as we're closing and flushing all of these - we'll use a write lock
        // across the whole operation to ensure this stays in sync
        cacheLock.writeLock().lock();
        try {
            // ensure all the clients are closed before we clear
            for (final AmazonBedrockBaseClient client : clientsCache.values()) {
                client.close();
            }

            // clear previously cached clients, they will be build lazily
            clientsCache.clear();
        } finally {
            cacheLock.writeLock().unlock();
        }
    }

    // used for testing
    int clientCount() {
        cacheLock.readLock().lock();
        try {
            return clientsCache.size();
        } finally {
            cacheLock.readLock().unlock();
        }
    }

    // used for testing
    void setClock(Clock newClock) {
        this.clock = Objects.requireNonNull(newClock);
    }
}
