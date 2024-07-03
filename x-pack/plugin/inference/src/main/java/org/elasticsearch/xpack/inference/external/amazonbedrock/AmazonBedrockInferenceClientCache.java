/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.amazonbedrock;

import com.amazonaws.http.IdleConnectionReaper;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xpack.inference.services.amazonbedrock.AmazonBedrockModel;
import org.joda.time.Instant;

import java.io.IOException;
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

    public AmazonBedrockInferenceClientCache(BiFunction<AmazonBedrockModel, TimeValue, AmazonBedrockBaseClient> creator) {
        this.creator = Objects.requireNonNull(creator);
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
            if (client != null) {
                client.resetExpiration();
                return client;
            }
        }

        cacheLock.readLock().lock();
        try {
            final AmazonBedrockBaseClient existing = clientsCache.get(modelHash);
            if (existing != null) {
                existing.resetExpiration();
                return existing;
            }

            cacheLock.readLock().unlock();
            cacheLock.writeLock().lock();
            try {
                final AmazonBedrockBaseClient builtClient = creator.apply(model, timeout);

                clientsCache.put(modelHash, builtClient);
                return builtClient;
            } finally {
                // Downgrade by acquiring read lock before releasing write lock
                // see : https://docs.oracle.com/javase/8/docs/api/java/util/concurrent/locks/ReentrantReadWriteLock.html
                cacheLock.readLock().lock();
                cacheLock.writeLock().unlock();
            }
        } finally {
            cacheLock.readLock().unlock();
        }
    }

    private void flushExpiredClients() {
        var currentTimestampMs = new Instant();
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

        // shutdown IdleConnectionReaper background thread
        // it will be restarted on new client usage
        IdleConnectionReaper.shutdown();
    }

    // used for testing
    public int clientCount() {
        cacheLock.readLock().lock();
        try {
            return clientsCache.size();
        } finally {
            cacheLock.readLock().unlock();
        }
    }
}
