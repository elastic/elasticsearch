/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.datasources.spi.StorageProvider;
import org.elasticsearch.xpack.esql.datasources.spi.StorageProviderFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Registry for StorageProvider implementations, keyed by URI scheme.
 * Allows pluggable discovery of storage providers based on the scheme
 * portion of a StoragePath (e.g., "http", "https", "s3", "file").
 *
 * <p>Providers are created lazily on first access rather than eagerly at startup,
 * so heavy dependencies (S3 client, HTTP client, etc.) are only loaded when
 * an EXTERNAL query actually targets that backend.
 *
 * <p>All providers are automatically wrapped with concurrency limiting and retry
 * logic for transient storage failures (503, 429, connection resets, timeouts)
 * unless the scheme is "file" (local filesystem). Wrap order:
 * {@code caller → Retryable(with adaptive backoff) → ConcurrencyLimited → raw provider}
 *
 * <p>Concurrency limiters and adaptive backoff state are shared per-scheme across
 * all providers (including per-query config providers), because cloud API rate limits
 * are per account/IP, not per client instance.
 *
 * <p>Registration methods are intended for single-threaded initialization only
 * (called from the {@link DataSourceModule} constructor).
 *
 * <p>This registry implements Closeable to properly close all created providers
 * when the registry is no longer needed.
 */
public class StorageProviderRegistry implements Closeable {
    private final Map<String, StorageProviderFactory> factories = new ConcurrentHashMap<>();
    private final Map<String, StorageProvider> providers = new ConcurrentHashMap<>();
    private final List<StorageProvider> createdProviders = new ArrayList<>();

    private final Map<String, ConcurrencyLimiter> limiters = new ConcurrentHashMap<>();
    private final Map<String, AdaptiveBackoff> backoffs = new ConcurrentHashMap<>();

    private final Settings settings;
    private volatile int maxConcurrentRequests;
    private volatile int throttleMaxRetryDurationSeconds;

    public StorageProviderRegistry(Settings settings) {
        this.settings = settings != null ? settings : Settings.EMPTY;
        this.maxConcurrentRequests = ExternalSourceSettings.MAX_CONCURRENT_REQUESTS.get(this.settings);
        this.throttleMaxRetryDurationSeconds = ExternalSourceSettings.THROTTLE_MAX_RETRY_DURATION.get(this.settings);
    }

    public void registerFactory(String scheme, StorageProviderFactory factory) {
        if (Strings.isNullOrEmpty(scheme)) {
            throw new IllegalArgumentException("Scheme cannot be null or empty");
        }
        if (factory == null) {
            throw new IllegalArgumentException("Factory cannot be null");
        }
        factories.put(scheme.toLowerCase(Locale.ROOT), factory);
    }

    public StorageProvider provider(StoragePath path) {
        if (path == null) {
            throw new IllegalArgumentException("Path cannot be null");
        }

        String scheme = path.scheme().toLowerCase(Locale.ROOT);
        StorageProvider provider = providers.get(scheme);
        if (provider == null) {
            provider = createDefaultProvider(scheme);
        }
        return provider;
    }

    public boolean hasProvider(String scheme) {
        if (Strings.isNullOrEmpty(scheme)) {
            return false;
        }
        String normalized = scheme.toLowerCase(Locale.ROOT);
        return factories.containsKey(normalized) || providers.containsKey(normalized);
    }

    public StorageProvider createProvider(String scheme, Settings settings, Map<String, Object> config) {
        String normalizedScheme = scheme.toLowerCase(Locale.ROOT);

        if (config == null || config.isEmpty()) {
            StorageProvider provider = providers.get(normalizedScheme);
            if (provider == null) {
                provider = createDefaultProvider(normalizedScheme);
            }
            return provider;
        }

        StorageProviderFactory factory = factories.get(normalizedScheme);
        if (factory == null) {
            throw new IllegalArgumentException("No SPI storage factory registered for scheme: " + scheme);
        }
        return wrapProvider(factory.create(settings, config), normalizedScheme);
    }

    private synchronized StorageProvider createDefaultProvider(String normalizedScheme) {
        StorageProvider provider = providers.get(normalizedScheme);
        if (provider != null) {
            return provider;
        }
        StorageProviderFactory factory = factories.get(normalizedScheme);
        if (factory == null) {
            throw new IllegalArgumentException("No storage provider registered for scheme: " + normalizedScheme);
        }
        provider = wrapProvider(factory.create(settings), normalizedScheme);
        providers.put(normalizedScheme, provider);
        createdProviders.add(provider);
        return provider;
    }

    private StorageProvider wrapProvider(StorageProvider provider, String scheme) {
        if ("file".equals(scheme)) {
            return provider;
        }
        ConcurrencyLimiter limiter = limiterForScheme(scheme);
        AdaptiveBackoff backoff = backoffForScheme(scheme);
        RetryPolicy retryPolicy = buildRetryPolicy(backoff);
        StorageProvider limited = new ConcurrencyLimitedStorageProvider(provider, limiter);
        return new RetryableStorageProvider(limited, retryPolicy);
    }

    private ConcurrencyLimiter limiterForScheme(String scheme) {
        return limiters.computeIfAbsent(scheme, k -> {
            int permits = maxConcurrentRequests;
            if (permits <= 0) {
                return ConcurrencyLimiter.UNLIMITED;
            }
            return new ConcurrencyLimiter(permits);
        });
    }

    private AdaptiveBackoff backoffForScheme(String scheme) {
        return backoffs.computeIfAbsent(scheme, k -> new AdaptiveBackoff());
    }

    private RetryPolicy buildRetryPolicy(AdaptiveBackoff backoff) {
        RetryPolicy policy = RetryPolicy.DEFAULT.withAdaptiveBackoff(backoff);
        if (throttleMaxRetryDurationSeconds > 0) {
            policy = policy.withTotalDurationBudget(throttleMaxRetryDurationSeconds * 1000L);
        }
        return policy;
    }

    @Override
    public void close() throws IOException {
        List<StorageProvider> toClose = new ArrayList<>(createdProviders);
        createdProviders.clear();
        providers.clear();
        IOUtils.close(toClose);
    }
}
