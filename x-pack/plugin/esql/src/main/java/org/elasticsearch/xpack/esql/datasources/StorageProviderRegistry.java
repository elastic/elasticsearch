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
import org.elasticsearch.xpack.esql.datasources.cache.StorageProviderCache;
import org.elasticsearch.xpack.esql.datasources.spi.ErrorPolicy;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.datasources.spi.StorageProvider;
import org.elasticsearch.xpack.esql.datasources.spi.StorageProviderFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
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
    private final Map<String, ConcurrencyBudgetAllocator> allocators = new ConcurrentHashMap<>();
    private final Map<String, AdaptiveBackoff> backoffs = new ConcurrentHashMap<>();

    // Cache for providers created with a non-empty config (WITH clause queries).
    // Avoids reconstructing cloud clients (S3, GCS, Azure) for repeated calls with the same config.
    private final StorageProviderCache configuredProviderCache = new StorageProviderCache();

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

    /**
     * Framework-level WITH keys that are consumed by {@link FileSourceFactory} / format readers
     * and must not be forwarded to storage provider configurations. References the canonical
     * constants so adding/renaming a framework option in one place updates the filter here too.
     */
    private static final Set<String> FRAMEWORK_KEYS = Set.of(
        FormatNameResolver.CONFIG_FORMAT,
        FormatNameResolver.CONFIG_READER,
        ErrorPolicy.CONFIG_MAX_ERRORS,
        ErrorPolicy.CONFIG_MAX_ERROR_RATIO,
        ErrorPolicy.CONFIG_ERROR_MODE
    );

    public StorageProvider createProvider(String scheme, Settings settings, Map<String, Object> config) {
        String normalizedScheme = scheme.toLowerCase(Locale.ROOT);

        Map<String, Object> storageConfig = stripFrameworkKeys(config);
        if (storageConfig == null || storageConfig.isEmpty()) {
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

        // Cache providers by (scheme, storageConfig) so queries with the same WITH-clause config
        // reuse the same cloud client and connection pool instead of constructing a new one.
        // The cache key uses the stripped config so framework-only keys (e.g. format) don't
        // produce spurious cache misses.
        StorageProviderCache.CacheKey cacheKey = new StorageProviderCache.CacheKey(normalizedScheme, storageConfig);
        try {
            return configuredProviderCache.getOrCreate(
                cacheKey,
                () -> wrapProvider(factory.create(settings, storageConfig), normalizedScheme)
            );
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            // The factory does not declare checked exceptions, so this path is unreachable.
            throw new RuntimeException("Unexpected checked exception from StorageProviderFactory", e);
        }
    }

    private static Map<String, Object> stripFrameworkKeys(Map<String, Object> config) {
        if (config == null || config.isEmpty()) {
            return config;
        }
        boolean hasFrameworkKeys = config.keySet().stream().anyMatch(FRAMEWORK_KEYS::contains);
        if (hasFrameworkKeys == false) {
            return config;
        }
        Map<String, Object> filtered = new HashMap<>(config);
        FRAMEWORK_KEYS.forEach(filtered::remove);
        return filtered;
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

    /**
     * Returns a per-query concurrency budget allocator for the given scheme, or {@code null}
     * if per-query budgeting is not applicable (file scheme or concurrency limiting disabled).
     */
    public ConcurrencyBudgetAllocator allocatorForScheme(String scheme) {
        if ("file".equals(scheme)) {
            return null;
        }
        int permits = maxConcurrentRequests;
        if (permits <= 0) {
            return null;
        }
        return allocators.computeIfAbsent(scheme, k -> new ConcurrencyBudgetAllocator(permits));
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
        // Close default (zero-config) providers first, then the config-keyed cache.
        IOUtils.close(toClose);
        configuredProviderCache.close();
    }
}
