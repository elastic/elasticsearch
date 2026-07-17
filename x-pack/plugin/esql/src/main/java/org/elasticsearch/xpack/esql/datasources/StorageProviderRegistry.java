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
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.esql.datasources.cache.StorageProviderCache;
import org.elasticsearch.xpack.esql.datasources.spi.Configured;
import org.elasticsearch.xpack.esql.datasources.spi.ErrorPolicy;
import org.elasticsearch.xpack.esql.datasources.spi.FileDataSourceConfiguration;
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
import java.util.function.BooleanSupplier;

/**
 * Registry for StorageProvider implementations, keyed by URI scheme.
 * Allows pluggable discovery of storage providers based on the scheme
 * portion of a StoragePath (e.g., "http", "https", "s3", "file").
 *
 * <p>Providers are created lazily on first access rather than eagerly at startup,
 * so heavy dependencies (S3 client, HTTP client, etc.) are only loaded when
 * an EXTERNAL query actually targets that backend.
 *
 * <p>All non-file providers are automatically wrapped with per-scheme concurrency limiting and retry logic for
 * transient storage failures (503, 429, connection resets, timeouts). Wrap order:
 * {@code caller → Retryable(with adaptive backoff) → ConcurrencyLimited → raw provider}
 *
 * <p>Concurrency limiters are shared per-scheme and adaptive backoff state is shared per-throttle-scope across all
 * providers (including per-query config providers), because cloud API rate limits are per account/IP, not per client
 * instance.
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

    private final Map<String, RetryPolicy> scopedPolicies = new ConcurrentHashMap<>();
    // Per-scheme in-flight-read permit semaphores and their per-query budget allocators. Shared per-scheme across
    // all providers (including per-query config providers): cloud API rate limits are per account/IP, not per client.
    private final Map<String, ConcurrencyLimiter> limiters = new ConcurrentHashMap<>();
    private final Map<String, ConcurrencyBudgetAllocator> allocators = new ConcurrentHashMap<>();

    // Cache for providers created with a non-empty per-query configuration map.
    // Avoids reconstructing cloud clients (S3, GCS, Azure) for repeated calls with the same config.
    private final StorageProviderCache configuredProviderCache = new StorageProviderCache();

    private final Settings settings;
    private final BooleanSupplier managedIdentityEnabled;
    /** Decrypts data-source secrets at the single provider-build chokepoint; {@code null} in tests with no encryption. */
    @Nullable
    private final DataSourceCredentials credentials;
    private final int throttleMaxRetryDurationSeconds;
    /** Per-node in-flight-read permit count sizing each per-scheme {@link ConcurrencyLimiter}; 0 disables limiting. */
    private final int maxConcurrentRequests;
    /** Schedules async read-retry continuations off a timer; {@code DIRECT} (no ThreadPool) in tests. */
    private final RetryScheduler retryScheduler;
    /**
     * Gate for {@code file://} local-disk reads. Defaults to {@link LocalFileAccess#UNRESTRICTED} in
     * test-only constructors; production always goes through the five-argument constructor via {@code DataSourceModule}.
     */
    private final LocalFileAccess localFileAccess;

    public StorageProviderRegistry(Settings settings) {
        this(settings, null);
    }

    /**
     * Test-only convenience constructor. The default {@code managedIdentityEnabled} supplier reads the cluster
     * setting directly and does <b>not</b> apply the stateless gate that production wiring enforces in
     * {@code EsqlPlugin} (where the boolean is forced to {@code false} when {@code DiscoveryNode.isStateless}).
     * Similarly, {@code localFileAccess} defaults to {@link LocalFileAccess#UNRESTRICTED} and does <b>not</b>
     * apply the stateless gate or the allowlist from {@code ExternalSourceSettings#LOCAL_ALLOWED_PATHS}.
     * Production always goes through the five-argument constructor via {@code DataSourceModule}.
     */
    public StorageProviderRegistry(Settings settings, @Nullable DataSourceCredentials credentials) {
        this(
            settings,
            credentials,
            () -> ExternalSourceSettings.MANAGED_IDENTITY_ENABLED.get(settings != null ? settings : Settings.EMPTY),
            RetryScheduler.DIRECT,
            LocalFileAccess.UNRESTRICTED
        );
    }

    public StorageProviderRegistry(
        Settings settings,
        @Nullable DataSourceCredentials credentials,
        BooleanSupplier managedIdentityEnabled,
        RetryScheduler retryScheduler
    ) {
        this(settings, credentials, managedIdentityEnabled, retryScheduler, LocalFileAccess.UNRESTRICTED);
    }

    public StorageProviderRegistry(
        Settings settings,
        @Nullable DataSourceCredentials credentials,
        BooleanSupplier managedIdentityEnabled,
        RetryScheduler retryScheduler,
        LocalFileAccess localFileAccess
    ) {
        this.settings = settings != null ? settings : Settings.EMPTY;
        this.credentials = credentials;
        this.managedIdentityEnabled = managedIdentityEnabled;
        this.retryScheduler = retryScheduler != null ? retryScheduler : RetryScheduler.DIRECT;
        this.throttleMaxRetryDurationSeconds = ExternalSourceSettings.THROTTLE_MAX_RETRY_DURATION.get(this.settings);
        this.localFileAccess = localFileAccess != null ? localFileAccess : LocalFileAccess.UNRESTRICTED;
        this.maxConcurrentRequests = ExternalSourceSettings.blobStoreConcurrency(this.settings);
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

        // Defense-in-depth: validate file:// access (disabled gate or path outside allowlist) before
        // returning the provider. This covers the bare-read data-node path and coordinator resolveMetadata
        // with an empty config map, both of which bypass createProviderTrackingConsumedKeys.
        localFileAccess.check(path);

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
    static final Set<String> FRAMEWORK_KEYS = Set.of(
        FormatNameResolver.CONFIG_FORMAT,
        FormatNameResolver.CONFIG_READER,
        ErrorPolicy.CONFIG_MAX_ERRORS,
        ErrorPolicy.CONFIG_MAX_ERROR_RATIO,
        ErrorPolicy.CONFIG_ERROR_MODE
    );

    /**
     * Convenience: returns the StorageProvider only.
     * Use {@link #createProviderTrackingConsumedKeys(String, Settings, Map)} when the consumed-keys set is needed.
     */
    public StorageProvider createProvider(String scheme, Settings settings, Map<String, Object> config) {
        return createProviderTrackingConsumedKeys(scheme, settings, config).value();
    }

    public Configured<StorageProvider> createProviderTrackingConsumedKeys(String scheme, Settings settings, Map<String, Object> config) {
        String normalizedScheme = scheme.toLowerCase(Locale.ROOT);

        // Gate file:// on the local-disk allowlist before any config work. This covers the inline-WITH path
        // on data nodes where no coordinator-side validateConfig runs. The path-aware check already fired at
        // planning time in FileSourceFactory.validateConfig; this is defense-in-depth for the scheme.
        if (normalizedScheme.equals("file") && localFileAccess.enabled() == false) {
            throw new IllegalArgumentException(LocalFileAccess.LOCAL_DISK_DISABLED_MESSAGE);
        }

        // Flatten the _datasource sub-map and decrypt any encrypted secrets here, so every provider
        // construction path gets plaintext credentials regardless of how it assembled its config.
        config = ExternalSourceResolver.storageConfig(config);
        if (credentials != null) {
            config = credentials.decryptInPlace(config);
        }
        Map<String, Object> storageConfig = stripFrameworkKeys(config);
        if (storageConfig == null || storageConfig.isEmpty()) {
            StorageProvider provider = providers.get(normalizedScheme);
            if (provider == null) {
                provider = createDefaultProvider(normalizedScheme);
            }
            return Configured.empty(provider);
        }

        StorageProviderFactory factory = factories.get(normalizedScheme);
        if (factory == null) {
            throw new IllegalArgumentException("No SPI storage factory registered for scheme: " + scheme);
        }

        // Gate auth=managed_identity on the cluster setting before constructing the provider. This covers the
        // inline-WITH path where no PUT-datasource validation runs.
        if (FileDataSourceConfiguration.isManagedIdentityAuth(storageConfig.get("auth"))
            && managedIdentityEnabled.getAsBoolean() == false) {
            throw new IllegalArgumentException(FileDataSourceConfiguration.MANAGED_IDENTITY_DISABLED_MESSAGE);
        }

        // Cache providers by (scheme, storageConfig) so queries with the same configuration map
        // reuse the same cloud client and connection pool instead of constructing a new one.
        // The cache key uses the stripped config so framework-only keys (e.g. format) don't
        // produce spurious cache misses.
        StorageProviderCache.CacheKey cacheKey = new StorageProviderCache.CacheKey(normalizedScheme, storageConfig);
        try {
            return configuredProviderCache.getOrCreate(cacheKey, () -> {
                Configured<StorageProvider> raw = factory.createTrackingConsumedKeys(settings, storageConfig);
                return new Configured<>(wrapProvider(raw.value(), normalizedScheme), raw.consumedKeys());
            });
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
        // Wrap order: caller -> Retryable(per-scope adaptive backoff) -> ConcurrencyLimited(per-scheme permits) -> raw.
        // The permit semaphore bounds in-flight reads per scheme; file:// is exempt (local reads are not rate-limited
        // and raise plain IOExceptions, never the throttling-typed ExternalUnavailableException the retry layer acts
        // on). The adaptive backoff is selected per throttle scope (per-bucket/account) at read time, so a hot bucket
        // backs off only its own traffic, not every read on the same store.
        StorageProvider bounded = "file".equals(scheme)
            ? provider
            : new ConcurrencyLimitedStorageProvider(provider, limiterForScheme(scheme));
        return new RetryableStorageProvider(bounded, retryScheduler, this::retryPolicyForScope);
    }

    /**
     * Per-query concurrency budget allocator for the given scheme, or {@code null} when per-query budgeting does not
     * apply (file scheme, or permit limiting disabled via {@code max_concurrent_requests=0}). Shared per-scheme so a
     * single query cannot starve others on the same backend.
     */
    public ConcurrencyBudgetAllocator allocatorForScheme(String scheme) {
        if ("file".equals(scheme) || maxConcurrentRequests <= 0) {
            return null;
        }
        return allocators.computeIfAbsent(scheme, k -> new ConcurrencyBudgetAllocator(maxConcurrentRequests));
    }

    private ConcurrencyLimiter limiterForScheme(String scheme) {
        return limiters.computeIfAbsent(
            scheme,
            k -> maxConcurrentRequests <= 0 ? ConcurrencyLimiter.UNLIMITED : new ConcurrencyLimiter(maxConcurrentRequests)
        );
    }

    private RetryPolicy retryPolicyForScope(StoragePath path) {
        // One RetryPolicy per distinct throttle scope (bucket/account), computed once and reused across every read
        // in that scope. Each carries its own AdaptiveBackoff, so a hot scope backs off only its own traffic. The
        // map is bounded by the number of distinct buckets the node ever reads; each entry is small and left
        // unpruned.
        return scopedPolicies.computeIfAbsent(throttleScope(path), k -> buildRetryPolicy().withAdaptiveBackoff(new AdaptiveBackoff()));
    }

    /**
     * The throttle-scope key for adaptive backoff: the store's own hot unit, derived generically from the path as
     * {@code scheme://host[:port]} — per-bucket for S3/GCS, per-account for Azure (the host carries the bucket or
     * the {@code account.blob.core.windows.net}; Azure throttles per account, not per container). A hot scope backs
     * off only its own traffic. The host is lowercased (DNS is case-insensitive) so the same bucket maps to one
     * scope; the port is included so two custom endpoints on the same host but different ports stay isolated.
     * {@code userInfo} is deliberately excluded: it can carry credentials (which must never be retained in this
     * long-lived map) and is not a throttle axis. Finer per-prefix granularity for S3 is the deferred per-store SPI
     * refinement.
     */
    static String throttleScope(StoragePath path) {
        StringBuilder sb = new StringBuilder(path.scheme()).append("://");
        if (path.host() != null) {
            sb.append(path.host().toLowerCase(Locale.ROOT));
        }
        if (path.port() >= 0) {
            sb.append(':').append(path.port());
        }
        return sb.toString();
    }

    private RetryPolicy buildRetryPolicy() {
        RetryPolicy policy = RetryPolicy.DEFAULT;
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
