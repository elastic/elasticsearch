/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.telemetry.InstrumentType;
import org.elasticsearch.telemetry.Measurement;
import org.elasticsearch.telemetry.RecordingMeterRegistry;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasources.cache.StorageProviderCache;
import org.elasticsearch.xpack.esql.datasources.spi.Configured;
import org.elasticsearch.xpack.esql.datasources.spi.CredentialPolicy;
import org.elasticsearch.xpack.esql.datasources.spi.DataSourceConfigDefinition;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalSourceMetrics;
import org.elasticsearch.xpack.esql.datasources.spi.FileDataSourceConfiguration;
import org.elasticsearch.xpack.esql.datasources.spi.FileDataSourceValidator;
import org.elasticsearch.xpack.esql.datasources.spi.StorageChildren;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.datasources.spi.StorageProvider;
import org.elasticsearch.xpack.esql.datasources.spi.StorageProviderFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

/**
 * Unit tests for {@link StorageProviderRegistry}: wrap-order, throttle-scope, and the {@code file://}
 * local-disk gate.
 */
public class StorageProviderRegistryTests extends ESTestCase {

    /**
     * Non-file providers are wrapped {@code Retryable → ConcurrencyLimited → raw}: the retry/backoff layer is
     * outermost and the per-scheme permit semaphore sits directly above the raw provider. The outermost wrapper the
     * caller sees must therefore be {@link RetryableStorageProvider}.
     */
    public void testWrapsWithRetryable() {
        try (StorageProviderRegistry registry = new StorageProviderRegistry(Settings.EMPTY)) {
            registry.registerFactory("stub", StorageProviderFactory.noConfigKeys(StubStorageProvider::new));
            StorageProvider wrapped = registry.provider(StoragePath.of("stub://bucket/file.csv"));
            assertThat(wrapped, org.hamcrest.Matchers.instanceOf(RetryableStorageProvider.class));
        } catch (IOException e) {
            throw new AssertionError(e);
        }
    }

    /**
     * Covers the registry's wrapping ({@code Retryable -> ConcurrencyLimited -> leaf}) plus a
     * {@link QueryBudgetedStorageProvider} built by hand over it, with a {@link RangeStorageObject} view on top.
     * That hand-built wrap mirrors {@code FileSourceFactory#operatorFactory()}, but this test does not exercise
     * that wiring; a change that drops or reorders the real wrap would not fail here. Attaching metrics on the
     * outermost object must reach the metered leaf. A decorator that does not forward {@code attachMetrics} stops
     * the attach at that layer, so no {@code storage.*} metric is published for that remote scheme.
     */
    public void testAttachMetricsReachesMeteredLeafThroughProductionChain() throws Exception {
        RecordingMeterRegistry meters = new RecordingMeterRegistry();
        byte[] data = "hello".getBytes(StandardCharsets.UTF_8);
        StoragePath path = StoragePath.of("s3://bucket/file.csv");
        Settings settings = Settings.builder().put(ExternalSourceSettings.MAX_CONCURRENT_REQUESTS.getKey(), 4).build();
        try (StorageProviderRegistry registry = new StorageProviderRegistry(settings)) {
            StorageObject leaf = TestStorageObjects.meteredLeaf(path, data);
            registry.registerFactory("s3", StorageProviderFactory.noConfigKeys(() -> new MeteredStubStorageProvider(leaf)));
            ConcurrencyBudgetAllocator allocator = registry.allocatorForScheme("s3");
            assertNotNull("explicit permits must yield a per-query allocator", allocator);
            QueryBudgetedStorageProvider budgeted = new QueryBudgetedStorageProvider(registry.provider(path), allocator.register());
            // Operators read through a split's RangeStorageObject view (FileSplitProvider#storageObjectForSplit), so the
            // outermost layer here is the range view, as in production.
            StorageObject obj = new RangeStorageObject(budgeted.newObject(path, data.length), 0, data.length);

            obj.attachMetrics(new ExternalSourceMetrics(meters), "s3");
            try (InputStream stream = obj.newStream(0, data.length)) {
                assertArrayEquals(data, stream.readAllBytes());
            }

            Measurement requests = TestStorageObjects.singleMeasurement(
                meters,
                InstrumentType.LONG_COUNTER,
                ExternalSourceMetrics.STORAGE_REQUESTS_TOTAL
            );
            assertEquals(1L, requests.getLong());
            assertEquals("s3", requests.attributes().get(ExternalSourceMetrics.TYPE_ATTRIBUTE));
        }
    }

    public void testLimiterForSchemeCarriesSchemeAndRaisableCarrier() {
        Settings settings = Settings.builder().put("esql.external.max_concurrent_requests", 2).build();
        try (StorageProviderRegistry registry = new StorageProviderRegistry(settings)) {
            ConcurrencyLimiter limiter = registry.limiterForScheme("gs");
            assertEquals("gs", limiter.scheme());
            assertEquals(2, limiter.maxPermits());
            assertTrue(limiter.settingCanRaiseLimit());
        } catch (IOException e) {
            throw new AssertionError(e);
        }
    }

    public void testLimiterForSchemeExplicitMaxIsUnraisable() {
        Settings settings = Settings.builder().put("esql.external.max_concurrent_requests", 500).build();
        ExternalSourceSettings.BlobStoreConcurrency expected = ExternalSourceSettings.blobStoreConcurrencyInfo(settings);
        try (StorageProviderRegistry registry = new StorageProviderRegistry(settings)) {
            ConcurrencyLimiter limiter = registry.limiterForScheme("gs");
            assertEquals("gs", limiter.scheme());
            assertEquals(expected.permits(), limiter.maxPermits());
            assertFalse(limiter.settingCanRaiseLimit());
        } catch (IOException e) {
            throw new AssertionError(e);
        }
    }

    public void testFrameworkOnlyWithDoesNotCloseDefault() {
        AtomicInteger closes = new AtomicInteger();
        try (StorageProviderRegistry registry = new StorageProviderRegistry(Settings.EMPTY)) {
            registry.registerFactory("stub", StorageProviderFactory.noConfigKeys(() -> new CountingStub(closes)));
            StoragePath path = StoragePath.of("stub://bucket/file.csv");
            StorageProvider def = registry.provider(path);
            StorageProvider fromWith = registry.createProvider("stub", Settings.EMPTY, Map.of("format", "parquet"));
            assertFalse(StorageProviderCache.isPooledLease(fromWith));
            assertSame(def, fromWith);
            StorageProviderCache.closeLease(fromWith);
            assertEquals("framework-only WITH must not true-close the scheme default", 0, closes.get());
        } catch (IOException e) {
            throw new AssertionError(e);
        }
        assertEquals(1, closes.get());
    }

    public void testStorageConfigCreateProviderReturnsPooledLease() {
        AtomicInteger closes = new AtomicInteger();
        try (StorageProviderRegistry registry = new StorageProviderRegistry(Settings.EMPTY)) {
            registry.registerFactory("stub", StorageProviderFactory.noConfigKeys(() -> new CountingStub(closes)));
            StorageProvider leased = registry.createProvider("stub", Settings.EMPTY, Map.of("region", "us-east-1"));
            assertTrue(StorageProviderCache.isPooledLease(leased));
            StorageProviderCache.closeLease(leased);
            assertEquals("returning a lease must not true-close the client", 0, closes.get());
        } catch (IOException e) {
            throw new AssertionError(e);
        }
        assertEquals(1, closes.get());
    }

    /**
     * Federated identity is gated at provider build on the resolved configuration, not on the {@code auth} string: an
     * explicit {@code auth=federated_identity} and an {@code auto} config carrying a federated field are both refused
     * while the setting is off, before any provider is constructed, and both build once it is on. The gate rides the
     * {@link CredentialPolicy} the plugin receives, applied by {@code StorageProviderFactory.of(..., policy)}.
     */
    public void testFederatedIdentityRefusedAtProviderBuildWhenDisabled() {
        AtomicInteger built = new AtomicInteger();
        AtomicBoolean federatedEnabled = new AtomicBoolean(false);
        CredentialPolicy policy = CredentialPolicy.of(() -> true, federatedEnabled::get);
        try (StorageProviderRegistry registry = new StorageProviderRegistry(Settings.EMPTY)) {
            registry.registerFactory(
                "stub",
                StorageProviderFactory.of(StubStorageProvider::new, StubConfiguration::fromQueryConfig, cfg -> {
                    built.incrementAndGet();
                    return new StubStorageProvider();
                }, policy)
            );

            for (Map<String, Object> federated : List.of(
                Map.<String, Object>of("auth", "federated_identity", "role_arn", "arn:aws:iam::123456789012:role/reader"),
                Map.<String, Object>of("role_arn", "arn:aws:iam::123456789012:role/reader")
            )) {
                IllegalArgumentException e = expectThrows(
                    IllegalArgumentException.class,
                    () -> registry.createProvider("stub", Settings.EMPTY, federated)
                );
                assertThat(e.getMessage(), equalTo(FileDataSourceValidator.FEDERATED_IDENTITY_DISABLED_MESSAGE));
            }
            assertEquals("a refused mode must not construct a provider", 0, built.get());

            // Static credentials are unaffected by the federated gate.
            StorageProviderCache.closeLease(
                registry.createProvider("stub", Settings.EMPTY, Map.of("access_key", "AKIA", "secret_key", "s3cr3t"))
            );
            assertEquals(1, built.get());

            federatedEnabled.set(true);
            StorageProviderCache.closeLease(
                registry.createProvider("stub", Settings.EMPTY, Map.of("auth", "federated_identity", "role_arn", "arn:aws:iam::1:role/r"))
            );
            assertEquals(2, built.get());
        } catch (IOException e) {
            throw new AssertionError(e);
        }
    }

    /** The policy also carries the managed-identity gate, for the typed check that mirrors the PUT validator. */
    public void testManagedIdentityRefusedByPolicyWhenDisabled() {
        AtomicInteger built = new AtomicInteger();
        CredentialPolicy policy = CredentialPolicy.of(() -> false, () -> true);
        try (StorageProviderRegistry registry = new StorageProviderRegistry(Settings.EMPTY)) {
            registry.registerFactory(
                "stub",
                StorageProviderFactory.of(StubStorageProvider::new, StubConfiguration::fromQueryConfig, cfg -> {
                    built.incrementAndGet();
                    return new StubStorageProvider();
                }, policy)
            );
            IllegalArgumentException e = expectThrows(
                IllegalArgumentException.class,
                () -> registry.createProvider("stub", Settings.EMPTY, Map.of("auth", "managed_identity", "region", "us-east-1"))
            );
            assertThat(e.getMessage(), equalTo(FileDataSourceConfiguration.MANAGED_IDENTITY_DISABLED_MESSAGE));
            assertEquals(0, built.get());
        } catch (IOException e) {
            throw new AssertionError(e);
        }
    }

    /** Minimal file-type configuration with one static credential pair and one federated field. */
    private static final class StubConfiguration extends FileDataSourceConfiguration {
        private static final DataSourceConfigDefinition ACCESS_KEY = DataSourceConfigDefinition.secret("access_key");
        private static final DataSourceConfigDefinition SECRET_KEY = DataSourceConfigDefinition.secret("secret_key");
        private static final DataSourceConfigDefinition REGION = DataSourceConfigDefinition.plaintext("region");
        private static final DataSourceConfigDefinition ROLE_ARN = DataSourceConfigDefinition.plaintext("role_arn").asFederatedAuth();
        private static final Map<String, DataSourceConfigDefinition> FIELDS = DataSourceConfigDefinition.mapOf(
            AUTH,
            ACCESS_KEY,
            SECRET_KEY,
            REGION,
            ROLE_ARN
        );

        private StubConfiguration(Map<String, Object> raw) {
            super(raw, FIELDS);
        }

        static Configured<StubConfiguration> fromQueryConfig(Map<String, Object> raw) {
            return filterAndConstruct(raw, FIELDS, StubConfiguration::new);
        }

        @Override
        public boolean hasCredentials() {
            return get("access_key") != null && get("secret_key") != null;
        }

        @Override
        public String unresolvedAuthMessage() {
            return "stub requires credentials or role_arn";
        }
    }

    /**
     * Stub whose {@link #newObject(StoragePath, long)} hands back a real metered leaf, so the registry's wrapping chain is
     * exercised end to end instead of stopping at the stub's throwing defaults.
     */
    private static class MeteredStubStorageProvider extends StubStorageProvider {
        private final StorageObject leaf;

        MeteredStubStorageProvider(StorageObject leaf) {
            this.leaf = leaf;
        }

        @Override
        public StorageObject newObject(StoragePath path, long length) {
            return leaf;
        }
    }

    /** Minimal no-op storage provider; only the scheme + lifecycle matter for the wrap-order assertion. */
    private static class StubStorageProvider implements StorageProvider {
        @Override
        public StorageChildren listChildren(StoragePath prefix, int limit) {
            return null; // directory-aware listing is irrelevant to this test double
        }

        @Override
        public StorageObject newObject(StoragePath path) {
            throw new UnsupportedOperationException();
        }

        @Override
        public StorageObject newObject(StoragePath path, long length) {
            throw new UnsupportedOperationException();
        }

        @Override
        public StorageObject newObject(StoragePath path, long length, Instant lastModified) {
            throw new UnsupportedOperationException();
        }

        @Override
        public StorageIterator listObjects(StoragePath prefix, boolean recursive) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean exists(StoragePath path) {
            throw new UnsupportedOperationException();
        }

        @Override
        public List<String> supportedSchemes() {
            return List.of("stub");
        }

        @Override
        public void close() {}
    }

    private static final class CountingStub extends StubStorageProvider {
        private final AtomicInteger closes;

        CountingStub(AtomicInteger closes) {
            this.closes = closes;
        }

        @Override
        public void close() {
            closes.incrementAndGet();
        }
    }

    public void testThrottleScopeIsPerBucketNotPerScheme() {
        // The adaptive-backoff scope is the store's hot unit (per-bucket), not per-scheme: two buckets on the same
        // scheme must get distinct scopes so a hot bucket backs off only its own traffic, while every object in one
        // bucket shares one scope.
        String bucketA = StorageProviderRegistry.throttleScope(StoragePath.of("s3://bucket-a/data/part-0.csv"));
        String bucketASibling = StorageProviderRegistry.throttleScope(StoragePath.of("s3://bucket-a/other/part-9.csv"));
        String bucketB = StorageProviderRegistry.throttleScope(StoragePath.of("s3://bucket-b/data/part-0.csv"));

        assertEquals("same bucket, different keys -> same throttle scope", bucketA, bucketASibling);
        assertNotEquals("different buckets -> different throttle scope (no cross-bucket backoff bleed)", bucketA, bucketB);
        assertEquals("s3://bucket-a", bucketA);
    }

    public void testThrottleScopePerStore() {
        assertEquals("gs://gbucket", StorageProviderRegistry.throttleScope(StoragePath.of("gs://gbucket/prefix/f.csv")));
        // The local filesystem has no host; it collapses to one scope (it never throttles, so the backoff is inert).
        assertEquals("file://", StorageProviderRegistry.throttleScope(StoragePath.of("file:///tmp/data/f.csv")));
    }

    // ---- file:// local-disk gate tests -------------------------------------------------------

    // A no-op StorageProviderFactory / StorageProvider for the file scheme used by gate tests.
    private static final StorageProvider NOOP_PROVIDER = new StorageProvider() {
        @Override
        public StorageChildren listChildren(StoragePath prefix, int limit) {
            return null; // directory-aware listing is irrelevant to this test double
        }

        @Override
        public StorageObject newObject(StoragePath path) {
            throw new UnsupportedOperationException("noop");
        }

        @Override
        public StorageObject newObject(StoragePath path, long length) {
            throw new UnsupportedOperationException("noop");
        }

        @Override
        public StorageObject newObject(StoragePath path, long length, Instant lastModified) {
            throw new UnsupportedOperationException("noop");
        }

        @Override
        public StorageIterator listObjects(StoragePath prefix, boolean recursive) {
            throw new UnsupportedOperationException("noop");
        }

        @Override
        public boolean exists(StoragePath path) {
            return true;
        }

        @Override
        public List<String> supportedSchemes() {
            return List.of("file");
        }

        @Override
        public void close() {}
    };

    private static final StorageProviderFactory NOOP_FACTORY = StorageProviderFactory.noConfigKeys(() -> NOOP_PROVIDER);

    private StorageProviderRegistry registryWithFileAccess(LocalFileAccess access) {
        StorageProviderRegistry registry = new StorageProviderRegistry(
            Settings.EMPTY,
            null,
            () -> false,  // workloadIdentityEnabled — irrelevant for these tests
            RetryScheduler.DIRECT,
            access
        );
        registry.registerFactory("file", NOOP_FACTORY);
        return registry;
    }

    // --- provider(StoragePath) — disabled gate ---

    public void testProviderRejectsFileWhenDisabled() {
        StorageProviderRegistry registry = registryWithFileAccess(
            LocalFileAccess.create(Settings.EMPTY)  // empty allowlist → disabled
        );
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> registry.provider(StoragePath.of("file:///etc/passwd"))
        );
        assertThat(e.getMessage(), containsString("esql.external.local_allowed_paths"));
    }

    public void testProviderAllowsFileWhenUnderRoot() throws IOException {
        Path allowed = createTempDir();
        Settings settings = Settings.builder().putList("esql.external.local_allowed_paths", allowed.toString()).build();
        StorageProviderRegistry registry = registryWithFileAccess(LocalFileAccess.create(settings));

        Path file = allowed.resolve("data.parquet");
        Files.createFile(file);
        // Should not throw
        StorageProvider provider = registry.provider(StoragePath.of("file://" + file.toAbsolutePath()));
        assertNotNull(provider);
    }

    public void testProviderRejectsFileOutsideRoot() throws IOException {
        Path allowed = createTempDir();
        Path outside = createTempDir();
        Settings settings = Settings.builder().putList("esql.external.local_allowed_paths", allowed.toString()).build();
        StorageProviderRegistry registry = registryWithFileAccess(LocalFileAccess.create(settings));

        Path file = outside.resolve("secret.csv");
        Files.createFile(file);
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> registry.provider(StoragePath.of("file://" + file.toAbsolutePath()))
        );
        assertThat(e.getMessage(), containsString("esql.external.local_allowed_paths"));
    }

    public void testProviderRejectsDotDotEscape() throws IOException {
        Path allowed = createTempDir();
        Path sibling = createTempDir();
        Settings settings = Settings.builder().putList("esql.external.local_allowed_paths", allowed.toString()).build();
        StorageProviderRegistry registry = registryWithFileAccess(LocalFileAccess.create(settings));

        String traversal = allowed.toAbsolutePath() + "/../" + sibling.getFileName() + "/secret.csv";
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> registry.provider(StoragePath.of("file://" + traversal))
        );
        assertThat(e.getMessage(), containsString("esql.external.local_allowed_paths"));
    }

    // --- createProviderTrackingConsumedKeys — scheme-level disabled reject ---

    public void testCreateProviderRejectsFileWhenDisabled() {
        StorageProviderRegistry registry = registryWithFileAccess(LocalFileAccess.create(Settings.EMPTY));
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> registry.createProviderTrackingConsumedKeys("file", Settings.EMPTY, java.util.Map.of())
        );
        assertThat(e.getMessage(), containsString("esql.external.local_allowed_paths"));
    }

    public void testCreateProviderAllowsFileWhenEnabled() throws IOException {
        Path allowed = createTempDir();
        Settings settings = Settings.builder().putList("esql.external.local_allowed_paths", allowed.toString()).build();
        StorageProviderRegistry registry = registryWithFileAccess(LocalFileAccess.create(settings));

        // Empty config — should proceed to the default provider without throwing
        Configured<StorageProvider> result = registry.createProviderTrackingConsumedKeys("file", Settings.EMPTY, java.util.Map.of());
        assertNotNull(result);
    }

    // --- Non-file schemes pass through the gate without rejection ---

    public void testProviderDoesNotRejectNonFileScheme() {
        LocalFileAccess disabledAccess = LocalFileAccess.create(Settings.EMPTY);
        // Should not throw for non-file schemes
        disabledAccess.check(StoragePath.of("s3://bucket/key.parquet"));
    }
}
