/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasources.spi.Configured;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.datasources.spi.StorageProvider;
import org.elasticsearch.xpack.esql.datasources.spi.StorageProviderFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;

import static org.hamcrest.Matchers.containsString;

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

    /** Minimal no-op storage provider; only the scheme + lifecycle matter for the wrap-order assertion. */
    private static final class StubStorageProvider implements StorageProvider {
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
        assertThat(e.getMessage(), containsString("esql.datasource.local_allowed_paths"));
    }

    public void testProviderAllowsFileWhenUnderRoot() throws IOException {
        Path allowed = createTempDir();
        Settings settings = Settings.builder().putList("esql.datasource.local_allowed_paths", allowed.toString()).build();
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
        Settings settings = Settings.builder().putList("esql.datasource.local_allowed_paths", allowed.toString()).build();
        StorageProviderRegistry registry = registryWithFileAccess(LocalFileAccess.create(settings));

        Path file = outside.resolve("secret.csv");
        Files.createFile(file);
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> registry.provider(StoragePath.of("file://" + file.toAbsolutePath()))
        );
        assertThat(e.getMessage(), containsString("esql.datasource.local_allowed_paths"));
    }

    public void testProviderRejectsDotDotEscape() throws IOException {
        Path allowed = createTempDir();
        Path sibling = createTempDir();
        Settings settings = Settings.builder().putList("esql.datasource.local_allowed_paths", allowed.toString()).build();
        StorageProviderRegistry registry = registryWithFileAccess(LocalFileAccess.create(settings));

        String traversal = allowed.toAbsolutePath() + "/../" + sibling.getFileName() + "/secret.csv";
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> registry.provider(StoragePath.of("file://" + traversal))
        );
        assertThat(e.getMessage(), containsString("esql.datasource.local_allowed_paths"));
    }

    // --- createProviderTrackingConsumedKeys — scheme-level disabled reject ---

    public void testCreateProviderRejectsFileWhenDisabled() {
        StorageProviderRegistry registry = registryWithFileAccess(LocalFileAccess.create(Settings.EMPTY));
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> registry.createProviderTrackingConsumedKeys("file", Settings.EMPTY, java.util.Map.of())
        );
        assertThat(e.getMessage(), containsString("esql.datasource.local_allowed_paths"));
    }

    public void testCreateProviderAllowsFileWhenEnabled() throws IOException {
        Path allowed = createTempDir();
        Settings settings = Settings.builder().putList("esql.datasource.local_allowed_paths", allowed.toString()).build();
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
