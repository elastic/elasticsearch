/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.datasources.spi.StorageProvider;
import org.elasticsearch.xpack.esql.datasources.spi.StorageProviderFactory;

import java.io.IOException;
import java.time.Instant;
import java.util.List;

public class StorageProviderRegistryTests extends ESTestCase {

    /**
     * After the concurrency-semaphore removal, the registry wraps each provider only with the retry layer — no
     * concurrency-limiting decorator. The {@code ConcurrencyLimited*} classes are gone, so the only structural
     * guard we can assert is that the outermost wrapper is still {@link RetryableStorageProvider} and nothing else.
     */
    public void testWrapsWithRetryableOnly() {
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
}
