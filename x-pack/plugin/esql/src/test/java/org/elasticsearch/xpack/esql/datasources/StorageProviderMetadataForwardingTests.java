/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasources.cache.StorageProviderCache;
import org.elasticsearch.xpack.esql.datasources.spi.Configured;
import org.elasticsearch.xpack.esql.datasources.spi.StorageChildren;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.datasources.spi.StorageProvider;

import java.io.IOException;
import java.lang.reflect.Method;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Every provider that wraps another one must declare {@link StorageProvider#objectMetadata}.
 * <p>
 * The method has a default, and that default resolves through {@code this.newObject}. So a decorator that does not
 * declare it does not inherit the delegate's override — it silently takes the delegate's handle path instead, and
 * the override never runs.
 * <p>
 * Named as strings because two are private nested classes, which the reflection allowed here cannot reach from
 * source.
 */
public class StorageProviderMetadataForwardingTests extends ESTestCase {

    private static final List<String> WRAPPING_PROVIDERS = List.of(
        "org.elasticsearch.xpack.esql.datasources.RetryableStorageProvider",
        "org.elasticsearch.xpack.esql.datasources.QueryBudgetedStorageProvider",
        "org.elasticsearch.xpack.esql.datasources.ConcurrencyLimitedStorageProvider",
        "org.elasticsearch.xpack.esql.datasources.cache.StorageProviderCache$PooledStorageProvider",
        "org.elasticsearch.xpack.esql.datasources.FileSourceFactory$DeferredPoolLease"
    );

    public void testEveryWrappingProviderForwardsObjectMetadata() throws Exception {
        for (String name : WRAPPING_PROVIDERS) {
            Class<?> wrapper = Class.forName(name);
            assertTrue(
                name + " is listed here but does not implement StorageProvider, so this entry checks nothing",
                StorageProvider.class.isAssignableFrom(wrapper)
            );
            assertTrue(
                wrapper.getName()
                    + " wraps a StorageProvider but does not declare objectMetadata, so it inherits the interface "
                    + "default and the delegate's override never runs. Forward it to the delegate.",
                declaresObjectMetadata(wrapper)
            );
        }
    }

    private static boolean declaresObjectMetadata(Class<?> wrapper) {
        for (Method method : wrapper.getMethods()) {
            if (method.getName().equals("objectMetadata")
                && method.getParameterCount() == 1
                && method.getParameterTypes()[0] == StoragePath.class
                && method.getDeclaringClass() == wrapper) {
                return true;
            }
        }
        return false;
    }

    /**
     * The default is what every provider but S3 uses, so it is the majority path through this method.
     */
    public void testDefaultReadsLengthAndModificationTimeFromTheHandle() throws IOException {
        StorageObject handle = mock(StorageObject.class);
        when(handle.length()).thenReturn(4096L);
        when(handle.lastModified()).thenReturn(Instant.parse("2026-02-03T04:05:06Z"));

        StorageEntry entry = providerReturning(handle).objectMetadata(StoragePath.of("s3://bucket/file.csv"));

        assertEquals(4096L, entry.length());
        assertEquals(Instant.parse("2026-02-03T04:05:06Z"), entry.lastModified());
    }

    /** A provider that reports no modification time still yields a usable key component. */
    public void testDefaultSubstitutesEpochForAnAbsentModificationTime() throws IOException {
        StorageObject handle = mock(StorageObject.class);
        when(handle.length()).thenReturn(1L);
        when(handle.lastModified()).thenReturn(null);

        assertEquals(Instant.EPOCH, providerReturning(handle).objectMetadata(StoragePath.of("s3://bucket/f")).lastModified());
    }

    /**
     * Declaring the method is not forwarding it: a decorator that declares it and does something other than ask
     * the delegate passes the gate above. These assert the call arrives.
     */
    public void testEachDecoratorsForwardReachesTheDelegate() throws Exception {
        StoragePath path = StoragePath.of("s3://bucket/file.csv");
        for (String name : List.of(
            "RetryableStorageProvider",
            "QueryBudgetedStorageProvider",
            "ConcurrencyLimitedStorageProvider",
            "PooledStorageProvider"
        )) {
            AtomicInteger calls = new AtomicInteger();
            StorageProvider delegate = countingProvider(calls);
            StorageProvider decorated = name.equals("PooledStorageProvider") ? pooled(delegate) : decorate(delegate, name);

            assertEquals(name + " did not forward objectMetadata", 7L, decorated.objectMetadata(path).length());
            assertEquals(name + " forwarded a different number of times", 1, calls.get());
        }
    }

    private static StorageProvider countingProvider(AtomicInteger calls) {
        return new StubProvider() {
            @Override
            public StorageEntry objectMetadata(StoragePath path) {
                calls.incrementAndGet();
                return new StorageEntry(path, 7L, Instant.EPOCH);
            }
        };
    }

    private static StorageProvider decorate(StorageProvider delegate, String which) {
        return switch (which) {
            case "RetryableStorageProvider" -> new RetryableStorageProvider(delegate, new RetryPolicy(3, 1, 10));
            case "QueryBudgetedStorageProvider" -> new QueryBudgetedStorageProvider(delegate, new QueryConcurrencyBudget(3, 60_000L, null));
            case "ConcurrencyLimitedStorageProvider" -> new ConcurrencyLimitedStorageProvider(
                delegate,
                new ConcurrencyLimiter("s3", new ExternalSourceSettings.BlobStoreConcurrency(3, false))
            );
            default -> throw new IllegalArgumentException(which);
        };
    }

    private static StorageProvider pooled(StorageProvider delegate) throws Exception {
        StorageProviderCache cache = new StorageProviderCache();
        return cache.getOrCreate(new StorageProviderCache.CacheKey("s3", Map.of("k", "v")), () -> Configured.empty(delegate)).value();
    }

    private StorageProvider providerReturning(StorageObject handle) {
        return new StubProvider() {
            @Override
            public StorageObject newObject(StoragePath path) {
                return handle;
            }
        };
    }

    private abstract static class StubProvider implements StorageProvider {
        @Override
        public StorageChildren listChildren(StoragePath prefix, int limit) {
            return null;
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
            return List.of();
        }

        @Override
        public void close() {}
    }
}
