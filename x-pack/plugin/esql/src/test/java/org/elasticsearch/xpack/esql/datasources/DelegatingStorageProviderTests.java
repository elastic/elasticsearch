/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.core.CheckedFunction;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasources.cache.StorageProviderCache;
import org.elasticsearch.xpack.esql.datasources.spi.Configured;
import org.elasticsearch.xpack.esql.datasources.spi.StorageChildren;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.datasources.spi.StorageProvider;

import java.io.IOException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * What {@link DelegatingStorageProvider} has to keep true for the five decorators that extend it.
 * <p>
 * A method on {@link StorageProvider} with a {@code default} resolves through {@code this}, so a wrapper that does
 * not declare it does not inherit the delegate's override — it silently takes the delegate's own handle path instead,
 * and the override never runs. Nothing in the language reports that: the call compiles and the suites pass. The base
 * exists so there is one place to get it right, and these tests are what hold it there.
 */
public class DelegatingStorageProviderTests extends ESTestCase {

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
     * The base forwards every call, so a decorator that does not mention a method still reaches the delegate.
     * This is the property the five decorators now inherit instead of copying, and the one a method added to
     * the interface later depends on.
     */
    public void testTheBaseForwardsEveryCallToTheDelegate() throws Exception {
        RecordingProvider delegate = new RecordingProvider();
        StorageProvider forwarding = new DelegatingStorageProvider() {
            @Override
            protected StorageProvider delegate() {
                return delegate;
            }

            @Override
            public void close() {}
        };
        StoragePath path = StoragePath.of("s3://bucket/file.csv");

        forwarding.newObject(path);
        forwarding.newObject(path, 1L);
        forwarding.newObject(path, 1L, Instant.EPOCH);
        forwarding.listObjects(path, false);
        forwarding.listChildren(path, 1);
        forwarding.exists(path);
        forwarding.objectMetadata(path);
        forwarding.supportedSchemes();
        forwarding.supportsStableMetadata();
        forwarding.listsInKeyOrder();

        assertEquals(
            "a call did not reach the delegate",
            List.of(
                "newObject/1",
                "newObject/2",
                "newObject/3",
                "listObjects",
                "listChildren",
                "exists",
                "objectMetadata",
                "supportedSchemes",
                "supportsStableMetadata",
                "listsInKeyOrder"
            ),
            delegate.seen
        );
    }

    /**
     * The base has to declare every method on the interface, not merely the ten that exist today. A method added to
     * {@link StorageProvider} with a {@code default} and not added here is inherited by the base exactly as it would
     * be by a decorator, so all five wrappers silently stop reaching their delegate for it at once — the failure the
     * base was introduced to make impossible. Asserted over the interface's own method set rather than a list of
     * names, so adding a method to the interface is what turns this red.
     */
    public void testTheBaseDeclaresEveryMethodOnTheInterface() throws Exception {
        List<String> inherited = new ArrayList<>();
        for (Method method : StorageProvider.class.getMethods()) {
            Method onBase = DelegatingStorageProvider.class.getMethod(method.getName(), method.getParameterTypes());
            if (onBase.getDeclaringClass() != DelegatingStorageProvider.class) {
                inherited.add(method.getName() + Arrays.toString(method.getParameterTypes()));
            }
        }
        assertEquals("the base does not declare these, so every decorator inherits the interface default", List.of(), inherited);
    }

    /**
     * close() is the one method the base declares abstract rather than forwarding, so the compiler asks each
     * decorator what owning the delegate means. Two of them hold a provider borrowed from a pool and have to return
     * it, and an inherited forward would have closed one under the queries still holding it.
     */
    public void testCloseIsAbstractRatherThanForwarded() throws Exception {
        assertTrue(
            "close() must stay abstract on the base",
            Modifier.isAbstract(DelegatingStorageProvider.class.getMethod("close").getModifiers())
        );
    }

    /** Records which method arrived, in order, so a forward that goes to the wrong one is visible. */
    private static final class RecordingProvider extends StubProvider {
        private final List<String> seen = new ArrayList<>();

        @Override
        public StorageObject newObject(StoragePath path) {
            seen.add("newObject/1");
            return null;
        }

        @Override
        public StorageObject newObject(StoragePath path, long length) {
            seen.add("newObject/2");
            return null;
        }

        @Override
        public StorageObject newObject(StoragePath path, long length, Instant lastModified) {
            seen.add("newObject/3");
            return null;
        }

        @Override
        public StorageIterator listObjects(StoragePath prefix, boolean recursive) {
            seen.add("listObjects");
            return null;
        }

        @Override
        public StorageChildren listChildren(StoragePath prefix, int limit) {
            seen.add("listChildren");
            return null;
        }

        @Override
        public boolean exists(StoragePath path) {
            seen.add("exists");
            return true;
        }

        @Override
        public StorageEntry objectMetadata(StoragePath path) {
            seen.add("objectMetadata");
            return new StorageEntry(StoragePath.of("s3://b/k"), 1L, Instant.EPOCH);
        }

        @Override
        public List<String> supportedSchemes() {
            seen.add("supportedSchemes");
            return List.of();
        }

        @Override
        public boolean supportsStableMetadata() {
            seen.add("supportsStableMetadata");
            return true;
        }

        @Override
        public boolean listsInKeyOrder() {
            seen.add("listsInKeyOrder");
            return true;
        }
    }

    /**
     * Declaring a method is not forwarding it: a decorator that declares it and does something other than ask the
     * delegate passes the declaring-class gate above. These assert the call actually arrives at the delegate.
     */
    public void testEachDecoratorsForwardReachesTheDelegate() throws Exception {
        StoragePath path = StoragePath.of("s3://bucket/file.csv");
        // Named by the constructor rather than by a string, so a renamed decorator is a compile error here.
        Map<String, CheckedFunction<StorageProvider, StorageProvider, Exception>> decorators = new LinkedHashMap<>();
        decorators.put(RetryableStorageProvider.class.getSimpleName(), d -> new RetryableStorageProvider(d, new RetryPolicy(3, 1, 10)));
        decorators.put(
            QueryBudgetedStorageProvider.class.getSimpleName(),
            d -> new QueryBudgetedStorageProvider(d, new QueryConcurrencyBudget(3, 60_000L, null))
        );
        decorators.put(
            ConcurrencyLimitedStorageProvider.class.getSimpleName(),
            d -> new ConcurrencyLimitedStorageProvider(
                d,
                new ConcurrencyLimiter("s3", new ExternalSourceSettings.BlobStoreConcurrency(3, false))
            )
        );
        // The pooled lease is the one that cannot be constructed directly: it is private to the cache.
        decorators.put("PooledStorageProvider", DelegatingStorageProviderTests::pooled);

        for (var decorator : decorators.entrySet()) {
            String name = decorator.getKey();
            AtomicInteger calls = new AtomicInteger();
            StorageProvider decorated = decorator.getValue().apply(countingProvider(calls));

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
