/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.store;

import org.apache.lucene.index.DocValuesSkipIndexType;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.elasticsearch.test.ESTestCase;

import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.Matchers.greaterThan;

@LuceneTestCase.SuppressFileSystems("ExtrasFS")
public class FieldInfoCachingDirectoryTests extends ESTestCase {

    public void testUnwrapWalksFilterChain() throws Exception {
        try (Directory raw = newDirectory()) {
            assertNull(FieldInfoCachingDirectory.unwrap(raw));

            FieldInfoCachingDirectory cached = new FieldInfoCachingDirectory(raw);
            assertSame(cached, FieldInfoCachingDirectory.unwrap(cached));

            // Wrap further in an arbitrary FilterDirectory and confirm we still find the cache via the chain.
            Directory outer = new FilterDirectory(cached) {};
            assertSame(cached, FieldInfoCachingDirectory.unwrap(outer));

            Directory outerOuter = new FilterDirectory(outer) {};
            assertSame(cached, FieldInfoCachingDirectory.unwrap(outerOuter));
        }
    }

    public void testUnwrapReturnsNullWhenAbsent() throws Exception {
        try (Directory raw = newDirectory()) {
            Directory layered = new FilterDirectory(new FilterDirectory(raw) {}) {};
            assertNull(FieldInfoCachingDirectory.unwrap(layered));
        }
    }

    public void testWrapIfEnabledIdempotent() throws Exception {
        assumeTrue("feature flag must be enabled for this test", FieldInfoCachingDirectory.FEATURE_FLAG.isEnabled());
        try (Directory raw = newDirectory()) {
            Directory wrapped = FieldInfoCachingDirectory.wrapIfEnabled(raw);
            assertNotSame(raw, wrapped);
            assertNotNull(FieldInfoCachingDirectory.unwrap(wrapped));

            Directory rewrapped = FieldInfoCachingDirectory.wrapIfEnabled(wrapped);
            assertSame("wrapIfEnabled must be idempotent", wrapped, rewrapped);
        }
    }

    public void testWrapIfEnabledNoOpWhenFlagDisabled() throws Exception {
        assumeFalse("only meaningful when feature flag is off", FieldInfoCachingDirectory.FEATURE_FLAG.isEnabled());
        try (Directory raw = newDirectory()) {
            Directory wrapped = FieldInfoCachingDirectory.wrapIfEnabled(raw);
            assertSame(raw, wrapped);
            assertNull(FieldInfoCachingDirectory.unwrap(wrapped));
        }
    }

    public void testInternFieldInfoReturnsSameInstanceForSameKey() throws Exception {
        try (Directory raw = newDirectory()) {
            FieldInfoCachingDirectory cache = new FieldInfoCachingDirectory(raw);
            Object key = "foo";
            FieldInfo first = cache.internFieldInfo(key, () -> makeFieldInfo("foo", 0, 0L));
            FieldInfo second = cache.internFieldInfo(
                key,
                () -> { throw new AssertionError("factory should not be invoked on cache hit"); }
            );
            assertSame(first, second);
        }
    }

    public void testInternFieldInfoDistinctForDistinctKeys() throws Exception {
        try (Directory raw = newDirectory()) {
            FieldInfoCachingDirectory cache = new FieldInfoCachingDirectory(raw);
            FieldInfo gen0 = cache.internFieldInfo("foo|0", () -> makeFieldInfo("foo", 0, 0L));
            FieldInfo gen1 = cache.internFieldInfo("foo|1", () -> makeFieldInfo("foo", 0, 1L));
            assertNotSame(gen0, gen1);
            assertEquals(0L, gen0.getDocValuesGen());
            assertEquals(1L, gen1.getDocValuesGen());
        }
    }

    public void testWeakReferenceReclaimsCanonical() throws Exception {
        try (Directory raw = newDirectory()) {
            FieldInfoCachingDirectory cache = new FieldInfoCachingDirectory(raw);
            // Intern a one-off FieldInfo, do not hold any reference to it after returning.
            cache.internFieldInfo("ephemeral", () -> makeFieldInfo("ephemeral", 0, 0L));
            assertEquals(1, cache.fieldInfoCacheSize());

            // The next intern call will drain stale refs; eventually GC will clear the weak ref and the entry will be removed.
            assertBusy(() -> {
                System.gc();
                // Touch the cache to trigger drainDeadRefs from the intern path.
                cache.internFieldInfo("trigger-" + System.nanoTime(), () -> makeFieldInfo("trigger", 0, 0L));
                assertTrue(
                    "ephemeral FieldInfo entry should be reclaimed once no strong reference remains",
                    cache.fieldInfoCacheSize() < 2
                );
            });
        }
    }

    public void testConcurrentInternSameKeyReturnsSingleCanonical() throws Exception {
        try (Directory raw = newDirectory()) {
            FieldInfoCachingDirectory cache = new FieldInfoCachingDirectory(raw);
            final int threads = 8;
            final CountDownLatch start = new CountDownLatch(1);
            final CountDownLatch done = new CountDownLatch(threads);
            final AtomicInteger factoryInvocations = new AtomicInteger();
            final FieldInfo[] observed = new FieldInfo[threads];

            for (int t = 0; t < threads; t++) {
                final int idx = t;
                new Thread(() -> {
                    try {
                        start.await();
                        observed[idx] = cache.internFieldInfo("shared-key", () -> {
                            factoryInvocations.incrementAndGet();
                            return makeFieldInfo("shared", 0, 0L);
                        });
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    } finally {
                        done.countDown();
                    }
                }, getTestName() + "-intern-" + t).start();
            }

            start.countDown();
            done.await();

            FieldInfo canonical = observed[0];
            assertNotNull(canonical);
            for (FieldInfo fi : observed) {
                assertSame("all threads must observe the same canonical instance", canonical, fi);
            }
            // At least one factory invocation. Multiple invocations may occur if threads race, but only one is retained as canonical.
            assertTrue("factory must have been invoked at least once", factoryInvocations.get() >= 1);
        }
    }

    public void testRamBytesUsedGrowsWithEntries() throws Exception {
        try (Directory raw = newDirectory()) {
            FieldInfoCachingDirectory cache = new FieldInfoCachingDirectory(raw);
            long empty = cache.ramBytesUsed();
            assertThat("empty cache should still account for instance overhead", empty, greaterThan(0L));

            cache.internFieldInfo("foo|0", () -> makeFieldInfo("foo", 0, 0L));
            long afterOne = cache.ramBytesUsed();
            assertThat("ramBytesUsed must grow after first intern", afterOne, greaterThan(empty));

            cache.internFieldInfo("foo|1", () -> makeFieldInfo("foo", 0, 1L));
            long afterTwo = cache.ramBytesUsed();
            assertThat("ramBytesUsed must grow further for a second distinct entry", afterTwo, greaterThan(afterOne));
        }
    }

    private static FieldInfo makeFieldInfo(String name, int number, long dvGen) {
        return new FieldInfo(
            name,
            number,
            false,
            false,
            false,
            IndexOptions.NONE,
            DocValuesType.NUMERIC,
            DocValuesSkipIndexType.NONE,
            dvGen,
            Map.of(),
            0,
            0,
            0,
            0,
            VectorEncoding.FLOAT32,
            VectorSimilarityFunction.EUCLIDEAN,
            false,
            false
        );
    }
}
