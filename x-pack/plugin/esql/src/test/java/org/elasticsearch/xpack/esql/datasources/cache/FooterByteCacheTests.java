/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.cache;

import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class FooterByteCacheTests extends ESTestCase {

    private FooterByteCache cache;

    private static final TimeValue TTL = TimeValue.timeValueMinutes(5);

    @Before
    public void initCache() {
        cache = new FooterByteCache(1024 * 1024, 512 * 1024, TTL);
    }

    public void testGetReturnsNullOnMiss() {
        FooterByteCache.Key key = new FooterByteCache.Key("file.parquet", 1000);
        assertNull(cache.get(key));
    }

    public void testPutAndGet() {
        FooterByteCache.Key key = new FooterByteCache.Key("file.parquet", 1000);
        byte[] data = randomByteArrayOfLength(256);
        cache.put(key, data);
        assertArrayEquals(data, cache.get(key));
    }

    public void testPutSkipsOversizedEntries() {
        FooterByteCache cache = new FooterByteCache(1024 * 1024, 100, TTL);
        FooterByteCache.Key key = new FooterByteCache.Key("file.parquet", 1000);
        byte[] oversized = randomByteArrayOfLength(200);
        cache.put(key, oversized);
        assertNull(cache.get(key));
    }

    public void testGetOrLoadPopulatesCache() throws ExecutionException {
        FooterByteCache.Key key = new FooterByteCache.Key("file.parquet", 1000);
        byte[] expected = randomByteArrayOfLength(256);
        byte[] result = cache.getOrLoad(key, k -> expected);
        assertArrayEquals(expected, result);
        assertArrayEquals(expected, cache.get(key));
    }

    public void testGetOrLoadEvictsOversizedEntries() throws ExecutionException {
        FooterByteCache smallMaxEntry = new FooterByteCache(1024 * 1024, 100, TTL);
        FooterByteCache.Key key = new FooterByteCache.Key("file.parquet", 1000);
        byte[] oversized = randomByteArrayOfLength(200);

        byte[] result = smallMaxEntry.getOrLoad(key, k -> oversized);
        assertArrayEquals(oversized, result);
        assertNull("Oversized entry should be evicted after getOrLoad", smallMaxEntry.get(key));
    }

    public void testGetOrLoadEvictsEmptyEntries() throws ExecutionException {
        FooterByteCache.Key key = new FooterByteCache.Key("file.parquet", 1000);
        byte[] result = cache.getOrLoad(key, k -> new byte[0]);
        assertEquals(0, result.length);
        assertNull("Empty entry should be evicted after getOrLoad", cache.get(key));
    }

    public void testLruEviction() {
        FooterByteCache tinyCache = new FooterByteCache(300, 200, TTL);
        byte[] data1 = randomByteArrayOfLength(150);
        byte[] data2 = randomByteArrayOfLength(150);
        byte[] data3 = randomByteArrayOfLength(150);

        FooterByteCache.Key key1 = new FooterByteCache.Key("a.parquet", 1000);
        FooterByteCache.Key key2 = new FooterByteCache.Key("b.parquet", 2000);
        FooterByteCache.Key key3 = new FooterByteCache.Key("c.parquet", 3000);

        tinyCache.put(key1, data1);
        tinyCache.put(key2, data2);
        tinyCache.put(key3, data3);

        assertNull("Oldest entry should be evicted", tinyCache.get(key1));
        assertNotNull(tinyCache.get(key3));
    }

    public void testInvalidateAll() {
        FooterByteCache.Key key = new FooterByteCache.Key("file.parquet", 1000);
        cache.put(key, randomByteArrayOfLength(100));
        assertNotNull(cache.get(key));
        cache.invalidateAll();
        assertNull(cache.get(key));
    }

    public void testSamePathDifferentLengthAreDifferentKeys() {
        FooterByteCache.Key key1 = new FooterByteCache.Key("file.parquet", 1000);
        FooterByteCache.Key key2 = new FooterByteCache.Key("file.parquet", 2000);
        byte[] data1 = randomByteArrayOfLength(100);
        byte[] data2 = randomByteArrayOfLength(100);

        cache.put(key1, data1);
        cache.put(key2, data2);

        assertArrayEquals(data1, cache.get(key1));
        assertArrayEquals(data2, cache.get(key2));
    }

    public void testSamePathSameLengthSharesCacheEntry() throws ExecutionException {
        FooterByteCache.Key key1 = new FooterByteCache.Key("file.parquet", 1000);
        FooterByteCache.Key key2 = new FooterByteCache.Key("file.parquet", 1000);
        byte[] data = randomByteArrayOfLength(100);

        cache.getOrLoad(key1, k -> data);
        AtomicInteger loadCount = new AtomicInteger();
        byte[] result = cache.getOrLoad(key2, k -> {
            loadCount.incrementAndGet();
            return randomByteArrayOfLength(100);
        });

        assertEquals("Loader should not have been called for existing key", 0, loadCount.get());
        assertArrayEquals(data, result);
    }

    /**
     * Verifies thundering-herd protection: concurrent getOrLoad calls for the same key
     * invoke the loader exactly once.
     */
    public void testThunderingHerdCoalescesConcurrentLoads() throws Exception {
        FooterByteCache.Key key = new FooterByteCache.Key("shared.parquet", 5000);
        byte[] expected = randomByteArrayOfLength(256);
        AtomicInteger loadCount = new AtomicInteger();
        CountDownLatch start = new CountDownLatch(1);
        int threadCount = randomIntBetween(4, 16);
        AtomicReference<AssertionError> failure = new AtomicReference<>();
        List<Thread> threads = new ArrayList<>(threadCount);

        for (int i = 0; i < threadCount; i++) {
            Thread t = new Thread(() -> {
                try {
                    start.await(10, TimeUnit.SECONDS);
                    byte[] result = cache.getOrLoad(key, k -> {
                        loadCount.incrementAndGet();
                        return expected;
                    });
                    assertArrayEquals(expected, result);
                } catch (AssertionError e) {
                    failure.compareAndSet(null, e);
                } catch (Exception e) {
                    failure.compareAndSet(null, new AssertionError("Unexpected exception", e));
                }
            }, "herd-" + i);
            t.start();
            threads.add(t);
        }

        start.countDown();
        for (Thread t : threads) {
            t.join(TimeUnit.SECONDS.toMillis(10));
            assertFalse("Thread " + t.getName() + " did not finish in time", t.isAlive());
        }

        AssertionError err = failure.get();
        if (err != null) {
            throw err;
        }
        assertEquals("Loader should have been called exactly once", 1, loadCount.get());
    }

    public void testGetOrLoadPropagatesLoaderException() {
        FooterByteCache.Key key = new FooterByteCache.Key("bad.parquet", 1000);
        ExecutionException ex = expectThrows(ExecutionException.class, () -> cache.getOrLoad(key, k -> {
            throw new RuntimeException("simulated I/O failure");
        }));
        assertNotNull(ex.getCause());
        assertEquals("simulated I/O failure", ex.getCause().getMessage());
    }

    public void testFromSettingsUsesConfiguredTtl() {
        Settings settings = Settings.builder().put("esql.external.cache.footer.ttl", "42s").build();
        FooterByteCache configured = FooterByteCache.fromSettings(settings);
        assertEquals(TimeValue.timeValueSeconds(42), configured.expireAfterAccess());
    }

    public void testFromSettingsDefaults() {
        FooterByteCache defaults = FooterByteCache.fromSettings(Settings.EMPTY);
        assertEquals(TimeValue.timeValueMinutes(5), defaults.expireAfterAccess());
        assertThat(defaults.maxEntryBytes(), lessThanOrEqualTo(FooterByteCache.DEFAULT_MAX_ENTRY_BYTES));
    }

    public void testFromSettingsClampsMaxEntryToBudgetFraction() {
        // The budget is heap-relative, so a fixed 2 MiB entry ceiling would let one entry evict
        // nearly the whole cache on a small heap. Entries are capped at a quarter of the budget.
        Settings settings = Settings.builder().put("esql.external.cache.footer.size", "1mb").build();
        FooterByteCache small = FooterByteCache.fromSettings(settings);
        assertEquals(256 * 1024L, small.maxEntryBytes());
    }

    public void testNonPositiveBudgetIsRejectedAtSettingsParseTime() {
        // The caches are built lazily on first reader construction, so a zero budget must be
        // rejected when the setting is parsed rather than on the first Parquet/ORC query.
        String key = randomFrom("esql.external.cache.footer.size", "esql.external.cache.footer.parsed.size");
        Setting<?> setting = key.endsWith("parsed.size")
            ? ExternalSourceCacheSettings.FOOTER_PARSED_CACHE_SIZE
            : ExternalSourceCacheSettings.FOOTER_CACHE_SIZE;
        Settings settings = Settings.builder().put(key, randomFrom("0b", "0%")).build();
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> setting.get(settings));
        assertThat(e.getMessage(), containsString("must be greater than 0"));
    }

    public void testEachConstructionIsIndependent() {
        // Distinct cache instances must not share entries.
        FooterByteCache other = new FooterByteCache(1024 * 1024, 512 * 1024, TTL);
        FooterByteCache.Key key = new FooterByteCache.Key("file.parquet", 1000);
        cache.put(key, randomByteArrayOfLength(100));
        assertNull("distinct cache instances must not share entries", other.get(key));
    }
}
