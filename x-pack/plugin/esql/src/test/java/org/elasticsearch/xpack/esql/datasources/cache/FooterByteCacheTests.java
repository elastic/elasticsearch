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
import org.elasticsearch.xpack.esql.datasources.spi.AbstractTestStorageObject;
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

    public void testKeyRejectsNullIdentity() {
        NullPointerException e = expectThrows(NullPointerException.class, () -> new FooterByteCache.Key(null, "file.parquet", 1000));
        assertThat(e.getMessage(), containsString("storageIdentity"));
    }

    public void testGetReturnsNullOnMiss() {
        FooterByteCache.Key key = new FooterByteCache.Key(AbstractTestStorageObject.NOOP, "file.parquet", 1000);
        assertNull(cache.get(key));
    }

    public void testPutAndGet() {
        FooterByteCache.Key key = new FooterByteCache.Key(AbstractTestStorageObject.NOOP, "file.parquet", 1000);
        byte[] data = randomByteArrayOfLength(256);
        cache.put(key, data);
        assertArrayEquals(data, cache.get(key));
    }

    public void testPutSkipsOversizedEntries() {
        FooterByteCache cache = new FooterByteCache(1024 * 1024, 100, TTL);
        FooterByteCache.Key key = new FooterByteCache.Key(AbstractTestStorageObject.NOOP, "file.parquet", 1000);
        byte[] oversized = randomByteArrayOfLength(200);
        cache.put(key, oversized);
        assertNull(cache.get(key));
    }

    public void testGetOrLoadPopulatesCache() throws ExecutionException {
        FooterByteCache.Key key = new FooterByteCache.Key(AbstractTestStorageObject.NOOP, "file.parquet", 1000);
        byte[] expected = randomByteArrayOfLength(256);
        byte[] result = cache.getOrLoad(key, k -> expected);
        assertArrayEquals(expected, result);
        assertArrayEquals(expected, cache.get(key));
    }

    public void testGetOrLoadEvictsOversizedEntries() throws ExecutionException {
        FooterByteCache smallMaxEntry = new FooterByteCache(1024 * 1024, 100, TTL);
        FooterByteCache.Key key = new FooterByteCache.Key(AbstractTestStorageObject.NOOP, "file.parquet", 1000);
        byte[] oversized = randomByteArrayOfLength(200);

        byte[] result = smallMaxEntry.getOrLoad(key, k -> oversized);
        assertArrayEquals(oversized, result);
        assertNull("Oversized entry should be evicted after getOrLoad", smallMaxEntry.get(key));
    }

    public void testGetOrLoadEvictsEmptyEntries() throws ExecutionException {
        FooterByteCache.Key key = new FooterByteCache.Key(AbstractTestStorageObject.NOOP, "file.parquet", 1000);
        byte[] result = cache.getOrLoad(key, k -> new byte[0]);
        assertEquals(0, result.length);
        assertNull("Empty entry should be evicted after getOrLoad", cache.get(key));
    }

    public void testLruEviction() {
        FooterByteCache tinyCache = new FooterByteCache(300, 200, TTL);
        byte[] data1 = randomByteArrayOfLength(150);
        byte[] data2 = randomByteArrayOfLength(150);
        byte[] data3 = randomByteArrayOfLength(150);

        FooterByteCache.Key key1 = new FooterByteCache.Key(AbstractTestStorageObject.NOOP, "a.parquet", 1000);
        FooterByteCache.Key key2 = new FooterByteCache.Key(AbstractTestStorageObject.NOOP, "b.parquet", 2000);
        FooterByteCache.Key key3 = new FooterByteCache.Key(AbstractTestStorageObject.NOOP, "c.parquet", 3000);

        tinyCache.put(key1, data1);
        tinyCache.put(key2, data2);
        tinyCache.put(key3, data3);

        assertNull("Oldest entry should be evicted", tinyCache.get(key1));
        assertNotNull(tinyCache.get(key3));
    }

    public void testInvalidateAll() {
        FooterByteCache.Key key = new FooterByteCache.Key(AbstractTestStorageObject.NOOP, "file.parquet", 1000);
        cache.put(key, randomByteArrayOfLength(100));
        assertNotNull(cache.get(key));
        cache.invalidateAll();
        assertNull(cache.get(key));
    }

    public void testSamePathDifferentLengthAreDifferentKeys() {
        FooterByteCache.Key key1 = new FooterByteCache.Key(AbstractTestStorageObject.NOOP, "file.parquet", 1000);
        FooterByteCache.Key key2 = new FooterByteCache.Key(AbstractTestStorageObject.NOOP, "file.parquet", 2000);
        byte[] data1 = randomByteArrayOfLength(100);
        byte[] data2 = randomByteArrayOfLength(100);

        cache.put(key1, data1);
        cache.put(key2, data2);

        assertArrayEquals(data1, cache.get(key1));
        assertArrayEquals(data2, cache.get(key2));
    }

    public void testSamePathSameLengthSharesCacheEntry() throws ExecutionException {
        FooterByteCache.Key key1 = new FooterByteCache.Key(AbstractTestStorageObject.NOOP, "file.parquet", 1000);
        FooterByteCache.Key key2 = new FooterByteCache.Key(AbstractTestStorageObject.NOOP, "file.parquet", 1000);
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
        FooterByteCache.Key key = new FooterByteCache.Key(AbstractTestStorageObject.NOOP, "shared.parquet", 5000);
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
        FooterByteCache.Key key = new FooterByteCache.Key(AbstractTestStorageObject.NOOP, "bad.parquet", 1000);
        ExecutionException ex = expectThrows(ExecutionException.class, () -> cache.getOrLoad(key, k -> {
            throw new RuntimeException("simulated I/O failure");
        }));
        assertNotNull(ex.getCause());
        assertEquals("simulated I/O failure", ex.getCause().getMessage());
    }

    /**
     * Reading an entry renews it. The reads below span more than twice the TTL, so under a write clock the
     * entry would be gone by the last one. This is the whole of what the clock buys: a dashboard's footer
     * working set is stored as one cohort in the run's opening seconds, and a write clock drops all of it
     * one TTL later however hard the dashboard is still using it.
     */
    public void testReadingAnEntryRenewsIt() {
        TimeValue ttl = TimeValue.timeValueSeconds(2);
        FooterByteCache shortLived = new FooterByteCache(1024 * 1024, 512 * 1024, ttl);
        FooterByteCache.Key key = new FooterByteCache.Key(AbstractTestStorageObject.NOOP, "file.parquet", 1000);
        byte[] data = randomByteArrayOfLength(64);

        shortLived.put(key, data);
        assertNotNull("the entry must be stored to begin with, or the assertion below passes vacuously", shortLived.get(key));
        long readUntil = System.nanoTime() + TimeValue.timeValueSeconds(5).nanos();
        while (System.nanoTime() < readUntil) {
            assertArrayEquals("an entry read at least once per TTL is still served past one TTL", data, shortLived.get(key));
            safeSleep(50);
        }
    }

    /**
     * The clock still releases what nothing is reading. Without this the test above would pass on a cache
     * with no clock at all, and the TTL would have stopped bounding anything.
     */
    public void testAnEntryNobodyReadsExpires() {
        TimeValue ttl = TimeValue.timeValueSeconds(2);
        FooterByteCache shortLived = new FooterByteCache(1024 * 1024, 512 * 1024, ttl);
        FooterByteCache.Key key = new FooterByteCache.Key(AbstractTestStorageObject.NOOP, "file.parquet", 1000);

        shortLived.put(key, randomByteArrayOfLength(64));
        assertNotNull("the entry must be stored to begin with, or the assertion below passes vacuously", shortLived.get(key));
        safeSleep(ttl.millis() * 3);

        assertNull("an entry unread for a TTL is released", shortLived.get(key));
    }

    /**
     * The write cap expires an entry even when it is being continuously read, so a same-length-overwrite
     * staleness window is bounded. TTL=500ms → write cap=3s (6×). Reads every 50ms keep the access clock
     * alive for five TTLs before the cap fires; the entry must be present at 2.5s (access clock renewal
     * confirmed, cap not yet fired) and absent at 3.5s (cap fired; continuous reads cannot keep it alive).
     * A cache with only an access clock would still serve the entry at 3.5s, so the final assertion
     * discriminates the cap from a pure access clock.
     */
    public void testWriteCapExpiresAnEntryDespiteContinuousReads() {
        TimeValue ttl = TimeValue.timeValueMillis(500);
        FooterByteCache shortLived = new FooterByteCache(1024 * 1024, 512 * 1024, ttl);
        FooterByteCache.Key key = new FooterByteCache.Key(AbstractTestStorageObject.NOOP, "file.parquet", 1000);
        byte[] data = randomByteArrayOfLength(64);

        shortLived.put(key, data);
        long writeNanos = System.nanoTime();
        assertNotNull("the entry must be stored to begin with", shortLived.get(key));

        long priorCapDeadline = writeNanos + TimeValue.timeValueMillis(2500).nanos();
        while (System.nanoTime() < priorCapDeadline) {
            shortLived.get(key);
            safeSleep(50);
        }
        assertNotNull(
            "entry kept alive by continuous reads must be present at 2.5s: access clock is renewed and write cap has not fired yet",
            shortLived.get(key)
        );

        long postCapDeadline = writeNanos + TimeValue.timeValueMillis(3500).nanos();
        while (System.nanoTime() < postCapDeadline) {
            shortLived.get(key);
            safeSleep(50);
        }
        assertNull("write cap must force-expire the entry at 3s even though it was continuously read through 3.5s", shortLived.get(key));
    }

    public void testFromSettingsUsesConfiguredTtl() {
        Settings settings = Settings.builder().put("esql.external.cache.footer.ttl", "42s").build();
        FooterByteCache configured = FooterByteCache.fromSettings(settings);
        assertEquals(TimeValue.timeValueSeconds(42), configured.expireAfterAccess());
    }

    public void testFromSettingsDefaults() {
        FooterByteCache defaults = FooterByteCache.fromSettings(Settings.EMPTY);
        assertEquals(TimeValue.timeValueMinutes(5), defaults.expireAfterAccess());
        assertEquals(TimeValue.timeValueMinutes(30), defaults.expireAfterWrite());
        assertThat(defaults.maxEntryBytes(), lessThanOrEqualTo(FooterByteCache.DEFAULT_MAX_ENTRY_BYTES));
    }

    public void testWriteCapIsSixTimesAccessTtl() {
        Settings settings = Settings.builder().put("esql.external.cache.footer.ttl", "3m").build();
        FooterByteCache configured = FooterByteCache.fromSettings(settings);
        assertEquals(TimeValue.timeValueMinutes(3), configured.expireAfterAccess());
        assertEquals(TimeValue.timeValueMinutes(18), configured.expireAfterWrite());
    }

    public void testWriteCapSaturatesOnLargeTtl() {
        // 20000 days exceeds Long.MAX_VALUE / WRITE_CAP_MULTIPLIER nanoseconds;
        // without the saturation guard, WRITE_CAP_MULTIPLIER * ttl_nanos wraps negative.
        FooterByteCache large = new FooterByteCache(1024 * 1024, 512 * 1024, TimeValue.timeValueDays(20000));
        assertEquals(Long.MAX_VALUE, large.expireAfterWrite().nanos());
        // Boundary: exactly Long.MAX_VALUE / WRITE_CAP_MULTIPLIER nanos does not saturate.
        long boundaryNanos = Long.MAX_VALUE / FooterByteCache.WRITE_CAP_MULTIPLIER;
        FooterByteCache boundary = new FooterByteCache(1024 * 1024, 512 * 1024, TimeValue.timeValueNanos(boundaryNanos));
        assertEquals(boundaryNanos * FooterByteCache.WRITE_CAP_MULTIPLIER, boundary.expireAfterWrite().nanos());
    }

    public void testFromSettingsClampsMaxEntryToBudgetFraction() {
        // The budget is heap-relative, so a fixed 2 MiB entry ceiling would let one entry evict
        // nearly the whole cache on a small heap. Entries are capped at a quarter of the budget.
        Settings settings = Settings.builder().put("esql.external.cache.footer.size", "1mb").build();
        FooterByteCache small = FooterByteCache.fromSettings(settings);
        assertEquals(256 * 1024L, small.maxEntryBytes());
    }

    public void testFromSettingsClampsMaxEntryToDefaultWhenBudgetIsLarge() {
        Settings settings = Settings.builder().put("esql.external.cache.footer.size", "32mb").build();
        FooterByteCache large = FooterByteCache.fromSettings(settings);
        assertEquals(FooterByteCache.DEFAULT_MAX_ENTRY_BYTES, large.maxEntryBytes());
        assertEquals(2L * 1024 * 1024, large.maxEntryBytes());
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
        FooterByteCache.Key key = new FooterByteCache.Key(AbstractTestStorageObject.NOOP, "file.parquet", 1000);
        cache.put(key, randomByteArrayOfLength(100));
        assertNull("distinct cache instances must not share entries", other.get(key));
    }
}
