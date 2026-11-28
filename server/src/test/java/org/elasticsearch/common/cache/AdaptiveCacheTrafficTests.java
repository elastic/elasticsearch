/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.common.cache;

import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.CountDownLatch;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

/**
 * Compares the stock LRU {@link Cache} with the adaptive eviction on synthetic traffic. Seeds are fixed so the numbers are
 * reproducible; run with {@code -Dtests.output=always} to see the table.
 */
public class AdaptiveCacheTrafficTests extends ESTestCase {

    private static final int REQUESTS = 600_000;
    private static final long CAPACITY = 2_000;

    private record Result(double lru, double adaptive, AdaptiveEviction.Stats stats) {}

    private static double replay(CacheTraffic traffic, Cache<Integer, Boolean> cache) throws Exception {
        for (int key : traffic.keys) {
            cache.computeIfAbsent(key, k -> Boolean.TRUE);
        }
        Cache.Stats s = cache.stats();
        return (double) s.getHits() / (s.getHits() + s.getMisses());
    }

    private static Result run(CacheTraffic traffic, long capacity) throws Exception {
        Cache<Integer, Boolean> lru = newCache(capacity, false);
        Cache<Integer, Boolean> adaptive = newCache(capacity, true);
        double l = replay(traffic, lru);
        double a = replay(traffic, adaptive);
        return new Result(l, a, adaptive.adaptiveStats());
    }

    public void testScenarios() throws Exception {
        final long seed = 2;
        List<Supplier<CacheTraffic>> scenarios = List.of(
            () -> CacheTraffic.zipf(REQUESTS, seed),
            () -> CacheTraffic.drift(REQUESTS, seed),
            () -> CacheTraffic.shift(REQUESTS, seed),
            () -> CacheTraffic.scans(REQUESTS, seed),
            () -> CacheTraffic.recency(REQUESTS, seed),
            () -> CacheTraffic.phaseMix(REQUESTS, seed)
        );
        logger.info(
            String.format(Locale.ROOT, "%n%-18s %9s %10s %10s   follow[lru,lfu] switches%n", "scenario", "LRU(stock)", "adaptive", "gain")
        );
        double[] lru = new double[scenarios.size()];
        double[] adaptive = new double[scenarios.size()];
        for (int i = 0; i < scenarios.size(); i++) {
            CacheTraffic traffic = scenarios.get(i).get();
            Result r = run(traffic, CAPACITY);
            lru[i] = r.lru();
            adaptive[i] = r.adaptive();
            AdaptiveEviction.Stats st = r.stats();
            logger.info(
                String.format(
                    Locale.ROOT,
                    "%-18s %9.4f %10.4f %+10.4f   [%.2f, %.2f] %d%n",
                    traffic.name,
                    r.lru(),
                    r.adaptive(),
                    r.adaptive() - r.lru(),
                    st.followShare()[0],
                    st.followShare()[1],
                    st.switches()
                )
            );
        }
        // qualitative expectations established in simulation: never clearly worse than LRU, clearly better where frequency matters
        for (int i = 0; i < lru.length; i++) {
            assertThat("scenario " + i, adaptive[i], greaterThanOrEqualTo(lru[i] - 0.01));
        }
        assertThat(adaptive[0], greaterThan(lru[0] + 0.05));   // stationary zipf
        assertThat(adaptive[2], greaterThan(lru[2] + 0.04));   // abrupt re-rank: needs the short default LFU memory
        assertThat(adaptive[3], greaterThan(lru[3] + 0.05));   // zipf + scans
        assertThat(adaptive[5], greaterThan(lru[5] + 0.02));   // phase mix
    }

    private static double hitRatio(CacheTraffic traffic, long capacity, AdaptiveEviction<Integer> config) throws Exception {
        Cache<Integer, Boolean> cache = CacheBuilder.<Integer, Boolean>builder()
            .setMaximumWeight(capacity)
            .setAdaptiveEviction(config)
            .build();
        return replay(traffic, cache);
    }

    /**
     * After the popularity ranking is re-drawn the LFU shadow cache must forget the old ranking in time: the default memory (100k
     * accesses) recovers clearly better than the 655k accesses of the first version, at capacities of 0.1% to 4% of the popular keys.
     */
    public void testDefaultSketchMemoryRecoversFromReRanking() throws Exception {
        final CacheTraffic traffic = CacheTraffic.shift(REQUESTS, 2);
        for (long capacity : new long[] { 500, CAPACITY }) {
            final double shortMemory = hitRatio(traffic, capacity, AdaptiveEviction.<Integer>builder().seed(1).build());
            final double longMemory = hitRatio(
                traffic,
                capacity,
                AdaptiveEviction.<Integer>builder().seed(1).sketchSample(10L * AdaptiveEviction.DEFAULT_SKETCH_WIDTH).build()
            );
            assertThat("capacity " + capacity, shortMemory, greaterThan(longMemory + 0.02));
        }
    }

    private static Cache<Integer, Boolean> newCache(long capacity, boolean adaptive) {
        CacheBuilder<Integer, Boolean> b = CacheBuilder.<Integer, Boolean>builder().setMaximumWeight(capacity);
        if (adaptive) {
            b.setAdaptiveEviction(AdaptiveEviction.<Integer>builder().seed(1).build());
        }
        return b.build();
    }

    private static double nsPerOp(Cache<Integer, Boolean> cache, int[] keys, int threads) throws Exception {
        final CountDownLatch start = new CountDownLatch(1);
        List<Thread> ts = new ArrayList<>();
        for (int t = 0; t < threads; t++) {
            final int offset = t * 7919;
            Thread th = new Thread(() -> {
                try {
                    start.await();
                    for (int i = 0; i < keys.length; i++) {
                        cache.computeIfAbsent(keys[(i + offset) % keys.length], k -> Boolean.TRUE);
                    }
                } catch (Exception e) {
                    throw new AssertionError(e);
                }
            });
            ts.add(th);
            th.start();
        }
        long t0 = System.nanoTime();
        start.countDown();
        for (Thread th : ts) {
            th.join();
        }
        return (System.nanoTime() - t0) / (double) ((long) keys.length * threads);
    }

    /** Not an assertion: prints the cost of the adaptive mode relative to the stock cache (single thread, contended, memory). */
    public void testOverheadReport() throws Exception {
        CacheTraffic traffic = CacheTraffic.zipf(400_000, 5);
        int[] keys = traffic.keys;
        final int rounds = 7;                               // the first two rounds warm up the JIT, the median of the rest is reported
        double[] lru1 = new double[rounds];
        double[] ad1 = new double[rounds];
        double[] lru8 = new double[rounds];
        double[] ad8 = new double[rounds];
        for (int round = 0; round < rounds; round++) {
            lru1[round] = nsPerOp(newCache(CAPACITY, false), keys, 1);
            ad1[round] = nsPerOp(newCache(CAPACITY, true), keys, 1);
            lru8[round] = nsPerOp(newCache(CAPACITY, false), keys, 8);
            ad8[round] = nsPerOp(newCache(CAPACITY, true), keys, 8);
        }
        final double mLru1 = median(lru1);
        final double mAd1 = median(ad1);
        final double mLru8 = median(lru8);
        final double mAd8 = median(ad8);
        logger.info(
            String.format(
                Locale.ROOT,
                "%nOVERHEAD capacity=%d: 1 thread  stock %.0f ns/op  adaptive %.0f ns/op (x%.2f, +%.0f ns); "
                    + "8 threads  stock %.0f ns/op  adaptive %.0f ns/op (x%.2f, +%.0f ns)%n",
                CAPACITY,
                mLru1,
                mAd1,
                mAd1 / mLru1,
                mAd1 - mLru1,
                mLru8,
                mAd8,
                mAd8 / mLru8,
                mAd8 - mLru8
            )
        );
        for (long cap : new long[] { 20_000, 200_000 }) {
            long base = usedHeap();
            Cache<Integer, Boolean> stock = newCache(cap, false);
            for (int i = 0; i < 2 * (int) cap; i++) {
                stock.computeIfAbsent(i, k -> Boolean.TRUE);
            }
            long afterStock = usedHeap();
            Cache<Integer, Boolean> adaptive = newCache(cap, true);
            for (int i = 0; i < 2 * (int) cap; i++) {
                adaptive.computeIfAbsent(i, k -> Boolean.TRUE);
            }
            long afterAdaptive = usedHeap();
            double stockPerEntry = (afterStock - base) / (double) stock.count();
            double adaptivePerEntry = (afterAdaptive - afterStock) / (double) adaptive.count();
            logger.info(
                String.format(
                    Locale.ROOT,
                    "MEMORY entries=%d: stock %.0f bytes/entry  adaptive %.0f bytes/entry  (x%.2f, +%.0f bytes/entry)%n",
                    stock.count(),
                    stockPerEntry,
                    adaptivePerEntry,
                    adaptivePerEntry / stockPerEntry,
                    adaptivePerEntry - stockPerEntry
                )
            );
            assertThat(stock.count() > 0 && adaptive.count() > 0, org.hamcrest.Matchers.is(true));
        }
    }

    private static double median(double[] values) {
        double[] measured = java.util.Arrays.copyOfRange(values, 2, values.length);
        java.util.Arrays.sort(measured);
        return measured[measured.length / 2];
    }

    private static long usedHeap() {
        for (int i = 0; i < 3; i++) {
            java.lang.management.ManagementFactory.getMemoryMXBean().gc();
        }
        Runtime rt = Runtime.getRuntime();
        return rt.totalMemory() - rt.freeMemory();
    }
}
