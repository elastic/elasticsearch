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
import java.util.SplittableRandom;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class AdaptiveEvictionTests extends ESTestCase {

    private static Cache<Integer, Integer> lruCache(long max) {
        return CacheBuilder.<Integer, Integer>builder().setMaximumWeight(max).build();
    }

    private static Cache<Integer, Integer> adaptiveCache(long max, AdaptiveEviction<Integer> config) {
        return CacheBuilder.<Integer, Integer>builder().setMaximumWeight(max).setAdaptiveEviction(config).build();
    }

    private static AdaptiveEviction<Integer> noPopularity() {
        return AdaptiveEviction.<Integer>builder().seed(1).build();
    }

    private static void touch(Cache<Integer, Integer> cache, int key) throws Exception {
        cache.computeIfAbsent(key, k -> k);
    }

    private static double hitRatio(Cache<Integer, Integer> cache) {
        Cache.Stats s = cache.stats();
        return (double) s.getHits() / (s.getHits() + s.getMisses());
    }

    public void testDisabledByDefault() {
        assertThat(lruCache(10).adaptiveStats(), is(nullValue()));
    }

    public void testRequiresMaximumWeight() {
        expectThrows(
            IllegalStateException.class,
            () -> CacheBuilder.<Integer, Integer>builder().setAdaptiveEviction(noPopularity()).build()
        );
    }

    public void testBuilderValidation() {
        expectThrows(IllegalArgumentException.class, () -> AdaptiveEviction.<Integer>builder().eta(0));
        expectThrows(IllegalArgumentException.class, () -> AdaptiveEviction.<Integer>builder().alpha(1));
        expectThrows(IllegalArgumentException.class, () -> AdaptiveEviction.<Integer>builder().sketchWidth(100));
    }

    /** With weights the cache must never exceed its maximum weight, and removal notifications must account for every entry. */
    public void testWeightBoundAndNotifications() throws Exception {
        final long max = 200;
        final AtomicLong removedWeight = new AtomicLong();
        final AtomicLong removedCount = new AtomicLong();
        Cache<Integer, Integer> cache = CacheBuilder.<Integer, Integer>builder()
            .setMaximumWeight(max)
            .weigher((k, v) -> 1 + (k % 7))
            .removalListener(n -> {
                removedWeight.addAndGet(1 + (n.getKey() % 7));
                removedCount.incrementAndGet();
            })
            .setAdaptiveEviction(AdaptiveEviction.<Integer>builder().seed(3).build())
            .build();
        SplittableRandom rnd = new SplittableRandom(42);
        long inserted = 0;
        long insertedWeight = 0;
        for (int i = 0; i < 50_000; i++) {
            int key = (int) (Math.pow(rnd.nextDouble(), 3) * 3000);
            final boolean[] loaded = { false };
            cache.computeIfAbsent(key, k -> {
                loaded[0] = true;
                return k;
            });
            if (loaded[0]) {
                inserted++;
                insertedWeight += 1 + (key % 7);
            }
            assertThat(cache.weight(), lessThanOrEqualTo(max));
        }
        assertThat(inserted, is(removedCount.get() + cache.count()));
        assertThat(insertedWeight, is(removedWeight.get() + cache.weight()));
    }

    public void testReplaceInvalidateAndClear() throws Exception {
        Cache<Integer, Integer> cache = adaptiveCache(100, noPopularity());
        for (int i = 0; i < 500; i++) {
            cache.put(i % 150, i);
            if (i % 11 == 0) {
                cache.invalidate(i % 150);
            }
            if (i % 97 == 0) {
                cache.invalidate((i + 1) % 150, -1);   // value mismatch: no-op
            }
            assertThat(cache.weight(), lessThanOrEqualTo(100L));
        }
        cache.invalidateAll();
        assertThat(cache.count(), is(0));
        assertThat(cache.weight(), is(0L));
        for (int i = 0; i < 1000; i++) {
            touch(cache, i % 300);
        }
        assertThat(cache.weight(), lessThanOrEqualTo(100L));
        assertThat(cache.adaptiveStats(), is(notNullValue()));
    }

    /** On traffic where recency is all that matters the adaptive cache should be about as good as LRU. */
    public void testRecencyTrafficIsAsGoodAsLru() throws Exception {
        Cache<Integer, Integer> lru = lruCache(500);
        Cache<Integer, Integer> adaptive = adaptiveCache(500, noPopularity());
        SplittableRandom rnd = new SplittableRandom(7);
        int[] recent = new int[400];
        int next = 0;
        int size = 0;
        for (int i = 0; i < 150_000; i++) {
            int key;
            if (size == 0 || rnd.nextDouble() < 0.12) {
                key = next++;
            } else {
                key = recent[rnd.nextInt(size)];
            }
            recent[i % recent.length] = key;
            size = Math.min(recent.length, size + 1);
            touch(lru, key);
            touch(adaptive, key);
        }
        assertThat(hitRatio(adaptive), greaterThanOrEqualTo(hitRatio(lru) - 0.01));
    }

    /** On a skewed independent-reference workload frequency matters; plain LRU is clearly beaten. */
    public void testSkewedTrafficBeatsLru() throws Exception {
        final int n = 20_000;
        double[] cdf = new double[n];
        double sum = 0;
        for (int i = 0; i < n; i++) {
            sum += 1.0 / Math.pow(i + 1, 0.9);
            cdf[i] = sum;
        }
        Cache<Integer, Integer> lru = lruCache(600);
        Cache<Integer, Integer> adaptive = adaptiveCache(600, noPopularity());
        SplittableRandom rnd = new SplittableRandom(11);
        for (int i = 0; i < 300_000; i++) {
            double u = rnd.nextDouble() * sum;
            int lo = 0, hi = n - 1;
            while (lo < hi) {
                int mid = (lo + hi) >>> 1;
                if (cdf[mid] < u) lo = mid + 1;
                else hi = mid;
            }
            touch(lru, lo);
            touch(adaptive, lo);
        }
        assertThat(hitRatio(adaptive), greaterThan(hitRatio(lru) + 0.03));
    }

    /** When the traffic changes character the followed policy changes. */
    public void testTrustMovesWhenTrafficChanges() throws Exception {
        final int n = 5_000;
        Cache<Integer, Integer> adaptive = adaptiveCache(300, AdaptiveEviction.<Integer>builder().seed(9).eta(0.5).alpha(1e-3).build());
        SplittableRandom rnd = new SplittableRandom(13);
        // phase 1: skewed popularity (frequency policy should win); phase 2: strict recency (LRU should win)
        for (int i = 0; i < 100_000; i++) {
            touch(adaptive, (int) (Math.pow(rnd.nextDouble(), 4) * n));
        }
        AdaptiveEviction.Stats afterSkew = adaptive.adaptiveStats();
        int[] recent = new int[200];
        int next = 100_000;
        for (int i = 0; i < 100_000; i++) {
            int key = (i < 5 || rnd.nextDouble() < 0.15) ? next++ : recent[rnd.nextInt(Math.min(i, recent.length))];
            recent[i % recent.length] = key;
            touch(adaptive, key);
        }
        AdaptiveEviction.Stats end = adaptive.adaptiveStats();
        assertThat(end.switches(), greaterThan(afterSkew.switches()));
        double lruShareLate = end.followShare()[0] * end.accesses() - afterSkew.followShare()[0] * afterSkew.accesses();
        assertThat(lruShareLate, greaterThan(0.5 * 100_000));
    }

    public void testConcurrentAccess() throws Exception {
        final long max = 400;
        Cache<Integer, Integer> cache = CacheBuilder.<Integer, Integer>builder()
            .setMaximumWeight(max)
            .weigher((k, v) -> 1 + (k % 3))
            .setAdaptiveEviction(AdaptiveEviction.<Integer>builder().seed(2).build())
            .build();
        final int threads = 8;
        final CountDownLatch start = new CountDownLatch(1);
        final List<Thread> ts = new ArrayList<>();
        final List<Throwable> failures = java.util.Collections.synchronizedList(new ArrayList<>());
        for (int t = 0; t < threads; t++) {
            final long seed = t + 100;
            Thread th = new Thread(() -> {
                try {
                    start.await();
                    SplittableRandom r = new SplittableRandom(seed);
                    for (int i = 0; i < 40_000; i++) {
                        int key = (int) (Math.pow(r.nextDouble(), 3) * 5000);
                        int op = r.nextInt(100);
                        if (op < 90) cache.computeIfAbsent(key, k -> k);
                        else if (op < 95) cache.put(key, key);
                        else if (op < 99) cache.invalidate(key);
                        else cache.refresh();
                    }
                } catch (Throwable e) {
                    failures.add(e);
                }
            });
            ts.add(th);
            th.start();
        }
        start.countDown();
        for (Thread th : ts) {
            th.join();
        }
        assertThat(failures.toString(), failures.size(), is(0));
        cache.refresh();
        assertThat(cache.weight(), lessThanOrEqualTo(max));
        long w = 0;
        int c = 0;
        for (Integer k : cache.keys()) {
            w += 1 + (k % 3);
            c++;
        }
        assertThat(c, is(cache.count()));
        assertThat(w, is(cache.weight()));
    }
}
