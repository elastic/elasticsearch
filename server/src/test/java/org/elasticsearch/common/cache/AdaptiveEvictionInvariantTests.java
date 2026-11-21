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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.lessThanOrEqualTo;

/**
 * Hammers a cache in adaptive mode with random operations and checks that the controller's primitive-array bookkeeping (key ids,
 * the shadow caches' lists, the table of real entries and its outside list) stays consistent with itself.
 */
public class AdaptiveEvictionInvariantTests extends ESTestCase {

    private static Cache<Integer, Integer> newCache(long maxWeight, int sketchWidth, long seed) {
        AdaptiveEviction.Builder<Integer> adaptive = AdaptiveEviction.<Integer>builder().seed(seed).sketchWidth(sketchWidth);
        return CacheBuilder.<Integer, Integer>builder()
            .setMaximumWeight(maxWeight)
            .weigher((k, v) -> 1 + (k % 9))
            .setAdaptiveEviction(adaptive.build())
            .build();
    }

    private static int zipfishKey(java.util.Random random, int keySpace) {
        // heavy head, long tail, plus a stream of never-repeated keys
        final double r = random.nextDouble();
        if (r < 0.1) {
            return keySpace + random.nextInt(1 << 20);
        }
        return (int) (keySpace * Math.pow(random.nextDouble(), 3));
    }

    private void randomOperations(Cache<Integer, Integer> cache, java.util.Random random, int operations, int keySpace, boolean check)
        throws Exception {
        for (int i = 0; i < operations; i++) {
            final int key = zipfishKey(random, keySpace);
            final int op = random.nextInt(1000);
            if (op < 800) {
                cache.computeIfAbsent(key, k -> k);
            } else if (op < 900) {
                cache.get(key);
            } else if (op < 995) {
                cache.invalidate(key);
            } else if (op < 999) {
                cache.put(key, key);
            } else {
                cache.invalidateAll();
            }
            if (check && i % 500 == 0) {
                cache.checkAdaptiveInvariants();
            }
        }
    }

    public void testRandomOperationsKeepBookkeepingConsistent() throws Exception {
        for (int round = 0; round < 12; round++) {
            final long maxWeight = randomFrom(20L, 100L, 400L, 2_000L);
            final int sketchWidth = randomFrom(16, 64, 1024, 1 << 16);
            final Cache<Integer, Integer> cache = newCache(maxWeight, sketchWidth, randomLong());
            randomOperations(cache, new java.util.Random(randomLong()), 60_000, randomFrom(50, 500, 5_000), true);
            cache.checkAdaptiveInvariants();
            assertThat(cache.weight(), lessThanOrEqualTo(maxWeight));
        }
    }

    public void testConcurrentOperationsKeepBookkeepingConsistent() throws Exception {
        final long maxWeight = randomFrom(100L, 1_000L);
        final Cache<Integer, Integer> cache = newCache(maxWeight, randomFrom(64, 1 << 12), randomLong());
        final int threads = 6;
        final CountDownLatch start = new CountDownLatch(1);
        final AtomicReference<Throwable> failure = new AtomicReference<>();
        final List<Thread> workers = new ArrayList<>();
        for (int t = 0; t < threads; t++) {
            final java.util.Random random = new java.util.Random(randomLong());
            final Thread thread = new Thread(() -> {
                try {
                    start.await();
                    randomOperations(cache, random, 40_000, 800, false);
                } catch (Throwable e) {
                    failure.compareAndSet(null, e);
                }
            });
            workers.add(thread);
            thread.start();
        }
        start.countDown();
        for (Thread thread : workers) {
            thread.join();
        }
        if (failure.get() != null) {
            throw new AssertionError(failure.get());
        }
        cache.checkAdaptiveInvariants();
        assertThat(cache.weight(), lessThanOrEqualTo(maxWeight));
    }
}
