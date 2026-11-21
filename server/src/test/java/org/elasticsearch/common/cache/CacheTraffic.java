/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.common.cache;

import java.util.List;
import java.util.SplittableRandom;

/**
 * Synthetic request streams, a Java port of the traffic model used to evaluate the adaptive
 * eviction in simulation. Requests come from three components: Z = Zipf over {@link #N} popular keys (whose ranking may drift
 * or be re-drawn), S = a scan of keys that are never re-used, and R = fresh keys that are re-referenced soon after first use
 * (temporal locality without long-run popularity).
 */
final class CacheTraffic {
    static final int N = 50_000;
    static final int SCAN = 400_000;
    static final int FRESH = 600_000;
    static final int OFF_SCAN = N;
    static final int OFF_FRESH = N + SCAN;
    private static final int BLOCK = 1_000;
    private static final int RECENCY_WINDOW = 1_500;
    private static final double RECENCY_NEW = 0.12;

    record Phase(int length, double zipf, double scan, double recency, String event) {}

    final String name;
    final int[] keys;

    CacheTraffic(String name, List<Phase> phases, long seed, double alpha, int driftSwaps, int shiftEvery) {
        this.name = name;
        int total = 0;
        for (Phase p : phases) {
            total += p.length();
        }
        keys = new int[total];
        final double[] zipfProbs = new double[N];
        double sum = 0;
        for (int i = 0; i < N; i++) {
            zipfProbs[i] = 1.0 / Math.pow(i + 1, alpha);
            sum += zipfProbs[i];
        }
        double[] cdf = new double[N];
        double acc = 0;
        for (int i = 0; i < N; i++) {
            zipfProbs[i] /= sum;
            acc += zipfProbs[i];
            cdf[i] = acc;
        }
        SplittableRandom rnd = new SplittableRandom(seed);
        int[] perm = shuffled(rnd);                         // rank -> key
        int[] ring = new int[RECENCY_WINDOW];
        int ringSize = 0;
        int ringPos = 0;
        int scanPtr = 0;
        int freshNext = 0;
        int t = 0;
        for (Phase ph : phases) {
            final int end = t + ph.length();
            while (t < end) {
                final int m = Math.min(BLOCK, end - t);
                if ("drift".equals(ph.event())) {
                    for (int s = 0; s < driftSwaps; s++) {
                        int i = rnd.nextInt(N);
                        int j = rnd.nextInt(N);
                        int tmp = perm[i];
                        perm[i] = perm[j];
                        perm[j] = tmp;
                    }
                }
                if ("shift".equals(ph.event()) && shiftEvery > 0 && (t % shiftEvery) < BLOCK) {
                    perm = shuffled(rnd);
                }
                for (int q = 0; q < m; q++) {
                    double u = rnd.nextDouble();
                    int key;
                    if (u < ph.zipf()) {
                        int lo = 0;
                        int hi = N - 1;
                        double x = rnd.nextDouble();
                        while (lo < hi) {
                            int mid = (lo + hi) >>> 1;
                            if (cdf[mid] < x) lo = mid + 1;
                            else hi = mid;
                        }
                        key = perm[lo];
                    } else if (u < ph.zipf() + ph.scan()) {
                        key = OFF_SCAN + (scanPtr++ % SCAN);
                    } else {
                        if (ringSize == 0 || rnd.nextDouble() < RECENCY_NEW) {
                            key = OFF_FRESH + (freshNext++ % FRESH);
                        } else {
                            key = ring[rnd.nextInt(ringSize)];
                        }
                        ring[ringPos] = key;
                        ringPos = (ringPos + 1) % RECENCY_WINDOW;
                        ringSize = Math.min(RECENCY_WINDOW, ringSize + 1);
                    }
                    keys[t + q] = key;
                }
                t += m;
            }
        }
    }

    private static int[] shuffled(SplittableRandom rnd) {
        int[] p = new int[N];
        for (int i = 0; i < N; i++) {
            p[i] = i;
        }
        for (int i = N - 1; i > 0; i--) {
            int j = rnd.nextInt(i + 1);
            int tmp = p[i];
            p[i] = p[j];
            p[j] = tmp;
        }
        return p;
    }

    static CacheTraffic zipf(int length, long seed) {
        return new CacheTraffic("stationary zipf", List.of(new Phase(length, 1, 0, 0, null)), seed, 0.9, 0, 0);
    }

    static CacheTraffic drift(int length, long seed) {
        return new CacheTraffic("slow drift", List.of(new Phase(length, 1, 0, 0, "drift")), seed, 0.9, 25, 0);
    }

    static CacheTraffic shift(int length, long seed) {
        return new CacheTraffic("abrupt re-rank", List.of(new Phase(length, 1, 0, 0, "shift")), seed, 0.9, 0, length / 6);
    }

    static CacheTraffic scans(int length, long seed) {
        return new CacheTraffic("zipf + scans", List.of(new Phase(length, 0.65, 0.35, 0, null)), seed, 0.9, 0, 0);
    }

    static CacheTraffic recency(int length, long seed) {
        return new CacheTraffic("recency only", List.of(new Phase(length, 0, 0, 1, null)), seed, 0.9, 0, 0);
    }

    static CacheTraffic phaseMix(int length, long seed) {
        int q = length / 4;
        return new CacheTraffic(
            "phase mix",
            List.of(
                new Phase(q, 1, 0, 0, null),
                new Phase(q, 0, 0, 1, null),
                new Phase(q, 0.65, 0.35, 0, null),
                new Phase(q, 0.4, 0, 0.6, "shift")
            ),
            seed,
            0.9,
            0,
            40_000
        );
    }
}
