/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb.es95;

/**
 * Single-pass scan statistics over a block of ordinal values. Reused across blocks by an
 * enclosing ordinal codec instance; {@link #recompute} resets all fields. Tracks per-run
 * ordinals and run lengths up to {@link #MAX_TRACKED_RUNS} entries; sets {@code nRuns} to
 * {@code MAX_TRACKED_RUNS + 1} as a sentinel once the cap is exceeded so RLE selection can
 * be cheaply skipped.
 *
 * <p>{@link #recomputeWithCycle} additionally runs cycle detection so a SORTED_SET codec
 * can pick {@link CycleCodec} when the flat ord stream repeats with period in
 * {@code [2, blockSize / MAX_CYCLE_DIVISOR]}. The plain {@link #recompute} method skips
 * cycle detection and is used by the SORTED path, which never sees this pattern.
 */
final class BlockStats {

    static final int MAX_TRACKED_RUNS = 16;

    /**
     * Cycle periods larger than {@code blockSize / MAX_CYCLE_DIVISOR} are not considered.
     * With blockSize 128 and divisor 8 the cap is 16, which covers typical multi-valued
     * ord patterns (e.g. a few IPs per host) while keeping a margin against false positives
     * from coincidental short-prefix matches.
     */
    static final int MAX_CYCLE_DIVISOR = 8;

    long min;
    long max;
    boolean allSame;
    int nRuns;
    final long[] runOrds = new long[MAX_TRACKED_RUNS + 1];
    final int[] runLens = new int[MAX_TRACKED_RUNS + 1];
    /** Detected cycle period, or 0 if no cycle was found or detection was not run. */
    int cycleLength;

    void recompute(final long[] in) {
        recomputeRunStats(in);
        cycleLength = 0;
    }

    void recomputeWithCycle(final long[] in) {
        recomputeRunStats(in);
        cycleLength = nRuns > 2 ? detectCycle(in) : 0;
    }

    private void recomputeRunStats(final long[] in) {
        long first = in[0];
        min = first;
        max = first;
        allSame = true;
        nRuns = 1;
        runOrds[0] = first;
        runLens[0] = 1;
        for (int i = 1; i < in.length; i++) {
            long v = in[i];
            if (v != first) {
                allSame = false;
            }
            if (v < min) {
                min = v;
            }
            if (v > max) {
                max = v;
            }
            if (v == in[i - 1]) {
                if (nRuns <= MAX_TRACKED_RUNS) {
                    runLens[nRuns - 1]++;
                }
            } else if (nRuns < MAX_TRACKED_RUNS + 1) {
                runOrds[nRuns] = v;
                runLens[nRuns] = 1;
                nRuns++;
            } else {
                nRuns = MAX_TRACKED_RUNS + 1;
            }
        }
    }

    private static int detectCycle(final long[] in) {
        final int maxPeriod = in.length / MAX_CYCLE_DIVISOR;
        int candidate = 0;
        for (int i = 1; i < in.length; i++) {
            if (in[i] == in[0]) {
                if (candidate == 0) {
                    candidate = i;
                } else if (i % candidate != 0) {
                    return 0;
                }
            }
        }
        if (candidate < 2 || candidate > maxPeriod) {
            return 0;
        }
        for (int i = candidate; i < in.length; i++) {
            if (in[i] != in[i - candidate]) {
                return 0;
            }
        }
        return candidate;
    }
}
