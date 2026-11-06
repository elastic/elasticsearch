/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.sketch;

/**
 * A small Bloom filter of the keys of one buffered page. After the proof only a few keys can be in the top N, and in the sorted
 * index most pages have no row of those keys, so a page whose Bloom filter has none of them does not need to be read. Reading the
 * buffered pages, which are cold in memory by then, is most of the time of the filter phase.
 * <p>
 * A Bloom filter has no false negatives: every key of the page must be added, including the first one (see
 * {@link CountSketch#addAll(long[], int, long[], int)}), or a page with candidate rows could be skipped and the result would be wrong.
 * Two bits are set per key. The size is a power of two bits chosen from the number of rows of the page, and is described by the
 * {@code shift} that turns a 64-bit hash into a bit index.
 */
final class PageBloom {
    private static final long H1 = 0xC2B2AE3D27D4EB4FL;
    private static final long H2 = 0x9E3779B97F4A7C15L;
    private static final int BITS_PER_ROW = 6;
    private static final int MIN_BITS_LOG2 = 10;
    private static final int MAX_BITS_LOG2 = 20;

    private PageBloom() {}

    /** The hash shift for a page of {@code positions} rows. The filter has {@code 2^(64 - shift)} bits. */
    static int shiftFor(int positions) {
        final long wanted = Math.max(2, (long) positions * BITS_PER_ROW);
        final int bitsLog2 = Math.clamp(64 - Long.numberOfLeadingZeros(wanted - 1), MIN_BITS_LOG2, MAX_BITS_LOG2);
        return 64 - bitsLog2;
    }

    static long[] create(int shift) {
        return new long[1 << (64 - shift - 6)];
    }

    static void add(long[] bloom, int shift, long key) {
        final long h = key * H1;
        int p = (int) (h >>> shift);
        bloom[p >>> 6] |= 1L << p;
        p = (int) ((h * H2) >>> shift);
        bloom[p >>> 6] |= 1L << p;
    }

    static boolean mayContain(long[] bloom, int shift, long key) {
        final long h = key * H1;
        int p = (int) (h >>> shift);
        if ((bloom[p >>> 6] & (1L << p)) == 0) {
            return false;
        }
        p = (int) ((h * H2) >>> shift);
        return (bloom[p >>> 6] & (1L << p)) != 0;
    }
}
