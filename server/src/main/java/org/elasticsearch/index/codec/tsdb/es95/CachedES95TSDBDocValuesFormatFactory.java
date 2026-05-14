/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb.es95;

import org.apache.lucene.codecs.DocValuesFormat;
import org.elasticsearch.index.codec.tsdb.BinaryDVCompressionMode;
import org.elasticsearch.index.codec.tsdb.pipeline.numeric.NumericCodecFactory;

/**
 * Factory for creating {@link ES95TSDBDocValuesFormat} instances with block size
 * configuration matching index settings.
 */
public final class CachedES95TSDBDocValuesFormatFactory {

    static final int BINARY_BLOCK_BYTES_SMALL = 128 * 1024;
    static final int BINARY_BLOCK_COUNT_SMALL = 1024;
    static final int BINARY_BLOCK_BYTES_LARGE = 512 * 1024;
    static final int BINARY_BLOCK_COUNT_LARGE = 8096;

    // NOTE: flat 8 element array (vs [2][2][2]) so createDocValuesFormat is one
    // bounds checked load instead of three dependent loads on the per field hot path.
    private static final DocValuesFormat[] INSTANCES = buildInstances();

    private CachedES95TSDBDocValuesFormatFactory() {}

    private static DocValuesFormat[] buildInstances() {
        final DocValuesFormat[] cache = new DocValuesFormat[8];
        for (int n = 0; n < 2; n++) {
            for (int b = 0; b < 2; b++) {
                for (int p = 0; p < 2; p++) {
                    cache[(n << 2) | (b << 1) | p] = build(n == 1, b == 1, p == 1);
                }
            }
        }
        return cache;
    }

    private static DocValuesFormat build(boolean useLargeNumericBlockSize, boolean useLargeBinaryBlockSize, boolean writePartitions) {
        final int numericBlockShift = useLargeNumericBlockSize
            ? ES95TSDBDocValuesFormat.NUMERIC_LARGE_BLOCK_SHIFT
            : ES95TSDBDocValuesFormat.NUMERIC_BLOCK_SHIFT;
        final int blockBytesThreshold = useLargeBinaryBlockSize ? BINARY_BLOCK_BYTES_LARGE : BINARY_BLOCK_BYTES_SMALL;
        final int blockCountThreshold = useLargeBinaryBlockSize ? BINARY_BLOCK_COUNT_LARGE : BINARY_BLOCK_COUNT_SMALL;
        return new ES95TSDBDocValuesFormat(
            ES95TSDBDocValuesFormat.DEFAULT_SKIP_INDEX_INTERVAL_SIZE,
            ES95TSDBDocValuesFormat.ORDINAL_RANGE_ENCODING_MIN_DOC_PER_ORDINAL,
            true,
            BinaryDVCompressionMode.COMPRESSED_ZSTD_LEVEL_1,
            true,
            numericBlockShift,
            writePartitions,
            blockBytesThreshold,
            blockCountThreshold,
            NumericCodecFactory.DEFAULT,
            ES95NumericFieldReader::defaultFallbackDecoder
        );
    }

    /**
     * Returns a cached ES95 doc values format matching the given settings.
     *
     * @param useLargeNumericBlockSize whether to use numeric blocks of 512 values (vs 128)
     * @param useLargeBinaryBlockSize  whether to use large binary block thresholds (512KB/8096 vs 128KB/1024)
     * @param writePartitions          whether to write prefix partitioned sorted fields
     * @return the configured format (shared, do not mutate)
     */
    public static DocValuesFormat createDocValuesFormat(
        boolean useLargeNumericBlockSize,
        boolean useLargeBinaryBlockSize,
        boolean writePartitions
    ) {
        final int idx = (useLargeNumericBlockSize ? 4 : 0) | (useLargeBinaryBlockSize ? 2 : 0) | (writePartitions ? 1 : 0);
        return INSTANCES[idx];
    }

    /**
     * Allocates a fresh ES95 doc values format with the given settings, bypassing the
     * instance cache. Intended for benchmarks that want to measure the wall clock cost
     * of bypassing the cache (the pre cache fix behavior); production code should call
     * {@link #createDocValuesFormat} instead.
     *
     * @param useLargeNumericBlockSize same semantics as {@link #createDocValuesFormat}
     * @param useLargeBinaryBlockSize  same semantics as {@link #createDocValuesFormat}
     * @param writePartitions          same semantics as {@link #createDocValuesFormat}
     * @return a freshly allocated format with the requested parameters
     */
    public static DocValuesFormat createDocValuesFormatUncached(
        boolean useLargeNumericBlockSize,
        boolean useLargeBinaryBlockSize,
        boolean writePartitions
    ) {
        return build(useLargeNumericBlockSize, useLargeBinaryBlockSize, writePartitions);
    }
}
