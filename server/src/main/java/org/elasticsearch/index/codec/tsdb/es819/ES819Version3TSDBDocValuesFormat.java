/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb.es819;

import org.elasticsearch.index.codec.tsdb.BinaryDVCompressionMode;

/**
 * Version 3 of {@link org.elasticsearch.index.codec.tsdb.es819.ES819TSDBDocValuesFormat} and has the following change:
 * <ul>
 *     <li>Changed how binary doc values encodes docOffsets from grouped vints to bitpacking</li>
 * </ul>
 *
 * Note that versions 0, 1, and 2 are implemented as a codec version in {@link ES819TSDBDocValuesFormat}.
 * However, codec versions don't work correctly in a mixed cluster or in stateless. There is no logic that prevents shards from
 * allocated with a new codec version to an older node that doesn't know about a new codec version. Only index versions prevent this
 * from happening.
 */
public class ES819Version3TSDBDocValuesFormat extends ES819TSDBDocValuesFormat {

    static final String CODEC_NAME = "ES8193TSDB";

    public ES819Version3TSDBDocValuesFormat() {
        super(
            CODEC_NAME,
            DEFAULT_SKIP_INDEX_INTERVAL_SIZE,
            ORDINAL_RANGE_ENCODING_MIN_DOC_PER_ORDINAL,
            OPTIMIZED_MERGE_ENABLE_DEFAULT,
            BinaryDVCompressionMode.COMPRESSED_ZSTD_LEVEL_1,
            true,
            NUMERIC_BLOCK_SHIFT,
            DocOffsetsCodec.BITPACKING
        );
    }

    public ES819Version3TSDBDocValuesFormat(boolean useLargeNumericBlock) {
        super(
            CODEC_NAME,
            DEFAULT_SKIP_INDEX_INTERVAL_SIZE,
            ORDINAL_RANGE_ENCODING_MIN_DOC_PER_ORDINAL,
            OPTIMIZED_MERGE_ENABLE_DEFAULT,
            BinaryDVCompressionMode.COMPRESSED_ZSTD_LEVEL_1,
            true,
            useLargeNumericBlock ? NUMERIC_LARGE_BLOCK_SHIFT : NUMERIC_BLOCK_SHIFT,
            DocOffsetsCodec.BITPACKING
        );
    }

    public ES819Version3TSDBDocValuesFormat(
        int skipIndexIntervalSize,
        int minDocsPerOrdinalForRangeEncoding,
        boolean enableOptimizedMerge,
        BinaryDVCompressionMode binaryDVCompressionMode,
        final boolean enablePerBlockCompression,
        int numericBlockShift
    ) {
        super(
            CODEC_NAME,
            skipIndexIntervalSize,
            minDocsPerOrdinalForRangeEncoding,
            enableOptimizedMerge,
            binaryDVCompressionMode,
            enablePerBlockCompression,
            numericBlockShift,
            DocOffsetsCodec.BITPACKING
        );
    }
}
