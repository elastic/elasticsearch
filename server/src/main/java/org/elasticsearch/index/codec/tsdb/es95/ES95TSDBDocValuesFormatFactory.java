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
public final class ES95TSDBDocValuesFormatFactory {

    static final int BINARY_BLOCK_BYTES_SMALL = 128 * 1024;
    static final int BINARY_BLOCK_COUNT_SMALL = 1024;
    static final int BINARY_BLOCK_BYTES_LARGE = 512 * 1024;
    static final int BINARY_BLOCK_COUNT_LARGE = 8096;

    private ES95TSDBDocValuesFormatFactory() {}

    /**
     * Creates an ES95 doc values format with block sizes matching the given settings.
     *
     * @param useLargeNumericBlockSize whether to use 512-value numeric blocks (vs 128)
     * @param useLargeBinaryBlockSize  whether to use large binary block thresholds (512KB/8096 vs 128KB/1024)
     * @param writePartitions          whether to write prefix-partitioned sorted fields
     * @return the configured format
     */
    public static DocValuesFormat createDocValuesFormat(
        boolean useLargeNumericBlockSize,
        boolean useLargeBinaryBlockSize,
        boolean writePartitions
    ) {
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
}
