/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb.es95;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.codec.tsdb.BinaryDVCompressionMode;
import org.elasticsearch.index.codec.tsdb.es95.runtable.RunTableSortedOrdinalWriter;
import org.elasticsearch.index.codec.tsdb.es95.runtable.RunTableSortedSetOrdinalWriter;
import org.elasticsearch.index.codec.tsdb.pipeline.FieldContextResolver;
import org.elasticsearch.index.codec.tsdb.pipeline.numeric.NumericCodecFactory;

/**
 * Run-table variant of {@link ES95TSDBDocValuesFormat} that installs {@link RunTableSortedCodec}
 * and {@link RunTableSortedSetCodec}, encoding sorted and sorted-set ordinals for TSDB dimension
 * fields in the run-table layout rather than the baseline blocked layout.
 *
 * <p>A distinct codec name ({@value CODEC_NAME}) is required because the Lucene SPI read path
 * selects the format class from the name stored in the segment file. Every segment written with
 * this format has a discriminator byte in every sorted ordinal metadata block; segments written
 * with {@link ES95TSDBDocValuesFormat} do not.
 */
public final class ES95RunTableTSDBDocValuesFormat extends ES95TSDBDocValuesFormat {

    static final String CODEC_NAME = "ES95RTTSDB";

    /**
     * No-argument constructor required by the Lucene SPI codec registry. Used exclusively for
     * reading segments written with this format. Run-table writes are disabled because
     * {@code fieldContextResolver} is {@code null}; all fields fall back to the baseline layout.
     */
    public ES95RunTableTSDBDocValuesFormat() {
        this(
            ES95TSDBDocValuesFormat.DEFAULT_SKIP_INDEX_INTERVAL_SIZE,
            ES95TSDBDocValuesFormat.ORDINAL_RANGE_ENCODING_MIN_DOC_PER_ORDINAL,
            true,
            BinaryDVCompressionMode.COMPRESSED_ZSTD_LEVEL_1,
            true,
            ES95TSDBDocValuesFormat.NUMERIC_BLOCK_SHIFT,
            false,
            ES95TSDBDocValuesFormat.BINARY_DV_BLOCK_BYTES_THRESHOLD_DEFAULT,
            ES95TSDBDocValuesFormat.BINARY_DV_BLOCK_COUNT_THRESHOLD_DEFAULT,
            NumericCodecFactory.DEFAULT,
            ES95NumericFieldReader::defaultFallbackDecoder,
            null
        );
    }

    ES95RunTableTSDBDocValuesFormat(
        int skipIndexIntervalSize,
        int minDocsPerOrdinalForRangeEncoding,
        boolean enableOptimizedMerge,
        final BinaryDVCompressionMode binaryDVCompressionMode,
        boolean enablePerBlockCompression,
        int numericBlockShift,
        boolean writePrefixPartitions,
        int blockBytesThreshold,
        int blockCountThreshold,
        final NumericCodecFactory numericCodecFactory,
        final FallbackDecoderFactory fallbackDecoderFactory,
        @Nullable final FieldContextResolver fieldContextResolver
    ) {
        super(
            CODEC_NAME,
            skipIndexIntervalSize,
            minDocsPerOrdinalForRangeEncoding,
            enableOptimizedMerge,
            binaryDVCompressionMode,
            enablePerBlockCompression,
            numericBlockShift,
            writePrefixPartitions,
            blockBytesThreshold,
            blockCountThreshold,
            numericCodecFactory,
            fallbackDecoderFactory,
            fieldContextResolver,
            new RunTableSortedCodec(new ES95SortedCodec(), RunTableSortedOrdinalWriter::new, fieldContextResolver),
            new RunTableSortedSetCodec(new ES95SortedSetCodec(), RunTableSortedSetOrdinalWriter::new, fieldContextResolver)
        );
    }
}
