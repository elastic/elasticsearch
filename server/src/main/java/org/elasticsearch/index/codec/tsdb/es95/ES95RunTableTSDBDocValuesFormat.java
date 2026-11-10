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
import org.elasticsearch.index.codec.tsdb.pipeline.FieldContextResolver;
import org.elasticsearch.index.codec.tsdb.pipeline.numeric.NumericCodecFactory;

/**
 * Variant of {@link ES95TSDBDocValuesFormat} that writes dimension fields in the run-table ordinal layout:
 * single-valued ones through {@link RunTableSortedCodec} and multi-valued {@code SortedSet} ones through
 * {@link RunTableSortedSetCodec}.
 *
 * <p>It needs its own name ({@value #CODEC_NAME}) because Lucene picks the reading format from the name
 * stored in the segment, and every sorted ordinal stream this format writes starts with the layout
 * discriminator that {@link ES95TSDBDocValuesFormat} segments lack. The format is registered for SPI
 * lookup unconditionally, so a segment written with it stays readable whatever the
 * {@code tsdb_run_table_ordinals} feature flag says; the flag only decides whether new segments use it.
 */
public final class ES95RunTableTSDBDocValuesFormat extends ES95TSDBDocValuesFormat {

    static final String CODEC_NAME = "ES95RTTSDB";

    /**
     * Required by SPI to read segments written with this format. Without a field context resolver the
     * writer would never pick the run-table layout, so instances created this way are only used to read.
     */
    public ES95RunTableTSDBDocValuesFormat() {
        this(NUMERIC_BLOCK_SHIFT, false, BINARY_DV_BLOCK_BYTES_THRESHOLD_DEFAULT, BINARY_DV_BLOCK_COUNT_THRESHOLD_DEFAULT, null);
    }

    ES95RunTableTSDBDocValuesFormat(
        int numericBlockShift,
        boolean writePrefixPartitions,
        int blockBytesThreshold,
        int blockCountThreshold,
        @Nullable final FieldContextResolver fieldContextResolver
    ) {
        super(
            CODEC_NAME,
            DEFAULT_SKIP_INDEX_INTERVAL_SIZE,
            ORDINAL_RANGE_ENCODING_MIN_DOC_PER_ORDINAL,
            true,
            BinaryDVCompressionMode.COMPRESSED_ZSTD_LEVEL_1,
            true,
            numericBlockShift,
            writePrefixPartitions,
            blockBytesThreshold,
            blockCountThreshold,
            NumericCodecFactory.DEFAULT,
            ES95NumericFieldReader::defaultFallbackDecoder,
            fieldContextResolver,
            new RunTableSortedCodec(SORTED_CODEC, fieldContextResolver),
            new RunTableSortedSetCodec(SORTED_SET_CODEC, fieldContextResolver)
        );
    }
}
