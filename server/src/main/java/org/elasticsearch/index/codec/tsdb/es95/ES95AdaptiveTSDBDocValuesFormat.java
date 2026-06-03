/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb.es95;

import org.elasticsearch.index.codec.tsdb.BinaryDVCompressionMode;
import org.elasticsearch.index.codec.tsdb.pipeline.numeric.NumericCodecFactory;

/**
 * Adaptive ordinal variant of {@link ES95TSDBDocValuesFormat}. Identical to the
 * base format except that ordinal blocks are encoded by per-field-type adaptive
 * subclasses ({@link ES95SortedOrdinalBlockCodec},
 * {@link ES95SortedSetOrdinalBlockCodec}) that pick the cheapest of CONST, RLE,
 * BITPACK_LOCAL, or BIT_PACKED mode instead of the legacy fixed-width bit packing
 * provided by {@link org.elasticsearch.index.codec.tsdb.TSDBSortedOrdinalBlockCodec}
 * and {@link org.elasticsearch.index.codec.tsdb.TSDBSortedSetOrdinalBlockCodec}.
 *
 * <p>Carries a distinct {@link #CODEC_NAME} so that segments written with the
 * adaptive ordinal block path are decoded back through the same path after
 * Lucene SPI re instantiates the format on read.
 *
 * <p>TODO: this distinct codec name approach is a POC testing shortcut for the
 * adaptive ordinal blocks feature. Before shipping we must replace this with a
 * proper {@code IndexVersions} gate plus codec version dispatch inside the
 * existing {@link ES95TSDBDocValuesFormat} (single codec name, version byte in
 * the {@code .dvm} header). The current setup means segments written by this
 * format are not readable by nodes that lack
 * {@code ES95AdaptiveTSDBDocValuesFormat} in their SPI registry, which is fine
 * while the feature is gated behind the snapshot build feature flag but
 * unacceptable for a GA rollout.
 */
public final class ES95AdaptiveTSDBDocValuesFormat extends ES95TSDBDocValuesFormat {

    static final String CODEC_NAME = "ES95TSDBAdaptive";

    /** SPI no arg constructor. */
    public ES95AdaptiveTSDBDocValuesFormat() {
        super(
            CODEC_NAME,
            DEFAULT_SKIP_INDEX_INTERVAL_SIZE,
            ORDINAL_RANGE_ENCODING_MIN_DOC_PER_ORDINAL,
            true,
            BinaryDVCompressionMode.COMPRESSED_ZSTD_LEVEL_1,
            true,
            NUMERIC_BLOCK_SHIFT,
            false,
            BINARY_DV_BLOCK_BYTES_THRESHOLD_DEFAULT,
            BINARY_DV_BLOCK_COUNT_THRESHOLD_DEFAULT,
            NumericCodecFactory.DEFAULT,
            ES95NumericFieldReader::defaultFallbackDecoder,
            true
        );
    }

    /** Factory entry point mirroring the parent constructor surface. */
    ES95AdaptiveTSDBDocValuesFormat(
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
        final FallbackDecoderFactory fallbackDecoderFactory
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
            true
        );
    }
}
