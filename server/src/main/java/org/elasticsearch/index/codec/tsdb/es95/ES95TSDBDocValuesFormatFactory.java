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
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.codec.tsdb.BinaryDVCompressionMode;
import org.elasticsearch.index.codec.tsdb.pipeline.FieldContextResolver;
import org.elasticsearch.index.codec.tsdb.pipeline.numeric.NumericCodecFactory;

/**
 * Factory for creating {@link ES95TSDBDocValuesFormat} instances with block size
 * configuration matching index settings. Every call allocates a fresh format because
 * every production caller supplies a per index {@link FieldContextResolver}, which
 * closes over {@code MapperService} state and cannot be globally cached.
 * {@code PerFieldFormatSupplier} caches one format per supplier, which is the right
 * boundary for the per index per shard reuse pattern.
 */
public final class ES95TSDBDocValuesFormatFactory {

    static final int BINARY_BLOCK_BYTES_SMALL = 128 * 1024;
    static final int BINARY_BLOCK_COUNT_SMALL = 1024;
    static final int BINARY_BLOCK_BYTES_LARGE = 512 * 1024;
    static final int BINARY_BLOCK_COUNT_LARGE = 8096;

    private ES95TSDBDocValuesFormatFactory() {}

    /**
     * Allocates a fresh ES95 doc values format matching the given settings.
     *
     * @param useLargeNumericBlockSize whether to use numeric blocks of 512 values (vs 128)
     * @param useLargeBinaryBlockSize  whether to use large binary block thresholds (512KB/8096 vs 128KB/1024)
     * @param writePartitions          whether to write prefix partitioned sorted fields
     * @param fieldContextResolver     bridge from the mapper layer that supplies a
     *                                 {@link org.elasticsearch.index.codec.tsdb.pipeline.FieldContext}
     *                                 per field, or {@code null} when mapper metadata
     *                                 is not available (the codec then uses a context
     *                                 with no data type or metric type information)
     * @return a freshly allocated format with the requested parameters
     */
    public static DocValuesFormat create(
        boolean useLargeNumericBlockSize,
        boolean useLargeBinaryBlockSize,
        boolean writePartitions,
        @Nullable final FieldContextResolver fieldContextResolver
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
            ES95NumericFieldReader::defaultFallbackDecoder,
            fieldContextResolver
        );
    }
}
