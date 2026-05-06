/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb.es95;

import org.apache.lucene.codecs.DocValuesConsumer;
import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.elasticsearch.index.codec.tsdb.AbstractTSDBDocValuesProducer;
import org.elasticsearch.index.codec.tsdb.BinaryDVCompressionMode;
import org.elasticsearch.index.codec.tsdb.DocOffsetsCodec;
import org.elasticsearch.index.codec.tsdb.NumericBlockCodec;
import org.elasticsearch.index.codec.tsdb.OrdinalBlockCodec;
import org.elasticsearch.index.codec.tsdb.PrefixedPartitionsWriter;
import org.elasticsearch.index.codec.tsdb.SortedFieldObserver;
import org.elasticsearch.index.codec.tsdb.SortedFieldObserverFactory;
import org.elasticsearch.index.codec.tsdb.TSDBDocValuesFormatConfig;
import org.elasticsearch.index.codec.tsdb.TSDBDocValuesFormatConfig.TermsDictConfig;
import org.elasticsearch.index.codec.tsdb.TSDBOrdinalBlockCodec;
import org.elasticsearch.index.codec.tsdb.pipeline.PipelineConfig;
import org.elasticsearch.index.codec.tsdb.pipeline.numeric.NumericCodecFactory;

import java.io.IOException;

/**
 * ES95 TSDB doc values format. Uses pipeline-based encoding for numeric fields via
 * {@link ES95NumericCodec} and reuses {@link TSDBOrdinalBlockCodec} for ordinals.
 * Non-numeric field types are handled identically to ES819 by the shared abstract
 * base classes. Each numeric field writes a self-describing
 * {@link org.elasticsearch.index.codec.tsdb.pipeline.FieldDescriptor} so decoders
 * reconstruct themselves from segment metadata.
 */
public class ES95TSDBDocValuesFormat extends DocValuesFormat {

    public static final int NUMERIC_BLOCK_SHIFT = 7;
    public static final int NUMERIC_LARGE_BLOCK_SHIFT = 9;
    static final int DIRECT_MONOTONIC_BLOCK_SHIFT = 16;
    static final String CODEC_NAME = "ES95TSDB";
    static final String DATA_CODEC = "ES95TSDBDocValuesData";
    static final String DATA_EXTENSION = "dvd";
    static final String META_CODEC = "ES95TSDBDocValuesMetadata";
    static final String META_EXTENSION = "dvm";
    static final String SKIP_CODEC = "ES95TSDBDocValuesSkip";
    static final String SKIP_EXTENSION = "dvs";

    static final int TERMS_DICT_BLOCK_LZ4_SHIFT = 6;
    static final int TERMS_DICT_BLOCK_LZ4_SIZE = 1 << TERMS_DICT_BLOCK_LZ4_SHIFT;
    static final int TERMS_DICT_BLOCK_LZ4_MASK = TERMS_DICT_BLOCK_LZ4_SIZE - 1;

    static final int TERMS_DICT_REVERSE_INDEX_SHIFT = 10;
    static final int TERMS_DICT_REVERSE_INDEX_SIZE = 1 << TERMS_DICT_REVERSE_INDEX_SHIFT;
    static final int TERMS_DICT_REVERSE_INDEX_MASK = TERMS_DICT_REVERSE_INDEX_SIZE - 1;

    static final int BINARY_DV_BLOCK_BYTES_THRESHOLD_DEFAULT = 512 * 1024;
    static final int BINARY_DV_BLOCK_COUNT_THRESHOLD_DEFAULT = 8096;

    static final int DEFAULT_SKIP_INDEX_INTERVAL_SIZE = 4096;

    static final int SKIP_INDEX_LEVEL_SHIFT = 3;
    static final int SKIP_INDEX_MAX_LEVEL = 4;

    static final int ORDINAL_RANGE_ENCODING_MIN_DOC_PER_ORDINAL = 512;
    static final int ORDINAL_RANGE_ENCODING_BLOCK_SHIFT = 12;

    static final OrdinalBlockCodec ORDINAL_CODEC = new TSDBOrdinalBlockCodec();

    static final TermsDictConfig TERMS_DICT_CONFIG = new TermsDictConfig(
        TERMS_DICT_BLOCK_LZ4_MASK,
        TERMS_DICT_BLOCK_LZ4_SHIFT,
        TERMS_DICT_REVERSE_INDEX_SHIFT,
        TERMS_DICT_REVERSE_INDEX_MASK
    );

    final boolean enableOptimizedMerge;
    final TSDBDocValuesFormatConfig formatConfig;
    final NumericCodecFactory numericCodecFactory;
    final FallbackDecoderFactory fallbackDecoderFactory;

    /**
     * Creates a new ES95 format with default configuration.
     * NOTE: required by SPI but not used at runtime; codec selection goes through
     * {@link ES95TSDBDocValuesFormatFactory}.
     */
    public ES95TSDBDocValuesFormat() {
        this(
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
            ES95NumericFieldReader::defaultFallbackDecoder
        );
    }

    ES95TSDBDocValuesFormat(
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
        super(CODEC_NAME);
        assert numericBlockShift == NUMERIC_BLOCK_SHIFT || numericBlockShift == NUMERIC_LARGE_BLOCK_SHIFT : numericBlockShift;
        if (skipIndexIntervalSize < 2) {
            throw new IllegalArgumentException("skipIndexIntervalSize must be > 1, got [" + skipIndexIntervalSize + "]");
        }
        this.enableOptimizedMerge = enableOptimizedMerge;
        this.numericCodecFactory = numericCodecFactory;
        this.fallbackDecoderFactory = fallbackDecoderFactory;
        this.formatConfig = new TSDBDocValuesFormatConfig(
            TSDBDocValuesFormatConfig.VERSION_CURRENT,
            TERMS_DICT_CONFIG,
            new TSDBDocValuesFormatConfig.SkipIndexConfig(SKIP_INDEX_LEVEL_SHIFT, SKIP_INDEX_MAX_LEVEL, skipIndexIntervalSize),
            new TSDBDocValuesFormatConfig.NumericConfig(
                numericBlockShift,
                ORDINAL_RANGE_ENCODING_BLOCK_SHIFT,
                minDocsPerOrdinalForRangeEncoding
            ),
            new TSDBDocValuesFormatConfig.BinaryConfig(
                blockBytesThreshold,
                blockCountThreshold,
                enablePerBlockCompression,
                binaryDVCompressionMode
            ),
            DIRECT_MONOTONIC_BLOCK_SHIFT,
            writePrefixPartitions
        );
    }

    @Override
    public DocValuesConsumer fieldsConsumer(final SegmentWriteState state) throws IOException {
        final NumericBlockCodec numericBlockCodec = new ES95NumericCodec(
            blockSize -> PipelineConfig.forLongs(blockSize).delta().offset().gcd().bitPack(),
            numericCodecFactory,
            fallbackDecoderFactory
        );
        return new ES95TSDBDocValuesConsumer(
            state,
            enableOptimizedMerge,
            DATA_CODEC,
            DATA_EXTENSION,
            META_CODEC,
            META_EXTENSION,
            SKIP_CODEC,
            SKIP_EXTENSION,
            formatConfig,
            DocOffsetsCodec.BITPACKING.getEncoder(),
            formatConfig.writePrefixPartitions()
                ? field -> field.number == AbstractTSDBDocValuesProducer.primarySortFieldNumber(state.segmentInfo, state.fieldInfos)
                    ? new PrefixedPartitionsWriter()
                    : SortedFieldObserver.NOOP
                : SortedFieldObserverFactory.NOOP,
            numericBlockCodec,
            ORDINAL_CODEC
        );
    }

    @Override
    public DocValuesProducer fieldsProducer(final SegmentReadState state) throws IOException {
        final NumericBlockCodec numericBlockCodec = new ES95NumericCodec(
            blockSize -> PipelineConfig.forLongs(blockSize).delta().offset().gcd().bitPack(),
            numericCodecFactory,
            fallbackDecoderFactory
        );
        return new ES95TSDBDocValuesProducer(
            state,
            DATA_CODEC,
            DATA_EXTENSION,
            META_CODEC,
            META_EXTENSION,
            SKIP_CODEC,
            SKIP_EXTENSION,
            formatConfig,
            DocOffsetsCodec.BITPACKING.getDecoder(),
            numericBlockCodec,
            ORDINAL_CODEC
        );
    }
}
