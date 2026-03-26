/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb.es94;

import org.apache.lucene.codecs.DocValuesConsumer;
import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.elasticsearch.index.codec.tsdb.AbstractTSDBDocValuesProducer;
import org.elasticsearch.index.codec.tsdb.BinaryDVCompressionMode;
import org.elasticsearch.index.codec.tsdb.DocOffsetsCodec;
import org.elasticsearch.index.codec.tsdb.PrefixedPartitionsWriter;
import org.elasticsearch.index.codec.tsdb.SortedFieldObserver;
import org.elasticsearch.index.codec.tsdb.SortedFieldObserverFactory;
import org.elasticsearch.index.codec.tsdb.TSDBDocValuesFormatConfig;
import org.elasticsearch.index.codec.tsdb.TSDBDocValuesFormatConfig.BinaryConfig;
import org.elasticsearch.index.codec.tsdb.TSDBDocValuesFormatConfig.NumericConfig;
import org.elasticsearch.index.codec.tsdb.TSDBDocValuesFormatConfig.SkipIndexConfig;
import org.elasticsearch.index.codec.tsdb.TSDBDocValuesFormatConfig.TermsDictConfig;
import org.elasticsearch.index.codec.tsdb.TSDBDocValuesFormatConfig.VersionConfig;
import org.elasticsearch.index.codec.tsdb.pipeline.PipelineConfig;
import org.elasticsearch.index.codec.tsdb.pipeline.numeric.NumericCodecFactory;
import org.elasticsearch.index.codec.tsdb.pipeline.numeric.NumericEncoder;

import java.io.IOException;

/**
 * ES94 TSDB doc values format. Functionally equivalent to ES819 for all doc
 * value types, with the sole difference that numeric fields are encoded via a
 * composable pipeline of stages ({@code delta>offset>gcd>bitpack}) instead of
 * the monolithic {@code TSDBDocValuesEncoder}. Each numeric field writes a
 * self-describing {@link org.elasticsearch.index.codec.tsdb.pipeline.FieldDescriptor}
 * so decoders need no implicit format knowledge.
 *
 * <p>Non-numeric field types (binary, sorted, sorted-set) are handled identically
 * to ES819 by the shared abstract base classes.
 */
public class ES94TSDBDocValuesFormat extends DocValuesFormat {

    static final String CODEC_NAME = "ES94TSDB";
    static final String DATA_CODEC = "ES94TSDBDocValuesData";
    static final String DATA_EXTENSION = "dvd";
    static final String META_CODEC = "ES94TSDBDocValuesMetadata";
    static final String META_EXTENSION = "dvm";

    static final int VERSION_START = 0;
    static final int VERSION_CURRENT = 0;

    static final int NUMERIC_BLOCK_SHIFT = 7;
    static final int NUMERIC_LARGE_BLOCK_SHIFT = 9;
    static final int DIRECT_MONOTONIC_BLOCK_SHIFT = 16;
    static final int ORDINAL_RANGE_ENCODING_BLOCK_SHIFT = 12;
    static final int ORDINAL_RANGE_ENCODING_MIN_DOC_PER_ORDINAL = 512;
    static final int DEFAULT_SKIP_INDEX_INTERVAL_SIZE = 4096;
    static final int SKIP_INDEX_LEVEL_SHIFT = 3;
    static final int SKIP_INDEX_MAX_LEVEL = 4;

    static final int TERMS_DICT_BLOCK_LZ4_SHIFT = 6;
    static final int TERMS_DICT_BLOCK_LZ4_SIZE = 1 << TERMS_DICT_BLOCK_LZ4_SHIFT;
    static final int TERMS_DICT_BLOCK_LZ4_MASK = TERMS_DICT_BLOCK_LZ4_SIZE - 1;
    static final int TERMS_DICT_REVERSE_INDEX_SHIFT = 10;
    static final int TERMS_DICT_REVERSE_INDEX_SIZE = 1 << TERMS_DICT_REVERSE_INDEX_SHIFT;
    static final int TERMS_DICT_REVERSE_INDEX_MASK = TERMS_DICT_REVERSE_INDEX_SIZE - 1;

    static final int BINARY_DV_BLOCK_BYTES_THRESHOLD_DEFAULT = 128 * 1024;
    static final int BINARY_DV_BLOCK_COUNT_THRESHOLD_DEFAULT = 1024;

    static final VersionConfig VERSION_CONFIG = new VersionConfig(
        VERSION_START,
        VERSION_CURRENT,
        VERSION_START,
        VERSION_START,
        VERSION_START
    );

    static final TermsDictConfig TERMS_DICT_CONFIG = new TermsDictConfig(
        TERMS_DICT_BLOCK_LZ4_MASK,
        TERMS_DICT_BLOCK_LZ4_SHIFT,
        TERMS_DICT_REVERSE_INDEX_SHIFT,
        TERMS_DICT_REVERSE_INDEX_MASK
    );

    static final NumericCodecFactory NUMERIC_CODEC_FACTORY = NumericCodecFactory.DEFAULT;

    final boolean enableOptimizedMerge;
    final DocOffsetsCodec docOffsetsCodec;
    final TSDBDocValuesFormatConfig formatConfig;
    final NumericEncoder numericEncoder;

    public ES94TSDBDocValuesFormat() {
        this(NUMERIC_BLOCK_SHIFT);
    }

    public ES94TSDBDocValuesFormat(int numericBlockShift) {
        this(
            DEFAULT_SKIP_INDEX_INTERVAL_SIZE,
            ORDINAL_RANGE_ENCODING_MIN_DOC_PER_ORDINAL,
            true,
            BinaryDVCompressionMode.COMPRESSED_ZSTD_LEVEL_1,
            true,
            numericBlockShift
        );
    }

    public ES94TSDBDocValuesFormat(
        int skipIndexIntervalSize,
        int minDocsPerOrdinalForRangeEncoding,
        boolean enableOptimizedMerge,
        BinaryDVCompressionMode binaryDVCompressionMode,
        boolean enablePerBlockCompression,
        int numericBlockShift
    ) {
        this(
            skipIndexIntervalSize,
            minDocsPerOrdinalForRangeEncoding,
            enableOptimizedMerge,
            binaryDVCompressionMode,
            enablePerBlockCompression,
            numericBlockShift,
            DocOffsetsCodec.GROUPED_VINT,
            BINARY_DV_BLOCK_BYTES_THRESHOLD_DEFAULT,
            BINARY_DV_BLOCK_COUNT_THRESHOLD_DEFAULT,
            false
        );
    }

    public ES94TSDBDocValuesFormat(
        int skipIndexIntervalSize,
        int minDocsPerOrdinalForRangeEncoding,
        boolean enableOptimizedMerge,
        BinaryDVCompressionMode binaryDVCompressionMode,
        boolean enablePerBlockCompression,
        int numericBlockShift,
        DocOffsetsCodec docOffsetsCodec,
        int blockBytesThreshold,
        int blockCountThreshold,
        boolean writePrefixPartitions
    ) {
        super(CODEC_NAME);
        assert numericBlockShift == NUMERIC_BLOCK_SHIFT || numericBlockShift == NUMERIC_LARGE_BLOCK_SHIFT : numericBlockShift;
        if (skipIndexIntervalSize < 2) {
            throw new IllegalArgumentException("skipIndexIntervalSize must be > 1, got [" + skipIndexIntervalSize + "]");
        }
        this.enableOptimizedMerge = enableOptimizedMerge;
        this.docOffsetsCodec = docOffsetsCodec;
        final PipelineConfig pipelineConfig = PipelineConfig.forLongs(1 << numericBlockShift).delta().offset().gcd().bitPack();
        this.numericEncoder = NUMERIC_CODEC_FACTORY.createEncoder(pipelineConfig);
        this.formatConfig = new TSDBDocValuesFormatConfig(
            VERSION_CONFIG,
            TERMS_DICT_CONFIG,
            new SkipIndexConfig(SKIP_INDEX_LEVEL_SHIFT, SKIP_INDEX_MAX_LEVEL, skipIndexIntervalSize),
            new NumericConfig(numericBlockShift, ORDINAL_RANGE_ENCODING_BLOCK_SHIFT, minDocsPerOrdinalForRangeEncoding),
            new BinaryConfig(blockBytesThreshold, blockCountThreshold, enablePerBlockCompression, binaryDVCompressionMode),
            DIRECT_MONOTONIC_BLOCK_SHIFT,
            writePrefixPartitions
        );
    }

    @Override
    public DocValuesConsumer fieldsConsumer(SegmentWriteState state) throws IOException {
        return new ES94TSDBDocValuesConsumer(
            state,
            enableOptimizedMerge,
            DATA_CODEC,
            DATA_EXTENSION,
            META_CODEC,
            META_EXTENSION,
            formatConfig,
            docOffsetsCodec.getEncoder(),
            formatConfig.writePrefixPartitions()
                ? field -> field.number == AbstractTSDBDocValuesProducer.primarySortFieldNumber(state.segmentInfo, state.fieldInfos)
                    ? new PrefixedPartitionsWriter()
                    : SortedFieldObserver.NOOP
                : SortedFieldObserverFactory.NOOP,
            numericEncoder
        );
    }

    @Override
    public DocValuesProducer fieldsProducer(SegmentReadState state) throws IOException {
        return new ES94TSDBDocValuesProducer(
            state,
            DATA_CODEC,
            DATA_EXTENSION,
            META_CODEC,
            META_EXTENSION,
            formatConfig,
            docOffsetsCodec.getDecoder(),
            NUMERIC_CODEC_FACTORY
        );
    }
}
