/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb.es819;

import org.apache.lucene.codecs.DocValuesConsumer;
import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.elasticsearch.common.util.FeatureFlag;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.index.codec.tsdb.BinaryDVCompressionMode;

import java.io.IOException;

/**
 * Evolved from {@link org.elasticsearch.index.codec.tsdb.ES87TSDBDocValuesFormat} and has the following changes:
 * <ul>
 *     <li>Moved numDocsWithField metadata statistic from SortedNumericEntry to NumericEntry. This allows for always summing
 *     numDocsWithField during segment merging, otherwise numDocsWithField needs to be computed for each segment merge per field.</li>
 *     <li>Moved docsWithFieldOffset, docsWithFieldLength, jumpTableEntryCount, denseRankPower metadata properties in the format to be
 *     after values metadata. So that the jump table can be stored after the values, which allows for iterating once over the merged
 *     view of all values. If index sorting is active merging a doc value field requires a merge sort which can be very cpu intensive.
 *     The previous format always has to merge sort a doc values field multiple times, so doing the merge sort just once saves on
 *     cpu resources.</li>
 *     <li>Version 1 adds block-wise compression to binary doc values. Each block contains a variable number of values so that each
 *     block is approximately the same size. To map a given value's index to the block containing the value, there are two parallel
 *     arrays. These contain the starting address for each block, and the starting value index for each block. Additional compression
 *     types may be added by creating a new mode in {@link org.elasticsearch.index.codec.tsdb.BinaryDVCompressionMode}.</li>
 * </ul>
 */
public class ES819TSDBDocValuesFormat extends org.apache.lucene.codecs.DocValuesFormat {

    public static final boolean BINARY_DV_COMPRESSION_FEATURE_FLAG = new FeatureFlag("binary_dv_compression").isEnabled();

    static final int NUMERIC_BLOCK_SHIFT = 7;
    public static final int NUMERIC_BLOCK_SIZE = 1 << NUMERIC_BLOCK_SHIFT;
    static final int NUMERIC_BLOCK_MASK = NUMERIC_BLOCK_SIZE - 1;
    static final int DIRECT_MONOTONIC_BLOCK_SHIFT = 16;
    static final String CODEC_NAME = "ES819TSDB";
    static final String DATA_CODEC = "ES819TSDBDocValuesData";
    static final String DATA_EXTENSION = "dvd";
    static final String META_CODEC = "ES819TSDBDocValuesMetadata";
    static final String META_EXTENSION = "dvm";
    static final byte NUMERIC = 0;
    static final byte BINARY = 1;
    static final byte SORTED = 2;
    static final byte SORTED_SET = 3;
    static final byte SORTED_NUMERIC = 4;

    static final int VERSION_START = 0;
    static final int VERSION_BINARY_DV_COMPRESSION = 1;
    static final int VERSION_CURRENT = VERSION_BINARY_DV_COMPRESSION;

    static final int TERMS_DICT_BLOCK_LZ4_SHIFT = 6;
    static final int TERMS_DICT_BLOCK_LZ4_SIZE = 1 << TERMS_DICT_BLOCK_LZ4_SHIFT;
    static final int TERMS_DICT_BLOCK_LZ4_MASK = TERMS_DICT_BLOCK_LZ4_SIZE - 1;

    static final int TERMS_DICT_REVERSE_INDEX_SHIFT = 10;
    static final int TERMS_DICT_REVERSE_INDEX_SIZE = 1 << TERMS_DICT_REVERSE_INDEX_SHIFT;
    static final int TERMS_DICT_REVERSE_INDEX_MASK = TERMS_DICT_REVERSE_INDEX_SIZE - 1;

    /**
     * These thresholds determine the size of a compressed binary block. We build a new block if the uncompressed data in the block
     * is 128k, or if the number of values is 1024. These values are a tradeoff between the high compression ratio and decompression
     * speed of large blocks, and the ability to avoid decompressing unneeded values provided by small blocks.
      */
    public static final int BLOCK_BYTES_THRESHOLD = 128 * 1024;
    public static final int BLOCK_COUNT_THRESHOLD = 1024;

    // number of documents in an interval
    private static final int DEFAULT_SKIP_INDEX_INTERVAL_SIZE = 4096;
    // bytes on an interval:
    // * 1 byte : number of levels
    // * 16 bytes: min / max value,
    // * 8 bytes: min / max docID
    // * 4 bytes: number of documents
    private static final long SKIP_INDEX_INTERVAL_BYTES = 29L;
    // number of intervals represented as a shift to create a new level, this is 1 << 3 == 8
    // intervals.
    static final int SKIP_INDEX_LEVEL_SHIFT = 3;
    // max number of levels
    // Increasing this number, it increases how much heap we need at index time.
    // we currently need (1 * 8 * 8 * 8) = 512 accumulators on heap
    static final int SKIP_INDEX_MAX_LEVEL = 4;
    // number of bytes to skip when skipping a level. It does not take into account the
    // current interval that is being read.
    static final long[] SKIP_INDEX_JUMP_LENGTH_PER_LEVEL = new long[SKIP_INDEX_MAX_LEVEL];

    static {
        // Size of the interval minus read bytes (1 byte for level and 4 bytes for maxDocID)
        SKIP_INDEX_JUMP_LENGTH_PER_LEVEL[0] = SKIP_INDEX_INTERVAL_BYTES - 5L;
        for (int level = 1; level < SKIP_INDEX_MAX_LEVEL; level++) {
            // jump from previous level
            SKIP_INDEX_JUMP_LENGTH_PER_LEVEL[level] = SKIP_INDEX_JUMP_LENGTH_PER_LEVEL[level - 1];
            // nodes added by new level
            SKIP_INDEX_JUMP_LENGTH_PER_LEVEL[level] += (1 << (level * SKIP_INDEX_LEVEL_SHIFT)) * SKIP_INDEX_INTERVAL_BYTES;
            // remove the byte levels added in the previous level
            SKIP_INDEX_JUMP_LENGTH_PER_LEVEL[level] -= (1 << ((level - 1) * SKIP_INDEX_LEVEL_SHIFT));
        }
    }

    // Default for escape hatch:
    static final boolean OPTIMIZED_MERGE_ENABLE_DEFAULT;
    static final String OPTIMIZED_MERGE_ENABLED_NAME = ES819TSDBDocValuesConsumer.class.getName() + ".enableOptimizedMerge";

    static {
        OPTIMIZED_MERGE_ENABLE_DEFAULT = getOptimizedMergeEnabledDefault();
    }

    @SuppressForbidden(
        reason = "TODO Deprecate any lenient usage of Boolean#parseBoolean https://github.com/elastic/elasticsearch/issues/128993"
    )
    private static boolean getOptimizedMergeEnabledDefault() {
        return Boolean.parseBoolean(System.getProperty(OPTIMIZED_MERGE_ENABLED_NAME, Boolean.TRUE.toString()));
    }

    /**
     * The default minimum number of documents per ordinal required to use ordinal range encoding.
     * If the average number of documents per ordinal is below this threshold, it is more efficient to encode doc values in blocks.
     * A much smaller value may be used in tests to exercise ordinal range encoding more frequently.
     */
    public static final int ORDINAL_RANGE_ENCODING_MIN_DOC_PER_ORDINAL = 512;

    /**
     * The block shift used in DirectMonotonicWriter when encoding the start docs of each ordinal with ordinal range encoding.
     */
    public static final int ORDINAL_RANGE_ENCODING_BLOCK_SHIFT = 12;

    final int skipIndexIntervalSize;
    final int minDocsPerOrdinalForRangeEncoding;
    final boolean enableOptimizedMerge;
    final BinaryDVCompressionMode binaryDVCompressionMode;
    final boolean enablePerBlockCompression;

    /** Default constructor. */
    public ES819TSDBDocValuesFormat() {
        this(
            DEFAULT_SKIP_INDEX_INTERVAL_SIZE,
            ORDINAL_RANGE_ENCODING_MIN_DOC_PER_ORDINAL,
            OPTIMIZED_MERGE_ENABLE_DEFAULT,
            BINARY_DV_COMPRESSION_FEATURE_FLAG ? BinaryDVCompressionMode.COMPRESSED_ZSTD_LEVEL_1 : BinaryDVCompressionMode.NO_COMPRESS,
            true
        );
    }

    public ES819TSDBDocValuesFormat(BinaryDVCompressionMode binaryDVCompressionMode) {
        this(
            DEFAULT_SKIP_INDEX_INTERVAL_SIZE,
            ORDINAL_RANGE_ENCODING_MIN_DOC_PER_ORDINAL,
            OPTIMIZED_MERGE_ENABLE_DEFAULT,
            binaryDVCompressionMode,
            true
        );
    }

    public ES819TSDBDocValuesFormat(BinaryDVCompressionMode binaryDVCompressionMode, boolean enablePerBlockCompression) {
        this(
            DEFAULT_SKIP_INDEX_INTERVAL_SIZE,
            ORDINAL_RANGE_ENCODING_MIN_DOC_PER_ORDINAL,
            OPTIMIZED_MERGE_ENABLE_DEFAULT,
            binaryDVCompressionMode,
            enablePerBlockCompression
        );
    }

    /** Doc values fields format with specified skipIndexIntervalSize. */
    public ES819TSDBDocValuesFormat(
        int skipIndexIntervalSize,
        int minDocsPerOrdinalForRangeEncoding,
        boolean enableOptimizedMerge,
        BinaryDVCompressionMode binaryDVCompressionMode,
        final boolean enablePerBlockCompression
    ) {
        super(CODEC_NAME);
        if (skipIndexIntervalSize < 2) {
            throw new IllegalArgumentException("skipIndexIntervalSize must be > 1, got [" + skipIndexIntervalSize + "]");
        }
        this.skipIndexIntervalSize = skipIndexIntervalSize;
        this.minDocsPerOrdinalForRangeEncoding = minDocsPerOrdinalForRangeEncoding;
        this.enableOptimizedMerge = enableOptimizedMerge;
        this.binaryDVCompressionMode = binaryDVCompressionMode;
        this.enablePerBlockCompression = enablePerBlockCompression;
    }

    @Override
    public DocValuesConsumer fieldsConsumer(SegmentWriteState state) throws IOException {
        return new ES819TSDBDocValuesConsumer(
            binaryDVCompressionMode,
            enablePerBlockCompression,
            state,
            skipIndexIntervalSize,
            minDocsPerOrdinalForRangeEncoding,
            enableOptimizedMerge,
            DATA_CODEC,
            DATA_EXTENSION,
            META_CODEC,
            META_EXTENSION
        );
    }

    @Override
    public DocValuesProducer fieldsProducer(SegmentReadState state) throws IOException {
        return new ES819TSDBDocValuesProducer(state, DATA_CODEC, DATA_EXTENSION, META_CODEC, META_EXTENSION);
    }
}
