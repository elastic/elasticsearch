/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb;

import org.apache.lucene.codecs.DocValuesConsumer;
import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;

import java.io.IOException;

public class ES87TSDBDocValuesFormat extends org.apache.lucene.codecs.DocValuesFormat {

    static final int NUMERIC_BLOCK_SHIFT = 7;
    public static final int NUMERIC_BLOCK_SIZE = 1 << NUMERIC_BLOCK_SHIFT;
    static final int NUMERIC_BLOCK_MASK = NUMERIC_BLOCK_SIZE - 1;
    static final int DIRECT_MONOTONIC_BLOCK_SHIFT = 16;
    static final String CODEC_NAME = "ES87TSDB";
    static final String DATA_CODEC = "ES87TSDBDocValuesData";
    static final String DATA_EXTENSION = "dvd";
    static final String META_CODEC = "ES87TSDBDocValuesMetadata";
    static final String META_EXTENSION = "dvm";
    static final int VERSION_START = 0;
    static final int VERSION_CURRENT = VERSION_START;
    static final byte NUMERIC = 0;
    static final byte BINARY = 1;
    static final byte SORTED = 2;
    static final byte SORTED_SET = 3;
    static final byte SORTED_NUMERIC = 4;

    static final int TERMS_DICT_BLOCK_LZ4_SHIFT = 6;
    static final int TERMS_DICT_BLOCK_LZ4_SIZE = 1 << TERMS_DICT_BLOCK_LZ4_SHIFT;
    static final int TERMS_DICT_BLOCK_LZ4_MASK = TERMS_DICT_BLOCK_LZ4_SIZE - 1;

    static final int TERMS_DICT_REVERSE_INDEX_SHIFT = 10;
    static final int TERMS_DICT_REVERSE_INDEX_SIZE = 1 << TERMS_DICT_REVERSE_INDEX_SHIFT;
    static final int TERMS_DICT_REVERSE_INDEX_MASK = TERMS_DICT_REVERSE_INDEX_SIZE - 1;

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

    private final int skipIndexIntervalSize;

    /** Default constructor. */
    public ES87TSDBDocValuesFormat() {
        this(DEFAULT_SKIP_INDEX_INTERVAL_SIZE);
    }

    /** Doc values fields format with specified skipIndexIntervalSize. */
    public ES87TSDBDocValuesFormat(int skipIndexIntervalSize) {
        super(CODEC_NAME);
        if (skipIndexIntervalSize < 2) {
            throw new IllegalArgumentException("skipIndexIntervalSize must be > 1, got [" + skipIndexIntervalSize + "]");
        }
        this.skipIndexIntervalSize = skipIndexIntervalSize;
    }

    @Override
    public DocValuesConsumer fieldsConsumer(SegmentWriteState state) throws IOException {
        return new ES87TSDBDocValuesConsumer(state, skipIndexIntervalSize, DATA_CODEC, DATA_EXTENSION, META_CODEC, META_EXTENSION);
    }

    @Override
    public DocValuesProducer fieldsProducer(SegmentReadState state) throws IOException {
        return new ES87TSDBDocValuesProducer(state, DATA_CODEC, DATA_EXTENSION, META_CODEC, META_EXTENSION);
    }
}
