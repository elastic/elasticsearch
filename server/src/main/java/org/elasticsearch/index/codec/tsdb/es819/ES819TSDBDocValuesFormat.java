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
 * </ul>
 */
public class ES819TSDBDocValuesFormat extends org.apache.lucene.codecs.DocValuesFormat {

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
    static final int VERSION_CURRENT = VERSION_START;

    static final int TERMS_DICT_BLOCK_LZ4_SHIFT = 6;
    static final int TERMS_DICT_BLOCK_LZ4_SIZE = 1 << TERMS_DICT_BLOCK_LZ4_SHIFT;
    static final int TERMS_DICT_BLOCK_LZ4_MASK = TERMS_DICT_BLOCK_LZ4_SIZE - 1;

    static final int TERMS_DICT_REVERSE_INDEX_SHIFT = 10;
    static final int TERMS_DICT_REVERSE_INDEX_SIZE = 1 << TERMS_DICT_REVERSE_INDEX_SHIFT;
    static final int TERMS_DICT_REVERSE_INDEX_MASK = TERMS_DICT_REVERSE_INDEX_SIZE - 1;

    // Default for escape hatch:
    static final boolean OPTIMIZED_MERGE_ENABLE_DEFAULT;
    static final String OPTIMIZED_MERGE_ENABLED_NAME = ES819TSDBDocValuesConsumer.class.getName() + ".enableOptimizedMerge";

    static {
        OPTIMIZED_MERGE_ENABLE_DEFAULT = Boolean.parseBoolean(System.getProperty(OPTIMIZED_MERGE_ENABLED_NAME, Boolean.TRUE.toString()));
    }

    private final boolean enableOptimizedMerge;

    /** Default constructor. */
    public ES819TSDBDocValuesFormat() {
        this(OPTIMIZED_MERGE_ENABLE_DEFAULT);
    }

    public ES819TSDBDocValuesFormat(boolean enableOptimizedMerge) {
        super(CODEC_NAME);
        this.enableOptimizedMerge = enableOptimizedMerge;
    }

    @Override
    public DocValuesConsumer fieldsConsumer(SegmentWriteState state) throws IOException {
        return new ES819TSDBDocValuesConsumer(state, enableOptimizedMerge, DATA_CODEC, DATA_EXTENSION, META_CODEC, META_EXTENSION);
    }

    @Override
    public DocValuesProducer fieldsProducer(SegmentReadState state) throws IOException {
        return new ES819TSDBDocValuesProducer(state, DATA_CODEC, DATA_EXTENSION, META_CODEC, META_EXTENSION);
    }
}
