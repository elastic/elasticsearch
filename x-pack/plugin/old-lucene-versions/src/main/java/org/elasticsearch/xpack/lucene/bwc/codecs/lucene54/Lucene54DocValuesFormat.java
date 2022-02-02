/*
 * @notice
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Modifications copyright (C) 2021 Elasticsearch B.V.
 */
package org.elasticsearch.xpack.lucene.bwc.codecs.lucene54;

import org.apache.lucene.codecs.DocValuesConsumer;
import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;

import java.io.IOException;

/**
 * Lucene 5.4 DocValues format.
 * @deprecated Only for reading old 6.0+ segments
 */
@Deprecated
public final class Lucene54DocValuesFormat extends DocValuesFormat {

    /** Sole Constructor */
    public Lucene54DocValuesFormat() {
        super("Lucene54");
    }

    @Override
    public DocValuesConsumer fieldsConsumer(SegmentWriteState state) throws IOException {
        return new Lucene54DocValuesConsumer(state, DATA_CODEC, DATA_EXTENSION, META_CODEC, META_EXTENSION);
    }

    @Override
    public DocValuesProducer fieldsProducer(SegmentReadState state) throws IOException {
        return new Lucene54DocValuesProducer(state, DATA_CODEC, DATA_EXTENSION, META_CODEC, META_EXTENSION);
    }

    static final String DATA_CODEC = "Lucene54DocValuesData";
    static final String DATA_EXTENSION = "dvd";
    static final String META_CODEC = "Lucene54DocValuesMetadata";
    static final String META_EXTENSION = "dvm";
    static final int VERSION_START = 0;
    static final int VERSION_CURRENT = VERSION_START;

    // indicates docvalues type
    static final byte NUMERIC = 0;
    static final byte BINARY = 1;
    static final byte SORTED = 2;
    static final byte SORTED_SET = 3;
    static final byte SORTED_NUMERIC = 4;

    // address terms in blocks of 16 terms
    static final int INTERVAL_SHIFT = 4;
    static final int INTERVAL_COUNT = 1 << INTERVAL_SHIFT;
    static final int INTERVAL_MASK = INTERVAL_COUNT - 1;

    // build reverse index from every 1024th term
    static final int REVERSE_INTERVAL_SHIFT = 10;
    static final int REVERSE_INTERVAL_COUNT = 1 << REVERSE_INTERVAL_SHIFT;
    static final int REVERSE_INTERVAL_MASK = REVERSE_INTERVAL_COUNT - 1;

    // for conversion from reverse index to block
    static final int BLOCK_INTERVAL_SHIFT = REVERSE_INTERVAL_SHIFT - INTERVAL_SHIFT;
    static final int BLOCK_INTERVAL_COUNT = 1 << BLOCK_INTERVAL_SHIFT;
    static final int BLOCK_INTERVAL_MASK = BLOCK_INTERVAL_COUNT - 1;

    /** Compressed using packed blocks of ints. */
    static final int DELTA_COMPRESSED = 0;
    /** Compressed by computing the GCD. */
    static final int GCD_COMPRESSED = 1;
    /** Compressed by giving IDs to unique values. */
    static final int TABLE_COMPRESSED = 2;
    /** Compressed with monotonically increasing values */
    static final int MONOTONIC_COMPRESSED = 3;
    /** Compressed with constant value (uses only missing bitset) */
    static final int CONST_COMPRESSED = 4;
    /** Compressed with sparse arrays. */
    static final int SPARSE_COMPRESSED = 5;

    /** Uncompressed binary, written directly (fixed length). */
    static final int BINARY_FIXED_UNCOMPRESSED = 0;
    /** Uncompressed binary, written directly (variable length). */
    static final int BINARY_VARIABLE_UNCOMPRESSED = 1;
    /** Compressed binary with shared prefixes */
    static final int BINARY_PREFIX_COMPRESSED = 2;

    /** Standard storage for sorted set values with 1 level of indirection:
     *  {@code docId -> address -> ord}. */
    static final int SORTED_WITH_ADDRESSES = 0;
    /** Single-valued sorted set values, encoded as sorted values, so no level
     *  of indirection: {@code docId -> ord}. */
    static final int SORTED_SINGLE_VALUED = 1;
    /** Compressed giving IDs to unique sets of values:
     * {@code docId -> setId -> ords} */
    static final int SORTED_SET_TABLE = 2;

    /** placeholder for missing offset that means there are no missing values */
    static final int ALL_LIVE = -1;
    /** placeholder for missing offset that means all values are missing */
    static final int ALL_MISSING = -2;

    // addressing uses 16k blocks
    static final int MONOTONIC_BLOCK_SIZE = 16384;
    static final int DIRECT_MONOTONIC_BLOCK_SHIFT = 16;
}
