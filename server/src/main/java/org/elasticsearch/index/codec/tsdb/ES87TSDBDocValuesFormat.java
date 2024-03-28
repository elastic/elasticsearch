/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
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

    static final int BINARY_BLOCK_SHIFT = 5;
    static final int BINARY_DOCS_PER_COMPRESSED_BLOCK = 1 << BINARY_BLOCK_SHIFT;
    static final long MAX_COMPRESSED_BLOCK_SIZE = 1 << 1;

    public ES87TSDBDocValuesFormat() {
        super(CODEC_NAME);
    }

    @Override
    public DocValuesConsumer fieldsConsumer(SegmentWriteState state) throws IOException {
        return new ES87TSDBDocValuesConsumer(state, DATA_CODEC, DATA_EXTENSION, META_CODEC, META_EXTENSION);
    }

    @Override
    public DocValuesProducer fieldsProducer(SegmentReadState state) throws IOException {
        return new ES87TSDBDocValuesProducer(state, DATA_CODEC, DATA_EXTENSION, META_CODEC, META_EXTENSION);
    }
}
