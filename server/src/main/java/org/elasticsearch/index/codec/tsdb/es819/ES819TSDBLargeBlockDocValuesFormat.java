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
 * Evolved from {@link ES819TSDBDocValuesFormat} but uses larger block size for numeric values (512 vs 128).
 */
public class ES819TSDBLargeBlockDocValuesFormat extends ES819TSDBDocValuesFormat {

    static final int NUMERIC_BLOCK_SHIFT = 9;
    static final String CODEC_NAME = "ES819TSDBLB";// block size: 512

    /** Default constructor. */
    public ES819TSDBLargeBlockDocValuesFormat() {
        super(CODEC_NAME);
    }

    /** Doc values fields format with specified skipIndexIntervalSize. */
    public ES819TSDBLargeBlockDocValuesFormat(
        int skipIndexIntervalSize,
        int minDocsPerOrdinalForRangeEncoding,
        boolean enableOptimizedMerge
    ) {
        super(CODEC_NAME, skipIndexIntervalSize, minDocsPerOrdinalForRangeEncoding, enableOptimizedMerge);
    }

    @Override
    public DocValuesConsumer fieldsConsumer(SegmentWriteState state) throws IOException {
        return new ES819TSDBDocValuesConsumer(
            state,
            skipIndexIntervalSize,
            minDocsPerOrdinalForRangeEncoding,
            enableOptimizedMerge,
            DATA_CODEC,
            DATA_EXTENSION,
            META_CODEC,
            META_EXTENSION,
            NUMERIC_BLOCK_SHIFT
        );
    }

    @Override
    public DocValuesProducer fieldsProducer(SegmentReadState state) throws IOException {
        return new ES819TSDBDocValuesProducer(state, DATA_CODEC, DATA_EXTENSION, META_CODEC, META_EXTENSION, NUMERIC_BLOCK_SHIFT);
    }
}
