/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb.es819;

import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.store.IndexOutput;
import org.elasticsearch.index.codec.tsdb.AbstractTSDBDocValuesConsumer;
import org.elasticsearch.index.codec.tsdb.DocOffsetsCodec;
import org.elasticsearch.index.codec.tsdb.NumericBlockWriter;
import org.elasticsearch.index.codec.tsdb.TSDBDocValuesEncoder;
import org.elasticsearch.index.codec.tsdb.TSDBDocValuesFormatConfig;

import java.io.IOException;

/**
 * Doc values consumer for the ES819 TSDB format. Delegates all shared wire-format logic
 * to {@link AbstractTSDBDocValuesConsumer} and provides the ES819-specific numeric block
 * encoding strategy via {@link TSDBDocValuesEncoder}.
 */
final class ES819TSDBDocValuesConsumer extends AbstractTSDBDocValuesConsumer {

    private final DocOffsetsCodec.Encoder docOffsetsEncoder;

    ES819TSDBDocValuesConsumer(
        DocOffsetsCodec.Encoder docOffsetsEncoder,
        SegmentWriteState state,
        boolean enableOptimizedMerge,
        String dataCodec,
        String dataExtension,
        String metaCodec,
        String metaExtension,
        TSDBDocValuesFormatConfig formatConfig
    ) throws IOException {
        super(state, enableOptimizedMerge, dataCodec, dataExtension, metaCodec, metaExtension, formatConfig);
        this.docOffsetsEncoder = docOffsetsEncoder;
    }

    @Override
    protected NumericBlockWriter numericBlockWriter(FieldInfo field, long[] sample, int blockSize) {
        TSDBDocValuesEncoder encoder = new TSDBDocValuesEncoder(blockSize);
        return new NumericBlockWriter() {
            @Override
            public void write(long[] values, int bs, IndexOutput out) throws IOException {
                encoder.encode(values, out);
            }

            @Override
            public void writeOrdinals(long[] values, IndexOutput out, int bitsPerOrd) throws IOException {
                encoder.encodeOrdinals(values, out, bitsPerOrd);
            }
        };
    }

    @Override
    protected DocOffsetsCodec.Encoder docOffsetsEncoder() {
        return docOffsetsEncoder;
    }
}
