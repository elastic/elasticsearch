/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb.es819;

import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.store.DataInput;
import org.elasticsearch.index.codec.tsdb.AbstractTSDBDocValuesProducer;
import org.elasticsearch.index.codec.tsdb.DocOffsetsCodec;
import org.elasticsearch.index.codec.tsdb.NumericBlockReader;
import org.elasticsearch.index.codec.tsdb.TSDBDocValuesEncoder;
import org.elasticsearch.index.codec.tsdb.TSDBDocValuesFormatConfig;

import java.io.IOException;

/**
 * ES819 doc values producer. Delegates all shared wire-format reading logic to
 * {@link AbstractTSDBDocValuesProducer} and provides the ES819-specific numeric
 * block decoding strategy via {@link TSDBDocValuesEncoder}.
 */
final class ES819TSDBDocValuesProducer extends AbstractTSDBDocValuesProducer {

    private final DocOffsetsCodec.Decoder docOffsetsDecoder;

    ES819TSDBDocValuesProducer(
        SegmentReadState state,
        String dataCodec,
        String dataExtension,
        String metaCodec,
        String metaExtension,
        DocOffsetsCodec.Decoder docOffsetsDecoder,
        TSDBDocValuesFormatConfig formatConfig
    ) throws IOException {
        super(state, dataCodec, dataExtension, metaCodec, metaExtension, formatConfig);
        this.docOffsetsDecoder = docOffsetsDecoder;
    }

    private ES819TSDBDocValuesProducer(ES819TSDBDocValuesProducer original) {
        super(original);
        this.docOffsetsDecoder = original.docOffsetsDecoder;
    }

    @Override
    protected NumericBlockReader createNumericBlockReader(NumericEntry entry) {
        TSDBDocValuesEncoder encoder = new TSDBDocValuesEncoder(numericBlockSize);
        return new NumericBlockReader() {
            @Override
            public void read(DataInput input, long[] values, int count) throws IOException {
                encoder.decode(input, values);
            }

            @Override
            public void readOrdinals(DataInput input, long[] values, int bitsPerOrd) throws IOException {
                encoder.decodeOrdinals(input, values, bitsPerOrd);
            }
        };
    }

    @Override
    protected AbstractTSDBDocValuesProducer createMergeInstance() {
        return new ES819TSDBDocValuesProducer(this);
    }

    @Override
    protected DocOffsetsCodec.Decoder docOffsetsDecoder() {
        return docOffsetsDecoder;
    }
}
