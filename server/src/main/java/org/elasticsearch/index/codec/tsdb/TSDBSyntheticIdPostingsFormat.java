/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb;

import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.codecs.FieldsConsumer;
import org.apache.lucene.codecs.FieldsProducer;
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.index.mapper.DataStreamTimestampFieldMapper;
import org.elasticsearch.index.mapper.SyntheticIdField;
import org.elasticsearch.index.mapper.TimeSeriesIdFieldMapper;
import org.elasticsearch.index.mapper.TimeSeriesRoutingHashFieldMapper;

import java.io.IOException;

public class TSDBSyntheticIdPostingsFormat extends PostingsFormat {

    public static final String SYNTHETIC_ID = SyntheticIdField.NAME;
    public static final String TIMESTAMP = DataStreamTimestampFieldMapper.DEFAULT_PATH;
    public static final String TS_ID = TimeSeriesIdFieldMapper.NAME;
    public static final String TS_ROUTING_HASH = TimeSeriesRoutingHashFieldMapper.NAME;

    static final String FORMAT_NAME = "TSDBSyntheticId";
    static final String SUFFIX = "0";

    public TSDBSyntheticIdPostingsFormat() {
        super(FORMAT_NAME);
    }

    @Override
    public FieldsProducer fieldsProducer(SegmentReadState state) throws IOException {
        DocValuesProducer docValuesProducer = null;
        boolean success = false;
        try {
            var codec = state.segmentInfo.getCodec();
            // Erase the segment suffix (used only for reading postings)
            docValuesProducer = codec.docValuesFormat().fieldsProducer(new SegmentReadState(state, ""));
            var fieldsProducer = new TSDBSyntheticIdFieldsProducer(state, docValuesProducer);
            success = true;
            return fieldsProducer;
        } finally {
            if (success == false) {
                IOUtils.close(docValuesProducer);
            }
        }
    }

    @Override
    public FieldsConsumer fieldsConsumer(SegmentWriteState state) throws IOException {
        assert false : "this should never be called";
        throw new UnsupportedOperationException();
    }
}
