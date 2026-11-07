/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.parquet.docvalues;

import org.apache.lucene.codecs.DocValuesConsumer;
import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;

import java.io.IOException;

/**
 * Doc values format whose data lives in external Parquet files. A segment written with this format is a local stub that only
 * references the files, so that the format name stored in the segment resolves to this class when the segment is opened.
 */
public final class ParquetDocValuesFormat extends DocValuesFormat {

    /** Placeholder name, see the open question on naming in the plan. */
    public static final String NAME = "ESParquet900";

    public ParquetDocValuesFormat() {
        super(NAME);
    }

    @Override
    public DocValuesConsumer fieldsConsumer(SegmentWriteState state) throws IOException {
        return new ParquetDocValuesConsumer(state);
    }

    @Override
    public DocValuesProducer fieldsProducer(SegmentReadState state) throws IOException {
        return ParquetMetaFile.read(state);
    }
}
