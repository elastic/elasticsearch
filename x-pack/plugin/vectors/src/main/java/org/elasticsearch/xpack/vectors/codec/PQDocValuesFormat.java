/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.vectors.codec;

import org.apache.lucene.codecs.DocValuesConsumer;
import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;

import java.io.IOException;

public class PQDocValuesFormat extends DocValuesFormat {
    private final DocValuesFormat delegate;

    protected PQDocValuesFormat(String name, DocValuesFormat delegate) {
        super(name);
        this.delegate = delegate;
    }

    @Override
    public DocValuesConsumer fieldsConsumer(SegmentWriteState state) throws IOException {
        DocValuesConsumer consumer = delegate.fieldsConsumer(state);
        return new PQDocValuesWriter(state, consumer, DATA_CODEC, DATA_EXTENSION, META_CODEC, META_EXTENSION);
    }

    @Override
    public DocValuesProducer fieldsProducer(SegmentReadState state) throws IOException {
        DocValuesProducer producer = delegate.fieldsProducer(state);
        return new PQDocValuesReader(state, producer, DATA_CODEC, DATA_EXTENSION, META_CODEC, META_EXTENSION);
    }

    static final int VERSION_START = 0;
    static final int VERSION_CURRENT = VERSION_START;

    // stores 1) original vectors 2) product centroids with product centroids square magnitudes
    // 3) docCentroids -- what product centroids this document vector belongs to
    static final String DATA_CODEC = "LucenePQData";
    static final String DATA_EXTENSION = "pq";
    static final String META_CODEC = "LucenePQMetadata";
    static final String META_EXTENSION = "pqm";
}
