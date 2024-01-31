/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper.vectors;

import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.codecs.KnnVectorsReader;
import org.apache.lucene.codecs.KnnVectorsWriter;
import org.apache.lucene.codecs.lucene99.Lucene99HnswScalarQuantizedVectorsFormat;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;

import java.io.IOException;

public class CustomLucene99HnswScalarQuantizedVectorsFormat extends KnnVectorsFormat {

    final Lucene99HnswScalarQuantizedVectorsFormat delegate;

    CustomLucene99HnswScalarQuantizedVectorsFormat(Lucene99HnswScalarQuantizedVectorsFormat delegate) {
        super(delegate.getName());
        this.delegate = delegate;
    }

    @Override
    public KnnVectorsWriter fieldsWriter(SegmentWriteState state) throws IOException {
        return delegate.fieldsWriter(state);
    }

    @Override
    public KnnVectorsReader fieldsReader(SegmentReadState state) throws IOException {
        return delegate.fieldsReader(state);
    }

    @Override
    public int getMaxDimensions(String fieldName) {
        return delegate.getMaxDimensions(fieldName);
    }

    @Override
    public String toString() {
        return "CustomLucene99HnswScalarQuantizedVectorsFormat[delegate=" + delegate + "]";
    }
}
