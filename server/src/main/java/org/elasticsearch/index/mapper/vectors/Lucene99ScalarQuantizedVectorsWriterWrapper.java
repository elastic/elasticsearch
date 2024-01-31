/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper.vectors;

import org.apache.lucene.codecs.FlatFieldVectorsWriter;
import org.apache.lucene.codecs.FlatVectorsWriter;
import org.apache.lucene.codecs.KnnFieldVectorsWriter;
import org.apache.lucene.codecs.lucene99.Lucene99ScalarQuantizedVectorsWriter;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.MergeState;
import org.apache.lucene.index.Sorter;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.hnsw.CloseableRandomVectorScorerSupplier;

import java.io.IOException;
import java.util.Collection;

public class Lucene99ScalarQuantizedVectorsWriterWrapper extends FlatVectorsWriter {

    final Lucene99ScalarQuantizedVectorsWriter delegate;

    Lucene99ScalarQuantizedVectorsWriterWrapper(Lucene99ScalarQuantizedVectorsWriter delegate) {
        this.delegate = delegate;
    }

    @Override
    public FlatFieldVectorsWriter<?> addField(FieldInfo fieldInfo, KnnFieldVectorsWriter<?> indexWriter) throws IOException {
        return delegate.addField(fieldInfo, indexWriter);
    }

    @Override
    public CloseableRandomVectorScorerSupplier mergeOneFieldToIndex(FieldInfo fieldInfo, MergeState mergeState) throws IOException {
        return delegate.mergeOneFieldToIndex(fieldInfo, mergeState);
    }

    @Override
    public void mergeOneField(FieldInfo fieldInfo, MergeState mergeState) throws IOException {
        delegate.mergeOneField(fieldInfo, mergeState);
    }

    @Override
    public void finish() throws IOException {
        delegate.finish();
    }

    @Override
    public void flush(int maxDoc, Sorter.DocMap sortMap) throws IOException {
        delegate.flush(maxDoc, sortMap);
    }

    @Override
    public void close() throws IOException {
        delegate.close();
    }

    @Override
    public long ramBytesUsed() {
        return delegate.ramBytesUsed(); // TODO: add a minimal overhead for the delegate field
    }

    @Override
    public Collection<Accountable> getChildResources() {
        return delegate.getChildResources();
    }

}
