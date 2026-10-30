/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors;

import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.codecs.KnnVectorsReader;
import org.apache.lucene.codecs.KnnVectorsWriter;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.elasticsearch.index.store.VectorFieldHint;

import java.io.IOException;

/**
 * The vectors format of a single field, which writes its files saying they hold that field. A segment records only the
 * delegate's name, so the segment is read as if the delegate had written it.
 */
public final class FieldKnnVectorsFormat extends KnnVectorsFormat {

    private final VectorFieldHint field;
    private final KnnVectorsFormat delegate;

    public FieldKnnVectorsFormat(String field, KnnVectorsFormat delegate) {
        super(delegate.getName());
        this.field = new VectorFieldHint(field);
        this.delegate = delegate;
    }

    @Override
    public KnnVectorsWriter fieldsWriter(SegmentWriteState state) throws IOException {
        return delegate.fieldsWriter(new SegmentWriteState(state, state.context.union(field)));
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
        return delegate.toString();
    }
}
