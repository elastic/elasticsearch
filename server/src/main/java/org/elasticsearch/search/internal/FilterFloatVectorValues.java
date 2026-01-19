/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.internal;

import org.apache.lucene.codecs.lucene95.HasIndexSlice;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.KnnVectorValues;
import org.apache.lucene.search.VectorScorer;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.Bits;

import java.io.IOException;
import java.util.Objects;

/**
 * A wrapper on {@link FloatVectorValues}.
 */
public abstract class FilterFloatVectorValues extends FloatVectorValues implements HasIndexSlice {

    /** Wrapped values */
    protected final FloatVectorValues in;
    private final IndexInput slice;

    /** Sole constructor */
    protected FilterFloatVectorValues(FloatVectorValues in) {
        this.in = Objects.requireNonNull(in);
        this.slice = in instanceof HasIndexSlice s ? s.getSlice() : null;
    }

    @Override
    public KnnVectorValues.DocIndexIterator iterator() {
        return in.iterator();
    }

    @Override
    public float[] vectorValue(int ord) throws IOException {
        return in.vectorValue(ord);
    }

    @Override
    public abstract FloatVectorValues copy() throws IOException;

    @Override
    public int dimension() {
        return in.dimension();
    }

    @Override
    public int size() {
        return in.size();
    }

    @Override
    public VectorScorer scorer(float[] target) throws IOException {
        return in.scorer(target);
    }

    @Override
    public int ordToDoc(int ord) {
        return in.ordToDoc(ord);
    }

    @Override
    public Bits getAcceptOrds(Bits acceptDocs) {
        return in.getAcceptOrds(acceptDocs);
    }

    @Override
    public IndexInput getSlice() {
        return slice;
    }
}
