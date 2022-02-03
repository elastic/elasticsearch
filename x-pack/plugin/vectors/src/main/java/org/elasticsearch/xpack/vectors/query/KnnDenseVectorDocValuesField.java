/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.vectors.query;

import org.apache.lucene.index.VectorValues;

import java.io.IOException;
import java.util.Iterator;

import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;

public class KnnDenseVectorDocValuesField extends DenseVectorDocValuesField {
    protected VectorValues input;
    protected float[] vector;
    protected final int dims;

    public KnnDenseVectorDocValuesField(VectorValues input, String name, int dims) {
        super(name);
        this.dims = dims;
        this.input = input;
    }

    @Override
    public void setNextDocId(int docId) throws IOException {
        int currentDoc = input.docID();
        if (currentDoc == NO_MORE_DOCS || docId < currentDoc) {
            vector = null;
        } else if (docId == currentDoc) {
            vector = input.vectorValue();
        } else {
            currentDoc = input.advance(docId);
            if (currentDoc == docId) {
                vector = input.vectorValue();
            } else {
                vector = null;
            }
        }
    }

    @Override
    public DenseVectorScriptDocValues getScriptDocValues() {
        return new DenseVectorScriptDocValues(this, dims);
    }

    public boolean isEmpty() {
        return vector == null;
    }

    @Override
    public DenseVector get() {
        if (isEmpty()) {
            return DenseVector.EMPTY;
        }

        return new KnnDenseVector(dims, vector);
    }

    @Override
    public DenseVector get(DenseVector defaultValue) {
        if (isEmpty()) {
            return defaultValue;
        }

        return new KnnDenseVector(dims, vector);
    }

    @Override
    public Iterator<Double> iterator() {
        return get().iterator();
    }

    @Override
    public DenseVector getInternal() {
        return get(null);
    }
}
