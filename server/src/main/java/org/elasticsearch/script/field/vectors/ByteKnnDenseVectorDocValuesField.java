/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.script.field.vectors;

import org.apache.lucene.index.ByteVectorValues;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper.ElementType;
import org.elasticsearch.index.mapper.vectors.DenseVectorScriptDocValues;

import java.io.IOException;

import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;

public class ByteKnnDenseVectorDocValuesField extends DenseVectorDocValuesField {
    protected ByteVectorValues input; // null if no vectors
    protected byte[] vector;
    protected final int dims;

    public ByteKnnDenseVectorDocValuesField(@Nullable ByteVectorValues input, String name, int dims) {
        this(input, name, dims, ElementType.BYTE);
    }

    protected ByteKnnDenseVectorDocValuesField(@Nullable ByteVectorValues input, String name, int dims, ElementType elementType) {
        super(name, elementType);
        this.dims = dims;
        this.input = input;
    }

    @Override
    public void setNextDocId(int docId) throws IOException {
        if (input == null) {
            return;
        }
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
    public DenseVectorScriptDocValues toScriptDocValues() {
        return new DenseVectorScriptDocValues(this, dims);
    }

    public boolean isEmpty() {
        return vector == null;
    }

    protected DenseVector getVector() {
        return new ByteKnnDenseVector(vector);
    }

    @Override
    public DenseVector get() {
        if (isEmpty()) {
            return DenseVector.EMPTY;
        }

        return getVector();
    }

    @Override
    public DenseVector get(DenseVector defaultValue) {
        if (isEmpty()) {
            return defaultValue;
        }

        return getVector();
    }

    @Override
    public DenseVector getInternal() {
        return get(null);
    }
}
