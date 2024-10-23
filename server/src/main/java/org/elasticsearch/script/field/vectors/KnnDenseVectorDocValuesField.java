/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.script.field.vectors;

import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.KnnVectorValues;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.mapper.vectors.DenormalizedCosineFloatVectorValues;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper.ElementType;
import org.elasticsearch.index.mapper.vectors.DenseVectorScriptDocValues;

import java.io.IOException;

import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;

public class KnnDenseVectorDocValuesField extends DenseVectorDocValuesField {
    protected final FloatVectorValues input; // null if no vectors
    protected final KnnVectorValues.DocIndexIterator iterator;
    protected float[] vector;
    protected final int dims;

    public KnnDenseVectorDocValuesField(@Nullable FloatVectorValues input, String name, int dims) {
        super(name, ElementType.FLOAT);
        this.dims = dims;
        this.input = input;
        this.iterator = input == null ? null : input.iterator();
    }

    @Override
    public void setNextDocId(int docId) throws IOException {
        if (input == null) {
            return;
        }
        int currentDoc = iterator.docID();
        if (currentDoc == NO_MORE_DOCS || docId < currentDoc) {
            vector = null;
        } else if (docId == currentDoc) {
            vector = input.vectorValue(iterator.index());
        } else {
            currentDoc = iterator.advance(docId);
            if (currentDoc == docId) {
                vector = input.vectorValue(iterator.index());
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

    @Override
    public DenseVector get() {
        if (isEmpty()) {
            return DenseVector.EMPTY;
        }

        if (input instanceof DenormalizedCosineFloatVectorValues normalized) {
            return new KnnDenseVector(vector, normalized.magnitude());
        }
        return new KnnDenseVector(vector);
    }

    @Override
    public DenseVector get(DenseVector defaultValue) {
        if (isEmpty()) {
            return defaultValue;
        }

        if (input instanceof DenormalizedCosineFloatVectorValues normalized) {
            return new KnnDenseVector(vector, normalized.magnitude());
        }
        return new KnnDenseVector(vector);
    }

    @Override
    public DenseVector getInternal() {
        return get(null);
    }
}
