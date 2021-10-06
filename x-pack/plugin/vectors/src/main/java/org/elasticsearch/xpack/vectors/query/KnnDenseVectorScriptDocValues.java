/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */


package org.elasticsearch.xpack.vectors.query;

import org.apache.lucene.index.VectorValues;
import org.apache.lucene.util.VectorUtil;

import java.io.IOException;
import java.util.Arrays;

import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;

public class KnnDenseVectorScriptDocValues extends DenseVectorScriptDocValues {
    private final VectorValues in;
    private float[] vector;

    KnnDenseVectorScriptDocValues(VectorValues in, int dims) {
        super(dims);
        this.in = in;
    }

    @Override
    public void setNextDocId(int docId) throws IOException {
        int currentDoc = in.docID();
        if (currentDoc == NO_MORE_DOCS || docId < currentDoc) {
            vector = null;
        } else if (docId == currentDoc) {
            vector = in.vectorValue();
        } else {
            currentDoc = in.advance(docId);
            if (currentDoc == docId) {
                vector = in.vectorValue();
            } else {
                vector = null;
            }
        }
    }

    private float[] getVectorChecked() {
        if (vector == null) {
            throw new IllegalArgumentException(MISSING_VECTOR_FIELD_MESSAGE);
        }
        return vector;
    }

    @Override
    public float[] getVectorValue() {
        float[] vector = getVectorChecked();
        // we need to copy the value, since {@link VectorValues} can reuse
        // the underlying array across documents
        return Arrays.copyOf(vector, vector.length);
    }

    @Override
    public float getMagnitude() {
        float[] vector = getVectorChecked();
        double magnitude = 0.0f;
        for (float elem : vector) {
            magnitude += elem * elem;
        }
        return (float) Math.sqrt(magnitude);
    }

    @Override
    public double dotProduct(float[] queryVector) {
        return VectorUtil.dotProduct(getVectorChecked(), queryVector);
    }

    @Override
    public double l1Norm(float[] queryVector) {
        float[] vectorValue = getVectorChecked();
        double result = 0.0;
        for (int i = 0; i < queryVector.length; i++) {
            result += Math.abs(vectorValue[i] - queryVector[i]);
        }
        return result;
    }

    @Override
    public double l2Norm(float[] queryVector) {
        return Math.sqrt(VectorUtil.squareDistance(getVectorValue(), queryVector));
    }

    @Override
    public int size() {
        if (vector == null) {
            return 0;
        } else {
            return 1;
        }
    }
}
