/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper.vectors;

import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.search.VectorScorer;

import java.io.IOException;

/**
 * Provides the denormalized vectors. Float vectors stored with cosine similarity are normalized by default. So when reading the value
 * for scripts, we to denormalize them.
 */
public class DenormalizedCosineFloatVectorValues extends FloatVectorValues {

    private final FloatVectorValues in;
    private final NumericDocValues magnitudeIn;
    private final float[] vector;
    private float magnitude = 1f;
    private boolean hasMagnitude;
    private int docId = -1;

    public DenormalizedCosineFloatVectorValues(FloatVectorValues in, NumericDocValues magnitudeIn) {
        this.in = in;
        this.magnitudeIn = magnitudeIn;
        this.vector = new float[in.dimension()];
    }

    @Override
    public int dimension() {
        return in.dimension();
    }

    @Override
    public int size() {
        return in.size();
    }

    @Override
    public DocIndexIterator iterator() {
        return in.iterator();
    }

    @Override
    public FloatVectorValues copy() throws IOException {
        return in.copy();
    }

    @Override
    public VectorScorer scorer(float[] floats) throws IOException {
        return in.scorer(floats);
    }

    public float magnitude() {
        return magnitude;
    }

    @Override
    public float[] vectorValue(int ord) throws IOException {
        int docId = ordToDoc(ord);
        if (docId != this.docId) {
            this.docId = docId;
            hasMagnitude = decodedMagnitude(docId);
            // We should only copy and transform if we have a stored a non-unit length magnitude
            if (hasMagnitude) {
                System.arraycopy(in.vectorValue(ord), 0, vector, 0, dimension());
                for (int i = 0; i < vector.length; i++) {
                    vector[i] *= magnitude;
                }
                return vector;
            } else {
                return in.vectorValue(ord);
            }
        } else {
            return hasMagnitude ? vector : in.vectorValue(ord);
        }
    }

    private boolean decodedMagnitude(int docId) throws IOException {
        if (magnitudeIn == null) {
            return false;
        }
        int currentDoc = magnitudeIn.docID();
        if (docId == currentDoc) {
            return true;
        } else {
            if (magnitudeIn.advanceExact(docId)) {
                magnitude = Float.intBitsToFloat((int) magnitudeIn.longValue());
                return true;
            } else {
                magnitude = 1f;
                return false;
            }
        }
    }
}
