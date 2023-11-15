/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper.vectors;

import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.NumericDocValues;

import java.io.IOException;

public class NormalizedCosineFloatVectorValues extends FloatVectorValues {

    private final FloatVectorValues in;
    private final NumericDocValues magnitudeIn;
    private final float[] vector;
    private float magnitude = 1f;
    private boolean hasMagnitude;
    private int docId = -1;

    public NormalizedCosineFloatVectorValues(FloatVectorValues in, NumericDocValues magnitudeIn) {
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
    public float[] vectorValue() throws IOException {
        // Lazy load vectors as we may iterate but not actually require the vector
        return vectorValue(in.docID());
    }

    @Override
    public int docID() {
        return in.docID();
    }

    @Override
    public int nextDoc() throws IOException {
        int docId = in.nextDoc();
        hasMagnitude = decodedMagnitude(docId);
        return docId;
    }

    @Override
    public int advance(int target) throws IOException {
        int docId = in.advance(target);
        hasMagnitude = decodedMagnitude(docId);
        return docId;
    }

    private float[] vectorValue(int docId) throws IOException {
        if (docId != this.docId) {
            this.docId = docId;
            // We should only copy and transform if we have a stored magnitude
            if (hasMagnitude) {
                System.arraycopy(in.vectorValue(), 0, vector, 0, dimension());
                for (int i = 0; i < vector.length; i++) {
                    vector[i] *= magnitude;
                }
                return vector;
            } else {
                return in.vectorValue();
            }
        } else {
            return hasMagnitude ? vector : in.vectorValue();
        }
    }

    private boolean decodedMagnitude(int docId) throws IOException {
        if (magnitudeIn == null) {
            return false;
        }
        int currentDoc = magnitudeIn.docID();
        if (currentDoc == NO_MORE_DOCS || docId < currentDoc) {
            magnitude = 1f;
            return false;
        } else if (docId == currentDoc) {
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
