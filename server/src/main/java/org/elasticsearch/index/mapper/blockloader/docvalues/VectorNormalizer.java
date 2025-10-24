/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper.blockloader.docvalues;

import org.apache.lucene.index.NumericDocValues;

import java.io.IOException;

/**
 * Utility for normalizing dense vector values using magnitude doc values.
 * This is used for cosine similarity when vectors are stored in normalized form.
 */
public class VectorNormalizer {

    private final NumericDocValues magnitudeDocValues;

    public VectorNormalizer(NumericDocValues magnitudeDocValues) {
        this.magnitudeDocValues = magnitudeDocValues;
    }

    /**
     * Normalizes a float vector by multiplying each element by the stored magnitude.
     * If all vectors are normalized or the magnitude is not stored for this doc,
     * returns the vector unchanged.
     *
     * @param vector the vector to normalize (modified in place)
     * @param docId the document ID to get the magnitude for
     * @return the normalized vector (same instance as input)
     */
    public float[] normalize(float[] vector, int docId) throws IOException {
        // If all vectors are normalized, no doc values will be present.
        // The vector may be normalized already, so we may not have a stored magnitude for all docs
        if (magnitudeDocValues != null && magnitudeDocValues.advanceExact(docId)) {
            float magnitude = Float.intBitsToFloat((int) magnitudeDocValues.longValue());
            for (int i = 0; i < vector.length; i++) {
                vector[i] *= magnitude;
            }
        }
        return vector;
    }

    /**
     * Gets the magnitude for a specific document without modifying any vector.
     * Returns 1.0f if no magnitude is stored.
     */
    public float getMagnitude(int docId) throws IOException {
        if (magnitudeDocValues != null && magnitudeDocValues.advanceExact(docId)) {
            return Float.intBitsToFloat((int) magnitudeDocValues.longValue());
        }
        return 1.0f;
    }
}
