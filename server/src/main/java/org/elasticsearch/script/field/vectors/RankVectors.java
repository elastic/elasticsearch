/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.script.field.vectors;

import java.util.Iterator;

public interface RankVectors {

    default void checkDimensions(int qvDims) {
        checkDimensions(getDims(), qvDims);
    }

    float maxSimDotProduct(float[][] query);

    float maxSimDotProduct(byte[][] query);

    float maxSimInvHamming(byte[][] query);

    Iterator<float[]> getVectors();

    float[] getMagnitudes();

    boolean isEmpty();

    int getDims();

    int size();

    static void checkDimensions(int dvDims, int qvDims) {
        if (dvDims != qvDims) {
            throw new IllegalArgumentException(
                "The query vector has a different number of dimensions [" + qvDims + "] than the document vectors [" + dvDims + "]."
            );
        }
    }

    private static String badQueryVectorType(Object queryVector) {
        return "Cannot use vector [" + queryVector + "] with class [" + queryVector.getClass().getName() + "] as query vector";
    }

    RankVectors EMPTY = new RankVectors() {
        public static final String MISSING_VECTOR_FIELD_MESSAGE = "rank-vectors value missing for a field,"
            + " use isEmpty() to check for a missing value";

        @Override
        public Iterator<float[]> getVectors() {
            throw new IllegalArgumentException(MISSING_VECTOR_FIELD_MESSAGE);
        }

        @Override
        public float[] getMagnitudes() {
            throw new IllegalArgumentException(MISSING_VECTOR_FIELD_MESSAGE);
        }

        @Override
        public boolean isEmpty() {
            return true;
        }

        @Override
        public int getDims() {
            throw new IllegalArgumentException(MISSING_VECTOR_FIELD_MESSAGE);
        }

        @Override
        public float maxSimDotProduct(float[][] query) {
            throw new IllegalArgumentException(MISSING_VECTOR_FIELD_MESSAGE);
        }

        @Override
        public float maxSimDotProduct(byte[][] query) {
            throw new IllegalArgumentException(MISSING_VECTOR_FIELD_MESSAGE);
        }

        @Override
        public float maxSimInvHamming(byte[][] query) {
            throw new IllegalArgumentException(MISSING_VECTOR_FIELD_MESSAGE);
        }

        @Override
        public int size() {
            return 0;
        }
    };
}
