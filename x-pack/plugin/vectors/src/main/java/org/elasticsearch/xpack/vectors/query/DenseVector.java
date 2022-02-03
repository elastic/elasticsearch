/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.vectors.query;

import java.util.NoSuchElementException;
import java.util.PrimitiveIterator;

public interface DenseVector extends Iterable<Double> {
    float getMagnitude();

    double dotProduct(float[] queryVector);

    double l1Norm(float[] queryVector);

    double l2Norm(float[] queryVector);

    float[] getVector();

    boolean isEmpty();

    int dims();

    int size();

    DenseVector EMPTY = new DenseVector() {
        static final String MISSING_VECTOR_FIELD_MESSAGE = "Dense vector value missing for a field,"
            + " use isEmpty() to check for a missing vector value";

        @Override
        public float getMagnitude() {
            throw new IllegalArgumentException(MISSING_VECTOR_FIELD_MESSAGE);
        }

        @Override
        public double dotProduct(float[] queryVector) {
            throw new IllegalArgumentException(MISSING_VECTOR_FIELD_MESSAGE);
        }

        @Override
        public double l1Norm(float[] queryVector) {
            throw new IllegalArgumentException(MISSING_VECTOR_FIELD_MESSAGE);
        }

        @Override
        public double l2Norm(float[] queryVector) {
            throw new IllegalArgumentException(MISSING_VECTOR_FIELD_MESSAGE);
        }

        @Override
        public float[] getVector() {
            throw new IllegalArgumentException(MISSING_VECTOR_FIELD_MESSAGE);
        }

        @Override
        public boolean isEmpty() {
            return false;
        }

        @Override
        public int dims() {
            throw new IllegalArgumentException(MISSING_VECTOR_FIELD_MESSAGE);
        }

        @Override
        public int size() {
            return 0;
        }

        @Override
        public PrimitiveIterator.OfDouble iterator() {
            return new PrimitiveIterator.OfDouble() {
                @Override
                public double nextDouble() {
                    throw new NoSuchElementException();
                }

                @Override
                public boolean hasNext() {
                    return false;
                }
            };
        }
    };
}
