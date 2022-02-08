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

    // The implementations of these vector operations
    double dotProduct(QueryVector queryVector);
    double l1Norm(QueryVector queryVector);
    double l2Norm(QueryVector queryVector);

    // Take an Object to provide a consistent API if called with a float array or a list of numbers.  These
    // all call into the implementations above.
    default double dotProduct(Object queryVector) {
        if (queryVector instanceof QueryVector qv) {
            return dotProduct(qv);
        }
        return dotProduct(new QueryVector(queryVector));
    }

    default double l1Norm(Object queryVector) {
        if (queryVector instanceof QueryVector qv) {
            return l1Norm(qv);
        }
        return l1Norm(new QueryVector(queryVector));
    }

    default double l2Norm(Object queryVector) {
        if (queryVector instanceof QueryVector qv) {
            return l2Norm(qv);
        }
        return l2Norm(new QueryVector(queryVector));
    }

    float[] getVector();

    boolean isEmpty();

    int getDims();

    int size();

    DenseVector EMPTY = new DenseVector() {
        public static final String MISSING_VECTOR_FIELD_MESSAGE = "Dense vector value missing for a field,"
            + " use isEmpty() to check for a missing vector value";

        @Override
        public float getMagnitude() {
            throw new IllegalArgumentException(MISSING_VECTOR_FIELD_MESSAGE);
        }

        @Override
        public double dotProduct(QueryVector queryVector) {
            throw new IllegalArgumentException(MISSING_VECTOR_FIELD_MESSAGE);
        }

        @Override
        public double l1Norm(QueryVector queryVector) {
            throw new IllegalArgumentException(MISSING_VECTOR_FIELD_MESSAGE);
        }

        @Override
        public double l2Norm(QueryVector queryVector) {
            throw new IllegalArgumentException(MISSING_VECTOR_FIELD_MESSAGE);
        }

        @Override
        public float[] getVector() {
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
