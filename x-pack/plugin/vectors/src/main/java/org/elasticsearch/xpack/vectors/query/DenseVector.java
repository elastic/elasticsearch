/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.vectors.query;

import java.util.List;

/**
 * DenseVector value type for the painless.
 */
/* dotProduct, l1Norm, l2Norm, cosineSimilarity have three flavors depending on the type of the queryVector
 * 1) float[], this is for the ScoreScriptUtils class bindings which have converted a List based query vector into an array
 * 2) QueryVector, a wrapped List.  A painless script will typically use Lists since they are easy to pass as params and have an easy
 *      literal syntax.  Working with Lists via QueryVector, instead of converting to a float[], trades off runtime operations against
 *      memory pressure.  Dense Vectors may have high dimensionality, up to 2048.  Allocating a float[] per doc per script API call is
 *      prohibitively expensive.
 * 3) Object, the whitelisted method for the painless API.  Calls into the float[] or QueryVector version based on the
        class of the argument and checks dimensionality. Wraps Lists as QueryVectors.
 */
public interface DenseVector {
    float[] getVector();

    float getMagnitude();

    double dotProduct(float[] queryVector);

    double dotProduct(QueryVector queryVector);

    default double dotProduct(Object queryVector) {
        if (queryVector instanceof float[] array) {
            checkDimensions(getDims(), array.length);
            return dotProduct(array);

        } else if (queryVector instanceof QueryVector qv) {
            checkDimensions(getDims(), qv.size());
            return dotProduct(qv);

        } else if (queryVector instanceof List<?> list) {
            QueryVector qv = new QueryVector(list);
            checkDimensions(getDims(), qv.size());
            return dotProduct(qv);
        }

        throw new IllegalArgumentException(badQueryVectorType(queryVector));
    }

    double l1Norm(float[] queryVector);

    double l1Norm(QueryVector queryVector);

    default double l1Norm(Object queryVector) {
        if (queryVector instanceof float[] array) {
            checkDimensions(getDims(), array.length);
            return l1Norm(array);

        } else if (queryVector instanceof QueryVector qv) {
            checkDimensions(getDims(), qv.size());
            return l1Norm(qv);

        } else if (queryVector instanceof List<?> list) {
            QueryVector qv = new QueryVector(list);
            checkDimensions(getDims(), qv.size());
            return l1Norm(qv);
        }

        throw new IllegalArgumentException(badQueryVectorType(queryVector));
    }

    double l2Norm(float[] queryVector);

    double l2Norm(QueryVector queryVector);

    default double l2Norm(Object queryVector) {
        if (queryVector instanceof float[] array) {
            checkDimensions(getDims(), array.length);
            return l2Norm(array);

        } else if (queryVector instanceof QueryVector qv) {
            checkDimensions(getDims(), qv.size());
            return l2Norm(qv);

        } else if (queryVector instanceof List<?> list) {
            QueryVector qv = new QueryVector(list);
            checkDimensions(getDims(), qv.size());
            return l2Norm(qv);
        }

        throw new IllegalArgumentException(badQueryVectorType(queryVector));
    }

    /**
     * Get the cosine similarity with the un-normalized query vector
     */
    default double cosineSimilarity(float[] queryVector) {
        return cosineSimilarity(queryVector, true);
    }

    /**
     * Get the cosine similarity with the query vector
     * @param normalizeQueryVector - normalize the query vector, does not change the contents of passed in query vector
     */
    double cosineSimilarity(float[] queryVector, boolean normalizeQueryVector);

    /**
     * Get the cosine similarity with the un-normalized query vector
     */
    double cosineSimilarity(QueryVector queryVector);

    /**
     * Get the cosine similarity with the un-normalized query vector.  Handles queryVectors of type float[], QueryVector and List.
     */
    default double cosineSimilarity(Object queryVector) {
        if (queryVector instanceof float[] array) {
            checkDimensions(getDims(), array.length);
            return cosineSimilarity(array);

        } else if (queryVector instanceof QueryVector qv) {
            checkDimensions(getDims(), qv.size());
            return cosineSimilarity(qv);

        } else if (queryVector instanceof List<?> list) {
            QueryVector qv = new QueryVector(list);
            checkDimensions(getDims(), qv.size());
            return cosineSimilarity(qv);
        }

        throw new IllegalArgumentException(badQueryVectorType(queryVector));
    }

    boolean isEmpty();

    int getDims();

    int size();

    static float getMagnitude(float[] vector) {
        double mag = 0.0f;
        for (float elem : vector) {
            mag += elem * elem;
        }
        return (float) Math.sqrt(mag);
    }

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

    DenseVector EMPTY = new DenseVector() {
        public static final String MISSING_VECTOR_FIELD_MESSAGE = "Dense vector value missing for a field,"
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
        public double dotProduct(QueryVector queryVector) {
            throw new IllegalArgumentException(MISSING_VECTOR_FIELD_MESSAGE);
        }

        @Override
        public double l1Norm(QueryVector queryVector) {
            throw new IllegalArgumentException(MISSING_VECTOR_FIELD_MESSAGE);
        }

        @Override
        public double l1Norm(float[] queryVector) {
            throw new IllegalArgumentException(MISSING_VECTOR_FIELD_MESSAGE);
        }

        @Override
        public double l2Norm(QueryVector queryVector) {
            throw new IllegalArgumentException(MISSING_VECTOR_FIELD_MESSAGE);
        }

        @Override
        public double l2Norm(float[] queryVector) {
            throw new IllegalArgumentException(MISSING_VECTOR_FIELD_MESSAGE);
        }

        @Override
        public double cosineSimilarity(float[] queryVector) {
            throw new IllegalArgumentException(MISSING_VECTOR_FIELD_MESSAGE);
        }

        @Override
        public double cosineSimilarity(float[] queryVector, boolean normalizeQueryVector) {
            throw new IllegalArgumentException(MISSING_VECTOR_FIELD_MESSAGE);
        }

        @Override
        public double cosineSimilarity(QueryVector queryVector) {
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
    };
}
