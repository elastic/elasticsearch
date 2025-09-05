/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.script.field.vectors;

import org.apache.lucene.util.BitUtil;
import org.apache.lucene.util.VectorUtil;

import java.util.List;

/**
 * DenseVector value type for the painless.
 * dotProduct, l1Norm, l2Norm, cosineSimilarity have three flavors depending on the type of the queryVector
 * 1) float[], this is for the ScoreScriptUtils class bindings which have converted a List based query vector into an array
 * 2) List, A painless script will typically use Lists since they are easy to pass as params and have an easy
 *      literal syntax.  Working with Lists directly, instead of converting to a float[], trades off runtime operations against
 *      memory pressure.  Dense Vectors may have high dimensionality, up to 4096.  Allocating a float[] per doc per script API
 *      call is prohibitively expensive.
 * 3) Object, the whitelisted method for the painless API.  Calls into the float[] or List version based on the
        class of the argument and checks dimensionality.
 */
public interface DenseVector {

    default void checkDimensions(int qvDims) {
        checkDimensions(getDims(), qvDims);
    }

    float[] getVector();

    float getMagnitude();

    int dotProduct(byte[] queryVector);

    double dotProduct(float[] queryVector);

    double dotProduct(List<Number> queryVector);

    @SuppressWarnings("unchecked")
    default double dotProduct(Object queryVector) {
        if (queryVector instanceof float[] floats) {
            checkDimensions(floats.length);
            return dotProduct(floats);
        } else if (queryVector instanceof List<?> list) {
            checkDimensions(list.size());
            return dotProduct((List<Number>) list);
        } else if (queryVector instanceof byte[] bytes) {
            checkDimensions(bytes.length);
            return dotProduct(bytes);
        }

        throw new IllegalArgumentException(badQueryVectorType(queryVector));
    }

    int l1Norm(byte[] queryVector);

    double l1Norm(float[] queryVector);

    double l1Norm(List<Number> queryVector);

    @SuppressWarnings("unchecked")
    default double l1Norm(Object queryVector) {
        if (queryVector instanceof float[] floats) {
            checkDimensions(floats.length);
            return l1Norm(floats);
        } else if (queryVector instanceof List<?> list) {
            checkDimensions(list.size());
            return l1Norm((List<Number>) list);
        } else if (queryVector instanceof byte[] bytes) {
            checkDimensions(bytes.length);
            return l1Norm(bytes);
        }

        throw new IllegalArgumentException(badQueryVectorType(queryVector));
    }

    int hamming(byte[] queryVector);

    int hamming(List<Number> queryVector);

    @SuppressWarnings("unchecked")
    default int hamming(Object queryVector) {
        if (queryVector instanceof List<?> list) {
            checkDimensions(list.size());
            return hamming((List<Number>) list);
        }
        if (queryVector instanceof byte[] bytes) {
            checkDimensions(bytes.length);
            return hamming(bytes);
        }

        throw new IllegalArgumentException(badQueryVectorType(queryVector));
    }

    double l2Norm(byte[] queryVector);

    double l2Norm(float[] queryVector);

    double l2Norm(List<Number> queryVector);

    @SuppressWarnings("unchecked")
    default double l2Norm(Object queryVector) {
        if (queryVector instanceof float[] floats) {
            checkDimensions(floats.length);
            return l2Norm(floats);
        } else if (queryVector instanceof List<?> list) {
            checkDimensions(list.size());
            return l2Norm((List<Number>) list);
        } else if (queryVector instanceof byte[] bytes) {
            checkDimensions(bytes.length);
            return l2Norm(bytes);
        }

        throw new IllegalArgumentException(badQueryVectorType(queryVector));
    }

    /**
     * Get the cosine similarity with the un-normalized query vector
     */
    default double cosineSimilarity(byte[] queryVector) {
        return cosineSimilarity(queryVector, getMagnitude(queryVector));
    }

    /**
     * Get the cosine similarity with the query vector
     * @param qvMagnitude - pre-calculated magnitude of the query vector
     */
    double cosineSimilarity(byte[] queryVector, float qvMagnitude);

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
    double cosineSimilarity(List<Number> queryVector);

    /**
     * Get the cosine similarity with the un-normalized query vector.  Handles queryVectors of type float[] and List.
     */
    @SuppressWarnings("unchecked")
    default double cosineSimilarity(Object queryVector) {
        if (queryVector instanceof float[] floats) {
            checkDimensions(floats.length);
            return cosineSimilarity(floats);
        } else if (queryVector instanceof List<?> list) {
            checkDimensions(list.size());
            return cosineSimilarity((List<Number>) list);
        } else if (queryVector instanceof byte[] bytes) {
            checkDimensions(bytes.length);
            return cosineSimilarity(bytes);
        }

        throw new IllegalArgumentException(badQueryVectorType(queryVector));
    }

    boolean isEmpty();

    int getDims();

    int size();

    static float getMagnitude(byte[] vector) {
        return (float) Math.sqrt(VectorUtil.dotProduct(vector, vector));
    }

    static float getMagnitude(byte[] vector, int dims) {
        int mag = 0;
        int i = 0;
        while (i < dims) {
            int elem = vector[i];
            mag += elem * elem;
            i++;
        }
        return (float) Math.sqrt(mag);
    }

    static float getBitMagnitude(byte[] vector, int dims) {
        int count = 0;
        int i = 0;
        for (int upperBound = dims & -8; i < upperBound; i += 8) {
            count += Long.bitCount((long) BitUtil.VH_NATIVE_LONG.get(vector, i));
        }

        while (i < dims) {
            count += Integer.bitCount(vector[i] & 255);
            ++i;
        }
        return (float) Math.sqrt(count);
    }

    static float getMagnitude(float[] vector) {
        return (float) Math.sqrt(VectorUtil.dotProduct(vector, vector));
    }

    static float getMagnitude(List<Number> vector) {
        double mag = 0.0f;
        for (Number number : vector) {
            float elem = number.floatValue();
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
        public int dotProduct(byte[] queryVector) {
            throw new IllegalArgumentException(MISSING_VECTOR_FIELD_MESSAGE);
        }

        @Override
        public double dotProduct(float[] queryVector) {
            throw new IllegalArgumentException(MISSING_VECTOR_FIELD_MESSAGE);
        }

        @Override
        public double dotProduct(List<Number> queryVector) {
            throw new IllegalArgumentException(MISSING_VECTOR_FIELD_MESSAGE);
        }

        @Override
        public int l1Norm(byte[] queryVector) {
            throw new IllegalArgumentException(MISSING_VECTOR_FIELD_MESSAGE);
        }

        @Override
        public double l1Norm(float[] queryVector) {
            throw new IllegalArgumentException(MISSING_VECTOR_FIELD_MESSAGE);
        }

        @Override
        public double l1Norm(List<Number> queryVector) {
            throw new IllegalArgumentException(MISSING_VECTOR_FIELD_MESSAGE);
        }

        @Override
        public int hamming(byte[] queryVector) {
            throw new IllegalArgumentException(MISSING_VECTOR_FIELD_MESSAGE);
        }

        @Override
        public int hamming(List<Number> queryVector) {
            throw new IllegalArgumentException(MISSING_VECTOR_FIELD_MESSAGE);
        }

        @Override
        public double l2Norm(byte[] queryVector) {
            throw new IllegalArgumentException(MISSING_VECTOR_FIELD_MESSAGE);
        }

        @Override
        public double l2Norm(List<Number> queryVector) {
            throw new IllegalArgumentException(MISSING_VECTOR_FIELD_MESSAGE);
        }

        @Override
        public double l2Norm(float[] queryVector) {
            throw new IllegalArgumentException(MISSING_VECTOR_FIELD_MESSAGE);
        }

        @Override
        public double cosineSimilarity(byte[] queryVector) {
            throw new IllegalArgumentException(MISSING_VECTOR_FIELD_MESSAGE);
        }

        @Override
        public double cosineSimilarity(byte[] queryVector, float qvMagnitude) {
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
        public double cosineSimilarity(List<Number> queryVector) {
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
