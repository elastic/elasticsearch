/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.vectors.query;

import java.util.List;
import java.util.NoSuchElementException;
import java.util.PrimitiveIterator;

public interface DenseVector extends Iterable<Double> {
    float getMagnitude();

    double dotProduct(float[] queryVector);

    default double dotProduct(List<?> queryVector) {
        return dotProduct(asFloatArray(queryVector));
    }

    default double dotProduct(Object queryVector) {
        if (queryVector instanceof float[] array) {
            return dotProduct(array);
        } else if (queryVector instanceof List<?> list) {
            return dotProduct(list);
        }
        throw new IllegalArgumentException(badContainer("dotProduct", queryVector));
    }

    double l1Norm(float[] queryVector);

    default double l1Norm(List<?> queryVector) {
        return l1Norm(asFloatArray(queryVector));
    }

    default double l1Norm(Object queryVector) {
        if (queryVector instanceof float[] array) {
            return l1Norm(array);
        } else if (queryVector instanceof List<?> list) {
            return l1Norm(list);
        }
        throw new IllegalArgumentException(badContainer("l1Norm", queryVector));
    }

    double l2Norm(float[] queryVector);

    default double l2Norm(List<?> queryVector) {
        return l2Norm(asFloatArray(queryVector));
    }

    default double l2Norm(Object queryVector) {
        if (queryVector instanceof float[] array) {
            return l2Norm(array);
        } else if (queryVector instanceof List<?> list) {
            return l2Norm(list);
        }
        throw new IllegalArgumentException(badContainer("l2Norm", queryVector));
    }

    float[] getVector();

    boolean isEmpty();

    int getDims();

    int size();

    private static String badContainer(String operation, Object queryVector) {
        return "Cannot perform "
            + operation
            + " on object ["
            + queryVector
            + "] with class ["
            + queryVector.getClass()
            + "], must be a float[] or List of Number";
    }

    static String badElement(Object element) {
        return "Cannot treat [" + element + "] of type [" + element.getClass().getName() + "] as Number";
    }

    static String badElement(Object element, int index) {
        return "Cannot treat [" + element + "] at index [" + index + "] of type [" + element.getClass().getName() + "] as Number";
    }

    static float[] asFloatArray(List<?> list) {
        float[] array = new float[list.size()];
        int i = 0;
        for (Object element : list) {
            if (element instanceof Number number) {
                array[i++] = number.floatValue();
            } else {
                throw new IllegalArgumentException(badElement(element, i));
            }
        }
        return array;
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
