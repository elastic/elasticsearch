/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.vectors.query;

import java.util.List;
import java.util.Objects;

/**
 * Provide consistent access to query vector elements from a backing list or float array.  Used to provide
 * a flexible API for DenseVector in painless, which does not have method overloading.
 */
public abstract class QueryVector {
    public abstract int size();

    public abstract float get(int i);

    abstract float[] asFloatArray();

    public static QueryVector fromArray(float[] vector) {
        return new ArrayQueryVector(vector);
    }

    public static QueryVector fromList(List<?> vector) {
        return new ListQueryVector(vector);
    }

    // This is for painless support, which does not support method overloading.
    public static QueryVector fromObject(Object vector) {
        if (vector instanceof List<?> list) {
            return new ListQueryVector(list);
        } else if (vector instanceof float[] array) {
            return new ArrayQueryVector(array);
        } else {
            throw new IllegalArgumentException(
                "Cannot use vector [" + vector + "] with class [" + vector.getClass().getName() + "] as query vector"
            );
        }
    }

    protected static class ListQueryVector extends QueryVector {
        protected final List<?> vector;

        private ListQueryVector(List<?> vector) {
            this.vector = Objects.requireNonNull(vector);
        }

        public int size() {
            return vector.size();
        }

        public float get(int i) {
            Object element = vector.get(i);
            if (element instanceof Number number) {
                return number.floatValue();
            }
            throw new IllegalArgumentException(
                "Cannot treat [" + element + "] at index [" + i + "] of type [" + element.getClass().getName() + "] as Number"
            );
        }

        float[] asFloatArray() {
            float[] array = new float[vector.size()];
            for (int i = 0; i < array.length; i++) {
                array[i] = get(i);
            }
            return array;
        }
    }

    protected static class ArrayQueryVector extends QueryVector {
        protected final float[] vector;

        private ArrayQueryVector(float[] vector) {
            this.vector = Objects.requireNonNull(vector);
        }

        public int size() {
            return vector.length;
        }

        public float get(int i) {
            return vector[i];
        }

        float[] asFloatArray() {
            return vector;
        }
    }
}
