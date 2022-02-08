/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.vectors.query;

import java.util.List;

/**
 * Provide consistent access to query vector elements from a backing list or float array.  Used to provide
 * a flexible API for DenseVector in painless, which does not have method overloading.
 */
public class QueryVector {
    protected final float[] array;
    protected final List<?> list;

    public QueryVector(float[] vector) {
        this.array = vector;
        this.list = null;
    }

    public QueryVector(List<?> vector) {
        this.array = null;
        this.list = vector;
    }

    public QueryVector(Object vector) {
        if (vector instanceof List<?> list) {
            this.array = null;
            this.list = list;
        } else if (vector instanceof float[] array) {
            this.array = array;
            this.list = null;
        } else {
            throw new IllegalArgumentException(
                "Cannot use vector [" + vector + "] with class [" + vector.getClass().getName() + "] as query vector"
            );
        }
    }

    public int size() {
        if (array != null) {
            return array.length;
        } else {
            assert list != null;
            return list.size();
        }
    }

    public float get(int i) {
        if (array != null) {
            return array[i];
        }
        assert list != null;
        return getFromList(list, i);
    }

    protected static float getFromList(List<?> list, int i) {
        Object element = list.get(i);
        if (element instanceof Number number) {
            return number.floatValue();
        }
        throw new IllegalArgumentException(badElement(element, i));
    }

    protected static String badElement(Object element, int index) {
        return "Cannot treat [" + element + "] at index [" + index + "] of type [" + element.getClass().getName() + "] as Number";
    }

    float[] asFloatArray() {
        if (array != null) {
            return array;
        }
        assert list != null;

        float[] array = new float[list.size()];
        for (int i = 0; i < array.length; i++) {
            Object element = list.get(i);
            if (element instanceof Number number) {
                array[i++] = number.floatValue();
            } else {
                throw new IllegalArgumentException(badElement(element, i));
            }
        }
        return array;
    }
}
