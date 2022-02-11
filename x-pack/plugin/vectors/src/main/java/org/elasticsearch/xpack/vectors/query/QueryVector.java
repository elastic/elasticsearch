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
public class QueryVector {
    protected final List<?> vector;
    protected float magnitude = -1.0f;

    public QueryVector(List<?> vector) {
        this.vector = Objects.requireNonNull(vector);
    }

    public float getMagnitude() {
        if (magnitude != -1.0f) {
            return magnitude;
        }
        double mag = 0.0f;
        for (int i = 0; i < vector.size(); i++) {
            float elem = get(i);
            mag += elem * elem;
        }
        magnitude = (float) Math.sqrt(mag);
        return magnitude;
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

    public int size() {
        return vector.size();
    }
}
