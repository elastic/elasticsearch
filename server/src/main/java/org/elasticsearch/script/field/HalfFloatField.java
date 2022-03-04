/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.script.field;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

public class HalfFloatField implements Field<Float> {

    protected final String name;
    protected final FieldSupplier.FloatSupplier supplier;

    public HalfFloatField(String name, FieldSupplier.FloatSupplier supplier) {
        this.name = name;
        this.supplier = supplier;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public boolean isEmpty() {
        return supplier.size() == 0;
    }

    @Override
    public int size() {
        return supplier.size();
    }

    /**
     * Does a downcast for defaultValue from a double to a float
     * to allow users to avoid explicit casting.
     */
    public float get(double defaultValue) {
        return get(0, defaultValue);
    }

    /**
     * Does a downcast for defaultValue from a double to a float
     * to allow users to avoid explicit casting.
     */
    public float get(int index, double defaultValue) {
        if (isEmpty() || index < 0 || index >= supplier.size()) {
            return (float) defaultValue;
        }

        return supplier.get(index);
    }

    @Override
    public Iterator<Float> iterator() {
        return new Iterator<Float>() {
            private int index = 0;

            @Override
            public boolean hasNext() {
                return index < supplier.size();
            }

            @Override
            public Float next() {
                if (hasNext() == false) {
                    throw new NoSuchElementException();
                }
                return supplier.get(index++);
            }
        };
    }

    /** Converts all the values to {@code Double} and returns them as a {@code List}. */
    public List<Double> asDoubles() {
        if (isEmpty()) {
            return Collections.emptyList();
        }

        List<Double> doubleValues = new ArrayList<>(supplier.size());

        for (int index = 0; index < supplier.size(); ++index) {
            doubleValues.add((double)supplier.get(index));
        }

        return doubleValues;
    }

    /** Returns the 0th index value as a {@code double} if it exists, otherwise {@code defaultValue}. */
    public double asDouble(double defaultValue) {
        return asDouble(0, defaultValue);
    }

    /** Returns the value at {@code index} as a {@code double} if it exists, otherwise {@code defaultValue}. */
    public double asDouble(int index, double defaultValue) {
        if (isEmpty() || index < 0 || index >= supplier.size()) {
            return defaultValue;
        }

        return supplier.get(index);
    }
}
