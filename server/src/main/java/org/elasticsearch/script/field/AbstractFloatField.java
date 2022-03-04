/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.script.field;

import java.util.Iterator;
import java.util.NoSuchElementException;

public abstract class AbstractFloatField implements Field<Float> {

    protected final String name;
    protected final FieldSupplier.FloatSupplier supplier;

    public AbstractFloatField(String name, FieldSupplier.FloatSupplier supplier) {
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

    public float get(double defaultValue) {
        return get(0, defaultValue);
    }

    public float get(int index, double defaultValue) {
        if (isEmpty() || index < 0 || index >= supplier.size()) {
            return (float)defaultValue;
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

}
