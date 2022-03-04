/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.script.field;

import java.util.NoSuchElementException;
import java.util.PrimitiveIterator;

public class LongField implements Field<Long> {

    protected final String name;
    protected final FieldSupplier.LongSupplier supplier;

    public LongField(String name, FieldSupplier.LongSupplier supplier) {
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

    /** Returns the 0th index value as an {@code long} if it exists, otherwise {@code defaultValue}. */
    public long get(long defaultValue) {
        return get(0, defaultValue);
    }

    /** Returns the value at {@code index} as an {@code long} if it exists, otherwise {@code defaultValue}. */
    public long get(int index, long defaultValue) {
        if (isEmpty() || index < 0 || index >= supplier.size()) {
            return defaultValue;
        }

        return supplier.get(index);
    }

    @Override
    public PrimitiveIterator.OfLong iterator() {
        return new PrimitiveIterator.OfLong() {
            private int index = 0;

            @Override
            public boolean hasNext() {
                return index < supplier.size();
            }

            @Override
            public Long next() {
                return nextLong();
            }

            @Override
            public long nextLong() {
                if (hasNext() == false) {
                    throw new NoSuchElementException();
                }

                return supplier.get(index++);
            }
        };
    }
}
