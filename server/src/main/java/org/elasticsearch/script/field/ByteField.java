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

public class ByteField implements Field<Byte> {

    protected final String name;
    protected final FieldSupplier.ByteSupplier supplier;

    public ByteField(String name, FieldSupplier.ByteSupplier supplier) {
        this.name = name;
        this.supplier = supplier;
    }

    @Override
    public String getName() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isEmpty() {
        return size() == 0;
    }

    @Override
    public int size() {
        return supplier.size();
    }

    public byte get(int defaultValue) {
        return get(0, defaultValue);
    }

    /**
     * Note: Constants in java and painless are ints, so letting the defaultValue be an int allows users to
     *       call this without casting. A byte variable will be automatically widened to an int.
     *       If the user does pass a value outside the range, it will be cast down to a byte.
     */
    public byte get(int index, int defaultValue) {
        if (isEmpty() || index < 0 || index >= supplier.size()) {
            return (byte) defaultValue;
        }

        return supplier.get(index);
    }

    @Override
    public Iterator<Byte> iterator() {
        return new Iterator<Byte>() {
            private int index = 0;

            @Override
            public boolean hasNext() {
                return index < supplier.size();
            }

            @Override
            public Byte next() {
                if (hasNext() == false) {
                    throw new NoSuchElementException();
                }
                return supplier.get(index++);
            }
        };
    }
}
