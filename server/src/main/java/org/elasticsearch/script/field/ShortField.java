/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.script.field;

import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.util.ArrayUtil;
import org.elasticsearch.index.fielddata.ScriptDocValues;

import java.io.IOException;
import java.util.Iterator;
import java.util.NoSuchElementException;

public class ShortField implements Field<Short> {

    protected final String name;
    protected final FieldSupplier.ShortSupplier supplier;

    public ShortField(String name, FieldSupplier.ShortSupplier supplier) {
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

    public short get(int defaultValue) {
        return get(0, defaultValue);
    }

    /**
     * Note: Constants in java and painless are ints, so letting the defaultValue be an int allows users to
     *       call this without casting. A short variable will be automatically widened to an int.
     *       If the user does pass a value outside the range, it will be cast down to a short.
     */
    public short get(int index, int defaultValue) {
        if (isEmpty() || index < 0 || index >= supplier.size()) {
            return (short) defaultValue;
        }

        return supplier.get(index);
    }

    @Override
    public Iterator<Short> iterator() {
        return new Iterator<Short>() {
            private int index = 0;

            @Override
            public boolean hasNext() {
                return index < supplier.size();
            }

            @Override
            public Short next() {
                if (hasNext() == false) {
                    throw new NoSuchElementException();
                }

                return supplier.get(index++);
            }
        };
    }
}
