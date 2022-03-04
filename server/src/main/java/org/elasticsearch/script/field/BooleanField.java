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

public class BooleanField implements Field<Boolean> {

    private final String name;
    private final ScriptFieldValues.BooleanValues supplier;

    public BooleanField(String name, ScriptFieldValues.BooleanValues supplier) {
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

    public boolean get(boolean defaultValue) {
        return get(0, defaultValue);
    }

    public boolean get(int index, boolean defaultValue) {
        if (isEmpty() || index < 0 || index >= supplier.size()) {
            return defaultValue;
        }

        return supplier.get(index);
    }

    @Override
    public Iterator<Boolean> iterator() {
        return new Iterator<Boolean>() {
            private int index = 0;

            @Override
            public boolean hasNext() {
                return index < supplier.size();
            }

            @Override
            public Boolean next() {
                if (hasNext() == false) {
                    throw new NoSuchElementException();
                }
                return supplier.get(index++);
            }
        };
    }
}
