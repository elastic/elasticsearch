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

/**
 * A script {@code Field} with no mapping, always returns {@code defaultValue}.
 */
public class EmptyField implements Field<Object> {

    private final String name;

    public EmptyField(String name) {
        this.name = name;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public boolean isEmpty() {
        return true;
    }

    @Override
    public int size() {
        return 0;
    }

    public Object get(Object defaultValue) {
        return get(0, defaultValue);
    }

    public Object get(int index, Object defaultValue) {
        return defaultValue;
    }

    @Override
    public Iterator<Object> iterator() {
        return new Iterator<>() {
            @Override
            public boolean hasNext() {
                return false;
            }

            @Override
            public Object next() {
                throw new NoSuchElementException();
            }
        };
    }
}
