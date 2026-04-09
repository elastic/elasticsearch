/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.script.field;

import java.util.Collections;
import java.util.Iterator;

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
        return Collections.emptyIterator();
    }
}
