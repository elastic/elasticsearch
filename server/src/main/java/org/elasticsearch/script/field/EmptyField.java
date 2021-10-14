/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.script.field;

import java.util.Collections;
import java.util.List;

/**
 * Script field with no mapping, always returns {@code defaultValue}.
 */
public class EmptyField<T> implements Field<T> {

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

    @Override
    public List<T> getValues() {
        return Collections.emptyList();
    }

    @Override
    public T getValue(T defaultValue) {
        return defaultValue;
    }

    @Override
    public T getValue(int index, T defaultValue) {
        return defaultValue;
    }
}
