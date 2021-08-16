/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.script;

/**
 * Helper for creating {@link Converter} classes which delegates all un-overridden methods to the underlying
 * {@link FieldValues}.
 */
public abstract class DelegatingFieldValues<T, D> implements FieldValues<T> {
    protected FieldValues<D> values;

    public DelegatingFieldValues(FieldValues<D> values) {
        this.values = values;
    }

    @Override
    public boolean isEmpty() {
        return values.isEmpty();
    }

    @Override
    public int size() {
        return values.size();
    }

    @Override
    public long getLongValue() {
        return values.getLongValue();
    }

    @Override
    public double getDoubleValue() {
        return values.getDoubleValue();
    }
}
