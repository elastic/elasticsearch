/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.rollup.v2;

public abstract class AbstractFieldProducer<T> implements Collectable<T> {

    protected final String field;
    protected boolean isEmpty;

    public AbstractFieldProducer(String field) {
        this.field = field;
        this.isEmpty = true;
    }

    @Override
    public abstract void collect(T value);

    public String field() {
        return field;
    }

    public abstract Object value();

    public abstract void reset();

    public boolean isEmpty() {
        return isEmpty;
    }
}
