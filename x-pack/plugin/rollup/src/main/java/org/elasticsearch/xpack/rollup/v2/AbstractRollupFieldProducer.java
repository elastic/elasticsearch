/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.rollup.v2;

/**
 * Base class for classes that read metric and label fields.
 */
abstract class AbstractRollupFieldProducer<T> {

    protected final String name;
    protected boolean isEmpty;

    AbstractRollupFieldProducer(String name) {
        this.name = name;
        this.isEmpty = true;
    }

    /**
     * Collect a value for the field applying the specific subclass collection strategy.
     * @param value the value to collect.
     */
    public abstract void collect(T value);

    /**
     * @return the name of the field.
     */
    public String name() {
        return name;
    }

    /**
     * @return the value of the field.
     */
    public abstract Object value();

    /**
     * Resets the collected value to the specific subclass reset value.
     */
    public abstract void reset();

    /**
     * @return true if the field has not collected any value.
     */
    public boolean isEmpty() {
        return isEmpty;
    }
}
