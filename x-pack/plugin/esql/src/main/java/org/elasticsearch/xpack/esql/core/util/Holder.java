/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.core.util;

import java.util.function.Supplier;

/**
 * Simply utility class used for setting a state, typically
 * for closures (which require outside variables to be final).
 */
public class Holder<T> {

    private T value = null;

    public Holder() {}

    public Holder(T value) {
        this.value = value;
    }

    @SuppressWarnings("HiddenField")
    public void set(T value) {
        this.value = value;
    }

    /**
     * Sets a value in the holder, but only if none has already been set.
     * @param value the new value to set.
     */
    public void setIfAbsent(T value) {
        if (this.value == null) {
            this.value = value;
        }
    }

    /**
     * Sets a value in the holder, but only if none has already been set.
     * @param value the new value to set.
     */
    public void setOnce(T value) {
        assert this.value == null : "Value has already been set to " + this.value;
        this.value = value;
    }

    public T get() {
        return value;
    }

    public T getOrDefault(Supplier<T> defaultValue) {
        return value != null ? value : defaultValue.get();
    }
}
