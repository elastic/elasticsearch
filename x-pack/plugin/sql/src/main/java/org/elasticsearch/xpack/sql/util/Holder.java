/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.sql.util;

/**
 * Simply utility class used for setting a state, typically
 * for closures (which require outside variables to be final).
 */
public class Holder<T> {

    private T value = null;

    public Holder() {
    }

    public Holder(T value) {
        this.value = value;
    }

    public void set(T value) {
        this.value = value;
    }

    public T get() {
        return value;
    }
}
