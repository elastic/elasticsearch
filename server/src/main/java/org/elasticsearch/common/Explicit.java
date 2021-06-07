/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common;

import java.util.Objects;

/**
 * Holds a value that is either:
 * a) set implicitly e.g. through some default value
 * b) set explicitly e.g. from a user selection
 *
 * When merging conflicting configuration settings such as
 * field mapping settings it is preferable to preserve an explicit
 * choice rather than a choice made only made implicitly by defaults.
 *
 */
public class Explicit<T> {

    private final T value;
    private final boolean explicit;
    /**
     * Create a value with an indication if this was an explicit choice
     * @param value a setting value
     * @param explicit true if the value passed is a conscious decision, false if using some kind of default
     */
    public Explicit(T value, boolean explicit) {
        this.value = value;
        this.explicit = explicit;
    }

    public T value() {
        return this.value;
    }

    /**
     *
     * @return true if the value passed is a conscious decision, false if using some kind of default
     */
    public boolean explicit() {
        return this.explicit;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Explicit<?> explicit1 = (Explicit<?>) o;
        return explicit == explicit1.explicit &&
            Objects.equals(value, explicit1.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(value, explicit);
    }
}
