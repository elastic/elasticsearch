/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.common;

import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.Objects;

/**
 * Glorified list for managing {@link Failure}s.
 */
public class Failures {

    private final Collection<Failure> failures;

    public Failures() {
        this.failures = new LinkedHashSet<>();
    }

    public Failures add(Failure failure) {
        if (failure != null) {
            failures.add(failure);
        }
        return this;
    }

    public boolean hasFailures() {
        return failures.size() > 0;
    }

    public Collection<Failure> failures() {
        return failures;
    }

    @Override
    public int hashCode() {
        return Objects.hash(failures);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Failures failures1 = (Failures) o;
        return Objects.equals(failures, failures1.failures);
    }

    @Override
    public String toString() {
        return failures.isEmpty() ? "[]" : Failure.failMessage(failures);
    }
}
