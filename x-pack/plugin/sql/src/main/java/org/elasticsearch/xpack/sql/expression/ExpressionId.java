/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Unique identifier for an expression.
 * <p>
 * We use an {@link AtomicLong} to guarantee that they are unique
 * and that they produce reproduceable values when run in subsequent
 * tests. They don't produce reproduceable values in production, but
 * you rarely debug with them in production and commonly do so in
 * tests.
 */
public class ExpressionId {
    private static final AtomicLong COUNTER = new AtomicLong();
    private final long id;

    public ExpressionId() {
        this.id = COUNTER.incrementAndGet();
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (obj == null || obj.getClass() != getClass()) {
            return false;
        }
        ExpressionId other = (ExpressionId) obj;
        return id == other.id;
    }

    @Override
    public String toString() {
        return Long.toString(id);
    }
}
