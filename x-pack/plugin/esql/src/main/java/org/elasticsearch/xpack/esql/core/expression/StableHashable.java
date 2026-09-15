/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.core.expression;

/**
 * Opt-in interface for expressions that can produce a stable hash code - one
 * that does not include runtime-assigned {@link NameId}s and is therefore
 * consistent across JVM runs. Used by {@link #compute(Expression)} to sort
 * commutative children into a canonical order.
 *
 * <p>Every expression type that may appear as a commutative child in
 * {@code BinaryOperator.canonicalize()} or {@code In.canonicalize()} must
 * implement this interface. {@link Attribute} and {@link Literal} are the
 * primary implementors.
 */
public interface StableHashable {

    /** Returns a stable hash code for this expression (no {@link NameId}). */
    int stableHash();

    /**
     * Returns a stable hash for {@code e}.
     *
     * @throws IllegalStateException if {@code e} does not implement {@link StableHashable}
     */
    static int compute(Expression e) {
        if (e instanceof StableHashable sh) {
            return sh.stableHash();
        }
        throw new IllegalStateException("Expression does not implement StableHashable: " + e.getClass().getName());
    }
}
