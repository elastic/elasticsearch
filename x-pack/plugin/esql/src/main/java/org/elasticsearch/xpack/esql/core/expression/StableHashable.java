/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.core.expression;

/**
 * Opt-in interface for expressions that can produce a stable hash code — one
 * that does not include runtime-assigned {@link NameId}s and is therefore
 * consistent across JVM runs.  Used by {@link #compute(Expression)} to sort
 * commutative children into a canonical order.
 *
 * <p>Leaf expressions ({@link Attribute}, {@link Literal}, …) must implement
 * this interface because the default recursion in {@link #compute(Expression)}
 * has no children to recurse into.  Composite expressions obtain a stable hash
 * automatically via the recursive fallback in {@code compute()}.
 */
public interface StableHashable {

    /** Returns a stable hash code for this expression (no {@link NameId}). */
    int stableHash();

    /**
     * Returns a stable hash for {@code e}: delegates to {@link #stableHash()}
     * when {@code e} implements this interface, otherwise recurses over
     * {@link Expression#children()} using the expression's class hash as a seed.
     */
    static int compute(Expression e) {
        if (e instanceof StableHashable sh) {
            return sh.stableHash();
        }
        int h = e.getClass().hashCode();
        for (Expression child : e.children()) {
            h = 31 * h + compute(child);
        }
        return h;
    }
}
