/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.generator;

import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Context threaded through the random query generator.
 */
public final class GenerationContext {

    /**
     * Maximum nesting depth for IN subqueries.
     */
    public static final int MAX_IN_SUBQUERY_NESTING_DEPTH = 2;

    private final int subqueryDepth;
    /**
     * Set to {@code true} the first time an {@code IN (subquery)} predicate is successfully generated in this context.
     * When {@link GenerativeFeature#IN_SUBQUERY} is enabled, the probability gate in {@code maybeInSubqueryBooleanExpression} is bypassed
     * until this flag is set, so the first suitable boolean-expression position attempts generation. Child contexts created by
     * {@link #withSubqueryDepth(int)} copy the current value into a new flag so speculative inner generation cannot leak back to the
     * parent.
     */
    private final AtomicBoolean hasGeneratedInSubquery;
    private final Set<GenerativeFeature> features;

    private GenerationContext(int subqueryDepth, AtomicBoolean hasGeneratedInSubquery, Set<GenerativeFeature> features) {
        this.subqueryDepth = subqueryDepth;
        this.hasGeneratedInSubquery = hasGeneratedInSubquery;
        this.features = features;
    }

    /**
     * Root context for a top-level query with the given opt-in features.
     */
    public static GenerationContext root(Set<GenerativeFeature> features) {
        return new GenerationContext(0, new AtomicBoolean(false), features);
    }

    /**
     * How deeply nested the current generation is inside subqueries.
     * E.g. 0 for the root query, 1+ inside a subquery.
     */
    public int subqueryDepth() {
        return subqueryDepth;
    }

    /**
     * Returns {@code true} if generation is happening inside a subquery body.
     */
    public boolean isWithinASubquery() {
        return subqueryDepth > 0;
    }

    /**
     * Returns {@code true} if an {@code IN (subquery)} predicate has already been generated in this context.
     */
    public boolean hasGeneratedInSubquery() {
        return hasGeneratedInSubquery.get();
    }

    /**
     * Marks that an IN subquery has been generated in this context.
     */
    public void setHasGeneratedInSubquery() {
        hasGeneratedInSubquery.set(true);
    }

    /**
     * Restores {@link #hasGeneratedInSubquery()} after a speculative command was generated but not kept.
     */
    public void restoreHasGeneratedInSubquery(boolean value) {
        hasGeneratedInSubquery.set(value);
    }

    /**
     * Returns {@code true} if the given feature is enabled in this context.
     */
    public boolean isFeatureEnabled(GenerativeFeature feature) {
        return features.contains(feature);
    }

    /**
     * Returns a copy of this context with the given subquery nesting depth. The child starts with the parent's current
     * {@code hasGeneratedInSubquery} value but uses its own flag, so discarded inner generation cannot mark the parent.
     */
    public GenerationContext withSubqueryDepth(int subqueryDepth) {
        return new GenerationContext(subqueryDepth, new AtomicBoolean(hasGeneratedInSubquery.get()), features);
    }
}
