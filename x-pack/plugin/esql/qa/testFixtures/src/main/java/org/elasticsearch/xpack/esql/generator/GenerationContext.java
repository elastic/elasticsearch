/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.generator;

import java.util.Set;

/**
 * Context threaded through the random query generator.
 */
public final class GenerationContext {

    private final int subqueryDepth;
    private final Set<GenerativeFeature> features;

    private GenerationContext(int subqueryDepth, Set<GenerativeFeature> features) {
        this.subqueryDepth = subqueryDepth;
        this.features = features;
    }

    /**
     * Root context for a top-level query with the given opt-in features.
     */
    public static GenerationContext root(Set<GenerativeFeature> features) {
        return new GenerationContext(0, features);
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
     * Returns {@code true} if the given feature is enabled in this context.
     */
    public boolean isFeatureEnabled(GenerativeFeature feature) {
        return features.contains(feature);
    }

    /**
     * Returns a copy of this context with the given subquery nesting depth.
     */
    public GenerationContext withSubqueryDepth(int subqueryDepth) {
        return new GenerationContext(subqueryDepth, features);
    }
}
