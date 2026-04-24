/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.generator;

/**
 * Context threaded through the random query generator.
 */
public final class GenerationContext {

    private final int subqueryDepth;

    private GenerationContext(int subqueryDepth) {
        this.subqueryDepth = subqueryDepth;
    }

    /**
     * Root context for a top-level query.
     */
    public static GenerationContext root() {
        return new GenerationContext(0);
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
     * Returns a copy of this context with the given subquery nesting depth.
     */
    public GenerationContext withSubqueryDepth(int subqueryDepth) {
        return new GenerationContext(subqueryDepth);
    }
}
