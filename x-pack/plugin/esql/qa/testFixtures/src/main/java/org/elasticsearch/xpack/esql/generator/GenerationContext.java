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

    public int subqueryDepth() {
        return subqueryDepth;
    }

    public boolean inSubquery() {
        return subqueryDepth > 0;
    }

    public GenerationContext withSubqueryDepth(int subqueryDepth) {
        return new GenerationContext(subqueryDepth);
    }
}
