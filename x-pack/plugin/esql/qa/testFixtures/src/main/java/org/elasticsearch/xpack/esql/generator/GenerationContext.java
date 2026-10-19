/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.generator;

import org.elasticsearch.xpack.esql.action.EsqlCapabilities;

import java.util.Set;
import java.util.function.Predicate;

/**
 * Context threaded through the random query generator.
 */
public final class GenerationContext {

    private final int subqueryDepth;
    private final Set<GenerativeFeature> features;
    private final Predicate<String> clusterCapabilities;

    private GenerationContext(int subqueryDepth, Set<GenerativeFeature> features, Predicate<String> clusterCapabilities) {
        this.subqueryDepth = subqueryDepth;
        this.features = features;
        this.clusterCapabilities = clusterCapabilities;
    }

    /**
     * Root context for a top-level query with the given opt-in features.
     *
     * @param clusterCapabilities tests whether the cluster under test supports an ES|QL capability, by name
     */
    public static GenerationContext root(Set<GenerativeFeature> features, Predicate<String> clusterCapabilities) {
        return new GenerationContext(0, features, clusterCapabilities);
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
     * Returns {@code true} if the cluster under test supports {@code cap}. Use this rather than
     * {@link EsqlCapabilities.Cap#isEnabled()}, which describes the test's own build: the cluster may run an older version.
     */
    public boolean clusterSupports(EsqlCapabilities.Cap cap) {
        return clusterSupports(cap.capabilityName());
    }

    /**
     * Returns {@code true} if the cluster under test advertises {@code capability}, for capabilities that aren't
     * {@link EsqlCapabilities.Cap}s, like the {@code fn_<name>_<capability>} ones functions declare.
     */
    public boolean clusterSupports(String capability) {
        return clusterCapabilities.test(capability);
    }

    /**
     * Returns a copy of this context with the given subquery nesting depth.
     */
    public GenerationContext withSubqueryDepth(int subqueryDepth) {
        return new GenerationContext(subqueryDepth, features, clusterCapabilities);
    }
}
