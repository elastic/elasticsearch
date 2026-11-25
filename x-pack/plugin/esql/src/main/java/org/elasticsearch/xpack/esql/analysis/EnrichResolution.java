/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.analysis;

import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.enrich.ResolvedEnrichPolicy;

import java.util.Map;

/**
 * Holds the resolution results of the enrich polices.
 * The results and errors are collected via {@link #addResolvedPolicy} and {@link #addError}.
 * And the results can be retrieved via {@link #getResolvedPolicy} and {@link #getError}
 * <p>
 * Keyed by the {@link Source} of the originating {@code ENRICH} occurrence rather than by policy name and mode: two
 * occurrences of the same policy name and mode can live in differently-scoped subqueries (e.g. one FROM-subquery branch vs.
 * another) and therefore resolve differently. {@code Source} is stable across the plan rewrites that happen between
 * pre-analysis (where resolution runs, see {@code EnrichPolicyResolver}) and analysis (where it's consumed, see
 * {@code Analyzer.ResolveEnrich}), unlike the {@code Enrich} node instance itself.
 */
public final class EnrichResolution {

    private final Map<Source, ResolvedEnrichPolicy> resolvedPolicies = ConcurrentCollections.newConcurrentMap();
    private final Map<Source, String> errors = ConcurrentCollections.newConcurrentMap();

    public ResolvedEnrichPolicy getResolvedPolicy(Source source) {
        return resolvedPolicies.get(source);
    }

    public String getError(Source source) {
        final String error = errors.get(source);
        if (error != null) {
            return error;
        } else {
            assert false : "unresolved enrich policy at [" + source + "]";
            return "unresolved enrich policy at [" + source + "]";
        }
    }

    public void addResolvedPolicy(Source source, ResolvedEnrichPolicy policy) {
        resolvedPolicies.putIfAbsent(source, policy);
    }

    public void addError(Source source, String reason) {
        errors.putIfAbsent(source, reason);
    }
}
