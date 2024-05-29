/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.analysis;

import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.xpack.esql.enrich.ResolvedEnrichPolicy;
import org.elasticsearch.xpack.esql.plan.logical.Enrich;

import java.util.Collection;
import java.util.Map;

/**
 * Holds the resolution results of the enrich polices.
 * The results and errors are collected via {@link #addResolvedPolicy} and {@link #addError}.
 * And the results can be retrieved via {@link #getResolvedPolicy} and {@link #getError}
 */
public final class EnrichResolution {

    private final Map<Key, ResolvedEnrichPolicy> resolvedPolicies = ConcurrentCollections.newConcurrentMap();
    private final Map<Key, String> errors = ConcurrentCollections.newConcurrentMap();

    public ResolvedEnrichPolicy getResolvedPolicy(String policyName, Enrich.Mode mode) {
        return resolvedPolicies.get(new Key(policyName, mode));
    }

    public Collection<ResolvedEnrichPolicy> resolvedEnrichPolicies() {
        return resolvedPolicies.values();

    }

    public String getError(String policyName, Enrich.Mode mode) {
        final String error = errors.get(new Key(policyName, mode));
        if (error != null) {
            return error;
        } else {
            assert false : "unresolved enrich policy [" + policyName + "] mode [" + mode + "]";
            return "unresolved enrich policy [" + policyName + "] mode [" + mode + "]";
        }
    }

    public void addResolvedPolicy(String policyName, Enrich.Mode mode, ResolvedEnrichPolicy policy) {
        resolvedPolicies.putIfAbsent(new Key(policyName, mode), policy);
    }

    public void addError(String policyName, Enrich.Mode mode, String reason) {
        errors.putIfAbsent(new Key(policyName, mode), reason);
    }

    private record Key(String policyName, Enrich.Mode mode) {

    }
}
