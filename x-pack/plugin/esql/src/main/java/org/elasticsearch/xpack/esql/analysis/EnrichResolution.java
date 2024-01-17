/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.analysis;

import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.xpack.core.enrich.EnrichPolicy;
import org.elasticsearch.xpack.ql.type.EsField;
import org.elasticsearch.xpack.ql.util.CollectionUtils;
import org.elasticsearch.xpack.ql.util.StringUtils;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Holds the resolution results of the enrich polices.
 * The results and errors are collected via {@link #addResolvedPolicy} and {@link #addError}.
 * And the results can be retrieved via {@link #getResolvedPolicy} and {@link #getError}
 */
public final class EnrichResolution {

    private final Map<String, ResolvedPolicy> resolvedPolicies = ConcurrentCollections.newConcurrentMap(); // policy name -> resolved policy
    private final Map<String, String> errors = ConcurrentCollections.newConcurrentMap(); // policy to error
    private final Set<String> existingPolicies = ConcurrentCollections.newConcurrentSet(); // for suggestion

    public ResolvedPolicy getResolvedPolicy(String policyName) {
        return resolvedPolicies.get(policyName);
    }

    public Collection<EnrichPolicy> resolvedEnrichPolicies() {
        return resolvedPolicies.values().stream().map(r -> r.policy).toList();
    }

    public String getError(String policyName) {
        final String error = errors.get(policyName);
        if (error != null) {
            return error;
        }
        return notFoundError(policyName);
    }

    public void addResolvedPolicy(
        String policyName,
        EnrichPolicy policy,
        Map<String, String> concreteIndices,
        Map<String, EsField> mapping
    ) {
        resolvedPolicies.put(policyName, new ResolvedPolicy(policy, concreteIndices, mapping));
    }

    public void addError(String policyName, String reason) {
        errors.put(policyName, reason);
    }

    public void addExistingPolicies(Set<String> policyNames) {
        existingPolicies.addAll(policyNames);
    }

    private String notFoundError(String policyName) {
        List<String> potentialMatches = StringUtils.findSimilar(policyName, existingPolicies);
        String msg = "unresolved enrich policy [" + policyName + "]";
        if (CollectionUtils.isEmpty(potentialMatches) == false) {
            msg += ", did you mean "
                + (potentialMatches.size() == 1 ? "[" + potentialMatches.get(0) + "]" : "any of " + potentialMatches)
                + "?";
        }
        return msg;
    }

    public record ResolvedPolicy(EnrichPolicy policy, Map<String, String> concreteIndices, Map<String, EsField> mapping) {

    }
}
