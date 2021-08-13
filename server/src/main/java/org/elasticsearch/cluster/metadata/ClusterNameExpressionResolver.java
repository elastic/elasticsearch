/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.transport.NoSuchRemoteClusterException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Resolves cluster names from an expression. The expression must be the exact match of a cluster
 * name or must be a wildcard expression.
 */
public final class ClusterNameExpressionResolver {

    private final WildcardExpressionResolver wildcardResolver = new WildcardExpressionResolver();

    /**
     * Resolves the provided cluster expression to matching cluster names. Supports exact or wildcard matches.
     * Throws {@link NoSuchRemoteClusterException} in case there are no registered remote clusters matching the provided expression.
     *
     * @param remoteClusters    the aliases for remote clusters
     * @param clusterExpression the expressions that can be resolved to cluster names.
     * @return the resolved cluster aliases.
     * @throws NoSuchRemoteClusterException if there are no remote clusters matching the provided expression
     */
    public List<String> resolveClusterNames(Set<String> remoteClusters, String clusterExpression) {
        if (remoteClusters.contains(clusterExpression)) {
            return Collections.singletonList(clusterExpression);
        } else if (Regex.isSimpleMatchPattern(clusterExpression)) {
            return wildcardResolver.resolve(remoteClusters, clusterExpression);
        } else {
            throw new NoSuchRemoteClusterException(clusterExpression);
        }
    }

    private static class WildcardExpressionResolver {

        private List<String> resolve(Set<String> remoteClusters, String clusterExpression) {
            if (isTrivialWildcard(clusterExpression)) {
                return resolveTrivialWildcard(remoteClusters);
            }

            Set<String> matches = matches(remoteClusters, clusterExpression);
            if (matches.isEmpty()) {
                return Collections.emptyList();
            } else {
                return new ArrayList<>(matches);
            }
        }

        private boolean isTrivialWildcard(String clusterExpression) {
            return Regex.isMatchAllPattern(clusterExpression);
        }

        private List<String> resolveTrivialWildcard(Set<String> remoteClusters) {
            return new ArrayList<>(remoteClusters);
        }

        private static Set<String> matches(Set<String> remoteClusters, String expression) {
            return otherWildcard(remoteClusters, expression);
        }

        private static Set<String> otherWildcard(Set<String> remoteClusters, String expression) {
            final String pattern = expression;
            return remoteClusters.stream()
                .filter(n -> Regex.simpleMatch(pattern, n))
                .collect(Collectors.toSet());
        }
    }
}
