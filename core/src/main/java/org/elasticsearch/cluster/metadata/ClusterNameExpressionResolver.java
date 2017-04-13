/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.Settings;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Resolves cluster names from an expression. The expression must be the exact match of a cluster
 * name or must be a wildcard expression.
 */
public final class ClusterNameExpressionResolver extends AbstractComponent {

    private final WildcardExpressionResolver wildcardResolver = new WildcardExpressionResolver();

    public ClusterNameExpressionResolver(Settings settings) {
        super(settings);
    }

    /**
     * Resolves the provided cluster expression to matching cluster names. This method only
     * supports exact or wildcard matches.
     *
     * @param remoteClusters    the aliases for remote clusters
     * @param clusterExpression the expressions that can be resolved to cluster names.
     * @return the resolved cluster aliases.
     */
    public List<String> resolveClusterNames(Set<String> remoteClusters, String clusterExpression) {
        if (remoteClusters.contains(clusterExpression)) {
            return Collections.singletonList(clusterExpression);
        } else if (Regex.isSimpleMatchPattern(clusterExpression)) {
            return wildcardResolver.resolve(remoteClusters, clusterExpression);
        } else {
            return Collections.emptyList();
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
            if (expression.indexOf("*") == expression.length() - 1) {
                return otherWildcard(remoteClusters, expression);
            } else {
                return otherWildcard(remoteClusters, expression);
            }
        }

        private static Set<String> otherWildcard(Set<String> remoteClusters, String expression) {
            final String pattern = expression;
            return remoteClusters.stream()
                .filter(n -> Regex.simpleMatch(pattern, n))
                .collect(Collectors.toSet());
        }
    }
}
