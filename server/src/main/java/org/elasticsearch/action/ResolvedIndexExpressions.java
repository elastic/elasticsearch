/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action;

import org.elasticsearch.action.ResolvedIndexExpression.LocalExpressions;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * A collection of {@link ResolvedIndexExpression}.
 */
public record ResolvedIndexExpressions(List<ResolvedIndexExpression> expressions) {

    public List<String> getLocalIndicesList() {
        return expressions.stream().flatMap(e -> e.localExpressions().expressions().stream()).toList();
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {
        private final List<ResolvedIndexExpression> expressions = new ArrayList<>();

        /**
         * @param original         the original expression that was resolved -- may be blank for "access all" cases
         * @param localExpressions is a HashSet as an optimization -- the set needs to be mutable, and we want to avoid copying it.
         *                         May be empty.
         */
        public void addLocalExpressions(
            String original,
            HashSet<String> localExpressions,
            ResolvedIndexExpression.LocalIndexResolutionResult resolutionResult
        ) {
            Objects.requireNonNull(original);
            Objects.requireNonNull(localExpressions);
            Objects.requireNonNull(resolutionResult);
            expressions.add(
                new ResolvedIndexExpression(original, new LocalExpressions(localExpressions, resolutionResult, null), new HashSet<>())
            );
        }

        /**
         * Exclude the given expressions from the local expressions of all prior added {@link ResolvedIndexExpression}.
         */
        public void excludeFromLocalExpressions(Set<String> expressionsToExclude) {
            Objects.requireNonNull(expressionsToExclude);
            if (expressionsToExclude.isEmpty() == false) {
                for (ResolvedIndexExpression prior : expressions) {
                    prior.localExpressions().expressions().removeAll(expressionsToExclude);
                }
            }
        }

        public ResolvedIndexExpressions build() {
            return new ResolvedIndexExpressions(expressions);
        }
    }
}
