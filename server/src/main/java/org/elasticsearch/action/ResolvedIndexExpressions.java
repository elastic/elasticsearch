/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
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

        public void putLocalExpressions(
            String original,
            Set<String> localExpressions,
            ResolvedIndexExpression.LocalIndexResolutionResult resolutionResult
        ) {
            expressions.add(
                new ResolvedIndexExpression(
                    original,
                    new ResolvedIndexExpression.LocalExpressions(new HashSet<>(localExpressions), resolutionResult, null),
                    new HashSet<>()
                )
            );
        }

        public void excludeAll(Set<String> expressionsToExclude) {
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
