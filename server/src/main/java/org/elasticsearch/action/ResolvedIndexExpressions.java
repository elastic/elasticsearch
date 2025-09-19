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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A collection of {@link ResolvedIndexExpression}, keyed by the original expression.
 *
 * <p>An example structure is:</p>
 *
 * <pre>{@code
 * {
 *   "my-index-*": {
 *      "original": "my-index-*",
 *      "localExpressions": {
 *          "expressions": ["my-index-000001", "my-index-000002"],
 *          "localIndexResolutionResult": "SUCCESS"
 *      },
 *      "remoteExpressions": ["remote1:my-index-*", "remote2:my-index-*"]
 *   },
 *   "my-index-000001": {
 *      "original": "my-index-000001",
 *      "localExpressions": {
 *          "expressions": ["my-index-000001"],
 *          "localIndexResolutionResult": "SUCCESS"
 *      },
 *      "remoteExpressions": ["remote1:my-index-000001", "remote2:my-index-000001"]
 *   }
 * }
 * }</pre>
 */
public record ResolvedIndexExpressions(Map<String, ResolvedIndexExpression> expressions) {

    public List<String> getLocalIndicesList() {
        return expressions.values().stream().flatMap(e -> e.localExpressions().expressions().stream()).toList();
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {
        private final Map<String, ResolvedIndexExpression> expressions = new LinkedHashMap<>();

        public void putLocalExpression(
            String original,
            Set<String> localExpressions,
            ResolvedIndexExpression.LocalIndexResolutionResult resolutionResult
        ) {
            // TODO is it always safe to overwrite an existing entry? exclusions can cause multiple calls for the same original
            // expression but with different local expressions
            expressions.put(
                original,
                new ResolvedIndexExpression(
                    original,
                    new ResolvedIndexExpression.LocalExpressions(new ArrayList<>(localExpressions), resolutionResult, null),
                    new ArrayList<>()
                )
            );
        }

        public void excludeAll(Set<String> expressionsToExclude) {
            if (expressionsToExclude.isEmpty() == false) {
                for (ResolvedIndexExpression prior : expressions.values()) {
                    prior.localExpressions().expressions().removeAll(expressionsToExclude);
                }
            }
        }

        public ResolvedIndexExpressions build() {
            return new ResolvedIndexExpressions(Map.copyOf(expressions));
        }
    }
}
