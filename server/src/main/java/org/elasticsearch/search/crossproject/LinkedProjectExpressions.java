/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.crossproject;

import org.elasticsearch.action.ResolvedIndexExpression;
import org.elasticsearch.action.ResolvedIndexExpressions;

import java.util.HashMap;
import java.util.Map;

/**
 * This class allows capturing context about index expression replacements performed on a linked project.
 * <p>
 * The replacements are keyed by the original index expression and have as value {@link ResolvedIndexExpression.LocalExpressions} that
 * contains the set of expression (if any) was found on the remote, the result of the resolution and possibly the exception thrown.
 *
 * <p>An example structure is:</p>
 *
 * <pre>{@code
 * {
 *   "P*:my-index-*": {
 *     "expressions": ["my-index-000001", "my-index-000002"],
 *     "localIndexResolutionResult": "SUCCESS"
 *   }
 * }
 * }</pre>
 *
 * @param resolvedExpressions a map keyed by the original expression and having as value the remote resolution for that expression.
 */
public record LinkedProjectExpressions(Map<String, ResolvedIndexExpression.LocalExpressions> resolvedExpressions) {
    public static LinkedProjectExpressions fromResolvedExpressions(ResolvedIndexExpressions resolvedExpressions) {
        Map<String, ResolvedIndexExpression.LocalExpressions> map = new HashMap<>();
        for (ResolvedIndexExpression e : resolvedExpressions.expressions()) {
            if (map.put(e.original(), e.localExpressions()) != null) {
                throw new IllegalStateException("duplicate key");
            }
        }
        return new LinkedProjectExpressions(map);
    }
}
