/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.core.Nullable;

import java.util.List;

/**
 * This class allows capturing context about index expression replacements performed on an {@link IndicesRequest.Replaceable} during
 * index resolution, in particular the results of local resolution, and the remote (unresolved) expressions if any.
 * <p>
 * The replacements are separated into local and remote expressions.
 * For local expressions, the class allows recording local index resolution results and exceptions if any.
 * For remote expressions, only the expressions are recorded.
 *
 * <p>An example structure is:</p>
 *
 * <pre>{@code
 * {
 *   "original": "my-index-*",
 *   "localExpressions": {
 *     "expressions": ["my-index-000001", "my-index-000002"],
 *     "localIndexResolutionResult": "SUCCESS"
 *   },
 *   "remoteExpressions": ["remote1:my-index-*", "remote2:my-index-*"]
 * }
 * }</pre>
 *
 * @param original the original index expression, as provided by the user
 * @param localExpressions the local expressions that replace the original along with their resolution result
 *                         and resolution exception if any
 * @param remoteExpressions the remote expressions that replace the original
 */
public record ResolvedIndexExpression(String original, LocalExpressions localExpressions, List<String> remoteExpressions) {
    enum LocalIndexResolutionResult {
        SUCCESS,
        CONCRETE_RESOURCE_MISSING,
        CONCRETE_RESOURCE_UNAUTHORIZED,
    }

    /**
     * Represents local (non-remote) resolution results, including expanded indices, the resolution result, and if any, an exception.
     * @param expressions
     * @param localIndexResolutionResult
     * @param exception
     */
    public record LocalExpressions(
        List<String> expressions,
        LocalIndexResolutionResult localIndexResolutionResult,
        @Nullable ElasticsearchException exception
    ) {}
}
