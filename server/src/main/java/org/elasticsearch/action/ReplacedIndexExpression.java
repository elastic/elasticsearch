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
 * A record that holds the result of an index expression replacement, e.g. local index resolution.
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
 * @param localExpressions the resolved local part of the expression
 * @param remoteExpressions the resolved remote part of the expression, each entry in "canonical" CCS form,
 *        e.g. "remote:index". This is a list instead of a record since remote expressions are not expanded,
 *        and we therefore don't need to track expansion results or errors.
 */
public record ReplacedIndexExpression(String original, LocalExpressions localExpressions, List<String> remoteExpressions) {
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
