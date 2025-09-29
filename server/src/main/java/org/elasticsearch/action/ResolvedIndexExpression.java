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

import java.util.Objects;
import java.util.Set;

/**
 * This class allows capturing context about index expression replacements performed on an {@link IndicesRequest.Replaceable} during
 * index resolution, in particular the results of local resolution, and the remote (unresolved) expressions if any.
 * <p>
 * The replacements are separated into local and remote expressions.
 * For local expressions, the class allows recording local index resolution results along with failure info.
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
 *                         and failure info
 * @param remoteExpressions the remote expressions that replace the original
 */
public record ResolvedIndexExpression(String original, LocalExpressions localExpressions, Set<String> remoteExpressions) {
    /**
     * Indicates if a local index resolution attempt was successful or failed.
     * Failures can be due to concrete resources not being visible (either missing or not visible due to indices options)
     * or unauthorized concrete resources.
     * A wildcard expression resolving to nothing is still considered a successful resolution.
     */
    public enum LocalIndexResolutionResult {
        SUCCESS,
        CONCRETE_RESOURCE_NOT_VISIBLE,
        CONCRETE_RESOURCE_UNAUTHORIZED,
    }

    /**
     * Represents local (non-remote) resolution results, including expanded indices, and a {@link LocalIndexResolutionResult}.
     */
    public static final class LocalExpressions {
        private final Set<String> expressions;
        private final LocalIndexResolutionResult localIndexResolutionResult;
        @Nullable
        private ElasticsearchException exception;

        public LocalExpressions(
            Set<String> expressions,
            LocalIndexResolutionResult localIndexResolutionResult,
            @Nullable ElasticsearchException exception
        ) {
            assert localIndexResolutionResult != LocalIndexResolutionResult.SUCCESS || exception == null
                : "If the local resolution result is SUCCESS, exception must be null";
            this.expressions = expressions;
            this.localIndexResolutionResult = localIndexResolutionResult;
            this.exception = exception;
        }

        public Set<String> expressions() {
            return expressions;
        }

        public LocalIndexResolutionResult localIndexResolutionResult() {
            return localIndexResolutionResult;
        }

        @Nullable
        public ElasticsearchException exception() {
            return exception;
        }

        public void setException(ElasticsearchException exception) {
            assert localIndexResolutionResult != LocalIndexResolutionResult.SUCCESS
                : "If the local resolution result is SUCCESS, exception must be null";
            Objects.requireNonNull(exception);

            this.exception = exception;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == this) return true;
            if (obj == null || obj.getClass() != this.getClass()) return false;
            var that = (LocalExpressions) obj;
            return Objects.equals(this.expressions, that.expressions)
                && Objects.equals(this.localIndexResolutionResult, that.localIndexResolutionResult)
                && Objects.equals(this.exception, that.exception);
        }

        @Override
        public int hashCode() {
            return Objects.hash(expressions, localIndexResolutionResult, exception);
        }

        @Override
        public String toString() {
            return "LocalExpressions["
                + "expressions="
                + expressions
                + ", "
                + "localIndexResolutionResult="
                + localIndexResolutionResult
                + ", "
                + "exception="
                + exception
                + ']';
        }
    }
}
