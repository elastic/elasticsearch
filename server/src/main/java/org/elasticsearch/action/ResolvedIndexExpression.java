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
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;

import java.io.IOException;
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
 *     "indices": ["my-index-000001", "my-index-000002"],
 *     "localIndexResolutionResult": "SUCCESS"
 *   },
 *   "remoteExpressions": ["remote1:my-index-*", "remote2:my-index-*"]
 * }
 * }</pre>
 *
 * @param original the original index expression, as provided by the user
 * @param localExpressions the local expressions that replace the original along with their resolution result
 *                         and failure info
 * @param remoteExpressions the remote expressions that replace the original one (in the case of CPS/flat index resolution).
 *                          Only set on the local ResolvedIndexExpression, empty otherwise.
 */
public record ResolvedIndexExpression(String original, LocalExpressions localExpressions, Set<String> remoteExpressions)
    implements
        Writeable {

    public ResolvedIndexExpression(StreamInput in) throws IOException {
        this(in.readString(), new LocalExpressions(in), in.readCollectionAsImmutableSet(StreamInput::readString));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(original);
        localExpressions.writeTo(out);
        out.writeStringCollection(remoteExpressions);
    }

    void writeToLegacy(StreamOutput out, ElasticsearchException legacyException) throws IOException {
        out.writeString(original);
        localExpressions.writeToLegacy(out, legacyException);
        out.writeStringCollection(remoteExpressions);
    }

    /**
     * Indicates if a local index resolution attempt was successful or failed.
     * Failures can be due to concrete resources not being visible (either missing or not visible due to indices options)
     * or unauthorized concrete resources.
     * A wildcard expression resolving to nothing is still considered a successful resolution.
     * The NONE result indicates that no local resolution was attempted because the expression is known to be remote-only.
     *
     * This distinction is needed to return either 403 (forbidden) or 404 (not found) to the user,
     * and must be propagated by the linked projects to the request coordinator.
     *
     * CONCRETE_RESOURCE_NOT_VISIBLE: Indicates that a non-wildcard expression was resolved to nothing,
     * either because the index does not exist or is closed.
     *
     * CONCRETE_RESOURCE_UNAUTHORIZED: Indicates that the expression could be resolved to a concrete index,
     * but the requesting user is not authorized to access it.
     *
     * NONE: No local resolution was attempted, typically because the expression is remote-only.
     *
     * SUCCESS: Local index resolution was successful.
     */
    public enum LocalIndexResolutionResult {
        NONE,
        SUCCESS,
        CONCRETE_RESOURCE_NOT_VISIBLE,
        CONCRETE_RESOURCE_UNAUTHORIZED,
    }

    /**
     * Represents local (non-remote) resolution results, including expanded indices, and a {@link LocalIndexResolutionResult}.
     */
    public static final class LocalExpressions implements Writeable {
        private final Set<String> indices;
        private final LocalIndexResolutionResult localIndexResolutionResult;
        // for BwC with transport versions before RESOLVED_INDEX_EXPRESSIONS_AUTH_TEMPLATE
        private transient ElasticsearchException legacyException;

        /**
         * @param indices represents the resolved concrete indices backing the expression
         */
        public LocalExpressions(Set<String> indices, LocalIndexResolutionResult localIndexResolutionResult) {
            this.indices = indices;
            this.localIndexResolutionResult = localIndexResolutionResult;
        }

        public Set<String> indices() {
            return indices;
        }

        public LocalIndexResolutionResult localIndexResolutionResult() {
            return localIndexResolutionResult;
        }

        ElasticsearchException legacyException() {
            return legacyException;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == this) return true;
            if (obj == null || obj.getClass() != this.getClass()) return false;
            var that = (LocalExpressions) obj;
            return Objects.equals(this.indices, that.indices)
                && Objects.equals(this.localIndexResolutionResult, that.localIndexResolutionResult);
        }

        @Override
        public int hashCode() {
            return Objects.hash(indices, localIndexResolutionResult);
        }

        @Override
        public String toString() {
            return "LocalExpressions[" + "indices=" + indices + ", " + "localIndexResolutionResult=" + localIndexResolutionResult + ']';
        }

        // Singleton for the case where all expressions in a ResolvedIndexExpression instance are remote
        public static final LocalExpressions NONE = new LocalExpressions(Set.of(), LocalIndexResolutionResult.NONE);

        public LocalExpressions(StreamInput in) throws IOException {
            this.indices = in.readCollectionAsImmutableSet(StreamInput::readString);
            this.localIndexResolutionResult = in.readEnum(LocalIndexResolutionResult.class);
            if (in.getTransportVersion().supports(ResolvedIndexExpressions.RESOLVED_INDEX_EXPRESSIONS_AUTH_TEMPLATE) == false) {
                this.legacyException = ElasticsearchException.readException(in);
            }
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeStringCollection(indices);
            out.writeEnum(localIndexResolutionResult);
            if (out.getTransportVersion().supports(ResolvedIndexExpressions.RESOLVED_INDEX_EXPRESSIONS_AUTH_TEMPLATE) == false) {
                ElasticsearchException.writeException(legacyException, out);
            }
        }

        private void writeToLegacy(StreamOutput out, ElasticsearchException legacyException) throws IOException {
            out.writeStringCollection(indices);
            out.writeEnum(localIndexResolutionResult);
            ElasticsearchException.writeException(legacyException, out);
        }
    }
}
