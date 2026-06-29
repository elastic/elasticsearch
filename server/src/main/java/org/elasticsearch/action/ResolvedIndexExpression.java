/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.cluster.metadata.IndexAbstraction;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;

import java.io.IOException;
import java.util.Map;
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

    private static final Logger logger = LogManager.getLogger(ResolvedIndexExpression.class);

    public ResolvedIndexExpression(StreamInput in) throws IOException {
        this(in.readString(), new LocalExpressions(in), in.readCollectionAsImmutableSet(StreamInput::readString));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(original);
        localExpressions.writeTo(out);
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
        private static final TransportVersion RESOLVED_ABSTRACTION_VERSION = TransportVersion.fromName(
            "resolved_abstraction_in_local_expressions"
        );

        private final Set<String> indices;
        private final LocalIndexResolutionResult localIndexResolutionResult;
        @Nullable
        private ElasticsearchException exception;
        /**
         * Per-index abstraction details, keyed by the same strings as {@link #indices} (including any selector suffix).
         * Null when received from a node that predates {@link #RESOLVED_ABSTRACTION_VERSION}.
         */
        @Nullable
        private final Map<String, IndexAbstraction> resolutions;

        /**
         * @param indices represents the resolved concrete indices backing the expression
         * @param resolutions per-index abstraction details; must have the same key set as {@code indices} when non-null
         */
        public LocalExpressions(
            Set<String> indices,
            LocalIndexResolutionResult localIndexResolutionResult,
            @Nullable ElasticsearchException exception,
            @Nullable Map<String, IndexAbstraction> resolutions
        ) {
            assert localIndexResolutionResult != LocalIndexResolutionResult.SUCCESS || exception == null
                : "If the local resolution result is SUCCESS, exception must be null";
            this.indices = indices;
            this.localIndexResolutionResult = localIndexResolutionResult;
            this.exception = exception;
            this.resolutions = resolutions;
        }

        /**
         * Convenience constructor for callers that do not yet populate abstraction details.
         *
         * @param indices represents the resolved concrete indices backing the expression
         */
        public LocalExpressions(
            Set<String> indices,
            LocalIndexResolutionResult localIndexResolutionResult,
            @Nullable ElasticsearchException exception
        ) {
            this(indices, localIndexResolutionResult, exception, null);
        }

        public Set<String> indices() {
            return indices;
        }

        public LocalIndexResolutionResult localIndexResolutionResult() {
            return localIndexResolutionResult;
        }

        @Nullable
        public ElasticsearchException exception() {
            return exception;
        }

        /**
         * Per-index abstraction details, keyed by the same strings as {@link #indices()}.
         * Returns null when received from a node that predates {@link #RESOLVED_ABSTRACTION_VERSION}.
         */
        @Nullable
        public Map<String, IndexAbstraction> resolutions() {
            return resolutions;
        }

        public void setExceptionIfUnset(ElasticsearchException exception) {
            assert localIndexResolutionResult != LocalIndexResolutionResult.SUCCESS
                : "If the local resolution result is SUCCESS, exception must be null";
            Objects.requireNonNull(exception);

            if (this.exception == null) {
                this.exception = exception;
            } else if (Objects.equals(this.exception.getMessage(), exception.getMessage()) == false) {
                // see https://github.com/elastic/elasticsearch/issues/135799
                var message = "Exception is already set: " + exception.getMessage();
                logger.debug(message);
                assert false : message;
            }
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == this) return true;
            if (obj == null || obj.getClass() != this.getClass()) return false;
            var that = (LocalExpressions) obj;
            return Objects.equals(this.indices, that.indices)
                && Objects.equals(this.localIndexResolutionResult, that.localIndexResolutionResult)
                && Objects.equals(this.exception, that.exception)
                && Objects.equals(this.resolutions, that.resolutions);
        }

        @Override
        public int hashCode() {
            return Objects.hash(indices, localIndexResolutionResult, exception, resolutions);
        }

        @Override
        public String toString() {
            return "LocalExpressions["
                + "indices="
                + indices
                + ", "
                + "localIndexResolutionResult="
                + localIndexResolutionResult
                + ", "
                + "exception="
                + exception
                + ", "
                + "resolutions="
                + resolutions
                + ']';
        }

        // Singleton for the case where all expressions in a ResolvedIndexExpression instance are remote
        public static final LocalExpressions NONE = new LocalExpressions(Set.of(), LocalIndexResolutionResult.NONE, null);

        public LocalExpressions(StreamInput in) throws IOException {
            this(
                in.readCollectionAsImmutableSet(StreamInput::readString),
                in.readEnum(LocalIndexResolutionResult.class),
                ElasticsearchException.readException(in),
                in.getTransportVersion().supports(RESOLVED_ABSTRACTION_VERSION) && in.readBoolean()
                    ? in.readMap(IndexAbstraction::read)
                    : null
            );
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeStringCollection(indices);
            out.writeEnum(localIndexResolutionResult);
            ElasticsearchException.writeException(exception, out);
            if (out.getTransportVersion().supports(RESOLVED_ABSTRACTION_VERSION)) {
                out.writeBoolean(resolutions != null);
                if (resolutions != null) {
                    out.writeMap(resolutions, IndexAbstraction::write);
                }
            }
        }
    }
}
