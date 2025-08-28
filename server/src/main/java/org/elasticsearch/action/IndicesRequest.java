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
import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.action.IndicesRequest.ReplacedExpression.hasCanonicalExpressionForOrigin;

/**
 * Needs to be implemented by all {@link org.elasticsearch.action.ActionRequest} subclasses that relate to
 * one or more indices. Allows to retrieve which indices the action relates to.
 * In case of internal requests originated during the distributed execution of an external request,
 * they will still return the indices that the original request related to.
 */
public interface IndicesRequest {

    /**
     * Returns the array of indices that the action relates to
     */
    String[] indices();

    /**
     * Returns the indices options used to resolve indices. They tell for instance whether a single index is
     * accepted, whether an empty array will be converted to _all, and how wildcards will be expanded if needed.
     */
    IndicesOptions indicesOptions();

    /**
     * Determines whether the request should be applied to data streams. When {@code false}, none of the names or
     * wildcard expressions in {@link #indices} should be applied to or expanded to any data streams. All layers
     * involved in the request's fulfillment including security, name resolution, etc., should respect this flag.
     */
    default boolean includeDataStreams() {
        return false;
    }

    interface ReplaceableIndices {
        String[] indices();

        default List<String> indicesAsList() {
            return List.of(indices());
        }

        @Nullable
        default Map<String, ReplacedExpression> replacedExpressionMap() {
            return null;
        }
    }

    record DummyReplaceableIndices(String[] indices) implements ReplaceableIndices {}

    record CompleteReplaceableIndices(Map<String, ReplacedExpression> replacedExpressionMap) implements ReplaceableIndices {
        @Override
        public String[] indices() {
            return ReplacedExpression.toIndices(replacedExpressionMap);
        }

        @Override
        public Map<String, ReplacedExpression> replacedExpressionMap() {
            return replacedExpressionMap;
        }
    }

    record CrossProjectReplaceableIndices(Map<String, ReplacedExpression> replacedExpressionMap) implements ReplaceableIndices {
        @Override
        public String[] indices() {
            return ReplacedExpression.toIndices(replacedExpressionMap);
        }

        @Override
        public Map<String, ReplacedExpression> replacedExpressionMap() {
            return replacedExpressionMap;
        }
    }

    interface Replaceable extends IndicesRequest {
        /**
         * Sets the indices that the action relates to.
         */
        IndicesRequest indices(String... indices);

        default IndicesRequest replaceableIndices(ReplaceableIndices replaceableIndices) {
            return this;
        }

        default ReplaceableIndices getReplaceableIndices() {
            return new DummyReplaceableIndices(indices());
        }

        default boolean hasCrossProjectExpressions() {
            return getReplaceableIndices() instanceof CrossProjectReplaceableIndices;
        }

        /**
         * Determines whether the request can contain indices on a remote cluster.
         * NOTE in theory this method can belong to the {@link IndicesRequest} interface because whether a request
         * allowing remote indices has no inherent relationship to whether it is {@link Replaceable} or not.
         * However, we don't have an existing request that is non-replaceable but allows remote indices.
         * In addition, authorization code currently relies on the fact that non-replaceable requests do not allow
         * remote indices.
         * That said, it is possible to remove this constraint should the needs arise in the future. We just need
         * proceed with extra caution.
         */
        default boolean allowsRemoteIndices() {
            return false;
        }

        // TODO probably makes more sense on a service class as opposed to the request itself
        default void remoteErrorHandling(Map<String, ReplaceableIndices> remoteResults) {}
    }

    interface CrossProjectReplaceable extends Replaceable {
        Logger logger = LogManager.getLogger(CrossProjectReplaceable.class);

        @Override
        default boolean allowsRemoteIndices() {
            return true;
        }

        @Override
        default void remoteErrorHandling(Map<String, ReplaceableIndices> remoteResults) {
            logger.info("Checking if we should throw in flat world for [{}]", getReplaceableIndices());
            // No CPS nothing to do
            if (false == hasCrossProjectExpressions()) {
                logger.info("Skipping because no cross-project expressions found...");
                return;
            }
            if (indicesOptions().allowNoIndices() && indicesOptions().ignoreUnavailable()) {
                // nothing to do since we're in lenient mode
                logger.info("Skipping index existence check in lenient mode");
                return;
            }

            Map<String, IndicesRequest.ReplacedExpression> replacedExpressions = getReplaceableIndices().replacedExpressionMap();
            assert replacedExpressions != null;
            logger.info("Replaced expressions to check: [{}]", replacedExpressions);
            for (IndicesRequest.ReplacedExpression replacedExpression : replacedExpressions.values()) {
                // TODO need to handle qualified expressions here, too
                String original = replacedExpression.original();
                List<ElasticsearchException> exceptions = new ArrayList<>();
                boolean exists = hasCanonicalExpressionForOrigin(replacedExpression.replacedBy());
                if (exists) {
                    logger.info("Local cluster has canonical expression for [{}], skipping remote existence check", original);
                    continue;
                }
                if (replacedExpression.error() != null) {
                    exceptions.add(replacedExpression.error());
                }

                for (var remoteResponse : remoteResults.values()) {
                    logger.info("Remote response resolved: [{}]", remoteResponse);
                    Map<String, IndicesRequest.ReplacedExpression> resolved = remoteResponse.replacedExpressionMap();
                    assert resolved != null;
                    if (resolved.containsKey(original) && resolved.get(original).replacedBy().isEmpty() == false) {
                        logger.info("Remote cluster has resolved entries for [{}], skipping further remote existence check", original);
                        exists = true;
                        break;
                    } else if (resolved.containsKey(original) && resolved.get(original).error() != null) {
                        exceptions.add(resolved.get(original).error());
                    }
                }

                if (false == exists && false == indicesOptions().ignoreUnavailable()) {
                    if (false == exceptions.isEmpty()) {
                        // we only ever get exceptions if they are security related
                        // back and forth on whether a mix or security and non-security (missing indices) exceptions should report
                        // as 403 or 404
                        ElasticsearchSecurityException e = new ElasticsearchSecurityException(
                            "authorization errors while resolving [" + original + "]",
                            RestStatus.FORBIDDEN
                        );
                        exceptions.forEach(e::addSuppressed);
                        throw e;
                    } else {
                        // TODO composite exception based on missing resources
                        throw new IndexNotFoundException(original);
                    }
                }
            }
        }
    }

    /**
     * For use cases where a Request instance cannot implement Replaceable due to not supporting wildcards
     * and only supporting a single index at a time, this is an alternative interface that the
     * security layer checks against to determine if remote indices are allowed for that Request type.
     *
     * This may change with https://github.com/elastic/elasticsearch/issues/105598
     */
    interface SingleIndexNoWildcards extends IndicesRequest {
        default boolean allowsRemoteIndices() {
            return true;
        }
    }

    /**
     * This subtype of request is for requests which may travel to remote clusters. These requests may need to provide additional
     * information to the system on top of the indices the action relates to in order to be handled correctly in all cases.
     */
    interface RemoteClusterShardRequest extends IndicesRequest {
        /**
         * Returns the shards this action is targeting directly, which may not obviously align with the indices returned by
         * {@code indices()}. This is mostly used by requests which fan out to a number of shards for the those fan-out requests.
         *
         * A default is intentionally not provided for this method. It is critical that this method be implemented correctly for all
         * remote cluster requests,
         */
        Collection<ShardId> shards();
    }

    final class ReplacedExpression implements Writeable {
        private final String original;
        private final List<String> replacedBy;
        private final boolean unauthorized;
        @Nullable
        private ElasticsearchException error;

        public ReplacedExpression(StreamInput in) throws IOException {
            this.original = in.readString();
            this.replacedBy = in.readCollectionAsList(StreamInput::readString);
            this.unauthorized = false;
            this.error = ElasticsearchException.readException(in);
        }

        public ReplacedExpression(
            String original,
            List<String> replacedBy,
            boolean unauthorized,
            @Nullable ElasticsearchException exception
        ) {
            this.original = original;
            this.replacedBy = replacedBy;
            this.unauthorized = unauthorized;
            this.error = exception;
        }

        public static String[] toIndices(Map<String, ReplacedExpression> replacedExpressions) {
            return replacedExpressions.values()
                .stream()
                .flatMap(indexExpression -> indexExpression.replacedBy().stream())
                .toArray(String[]::new);
        }

        public ReplacedExpression(String original, List<String> replacedBy) {
            this(original, replacedBy, false, null);
        }

        public void setError(ElasticsearchException error) {
            this.error = error;
        }

        // TODO does not belong here
        public static boolean hasCanonicalExpressionForOrigin(List<String> replacedBy) {
            return replacedBy.stream().anyMatch(e -> false == CrossProjectUtils.isQualifiedIndexExpression(e));
        }

        public String original() {
            return original;
        }

        public List<String> replacedBy() {
            return replacedBy;
        }

        public boolean unauthorized() {
            return unauthorized;
        }

        public ElasticsearchException error() {
            return error;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == this) return true;
            if (obj == null || obj.getClass() != this.getClass()) return false;
            var that = (ReplacedExpression) obj;
            return Objects.equals(this.original, that.original)
                && Objects.equals(this.replacedBy, that.replacedBy)
                && Objects.equals(this.unauthorized, that.unauthorized)
                && Objects.equals(this.error, that.error);
        }

        @Override
        public int hashCode() {
            return Objects.hash(original, replacedBy, unauthorized, error);
        }

        @Override
        public String toString() {
            return "ReplacedExpression["
                + "original="
                + original
                + ", "
                + "replacedBy="
                + replacedBy
                + ", "
                + "unauthorized="
                + unauthorized
                + ", "
                + "errors="
                + error
                + ']';
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(original);
            out.writeStringCollection(replacedBy);
            ElasticsearchException.writeException(error, out);
        }
    }

}
