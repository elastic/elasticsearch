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
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.shard.ShardId;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;

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

    interface Replaceable extends IndicesRequest {
        /**
         * Sets the indices that the action relates to.
         */
        IndicesRequest indices(String... indices);

        default void setReplacedExpressions(@Nullable Map<String, ReplacedExpression> replacedExpressions) {
            if (false == storeReplacedExpressions()) {
                assert false : "setReplacedExpressions should not be called when storeReplacedExpressions is false";
                throw new IllegalStateException("setReplacedExpressions should not be called when storeReplacedExpressions is false");
            }
        }

        @Nullable
        default Map<String, ReplacedExpression> getReplacedExpressions() {
            return null;
        }

        default boolean storeReplacedExpressions() {
            return false;
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
    }

    interface CrossProjectReplaceable extends Replaceable {
        @Override
        default boolean allowsRemoteIndices() {
            return true;
        }

        boolean shouldApplyCrossProjectHandling();

        @Override
        default boolean storeReplacedExpressions() {
            return true;
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
