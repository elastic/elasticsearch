/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action;

import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.shard.ShardId;

import java.util.Collection;

/**
 * Interface implemented by all {@link org.elasticsearch.action.ActionRequest} subclasses that relate to
 * one or more indices. Allows retrieval of which indices the action operates on.
 *
 * <p>In case of internal requests originated during the distributed execution of an external request,
 * they will still return the indices that the original request related to, maintaining the context
 * of the original operation.
 *
 * <p><b>Usage Examples:</b></p>
 * <pre>{@code
 * // Implement IndicesRequest in a custom action request
 * public class MyIndexRequest extends ActionRequest implements IndicesRequest {
 *     private String[] indices;
 *     private IndicesOptions options;
 *
 *     @Override
 *     public String[] indices() {
 *         return indices;
 *     }
 *
 *     @Override
 *     public IndicesOptions indicesOptions() {
 *         return options;
 *     }
 * }
 *
 * // Use IndicesRequest methods
 * IndicesRequest request = new MyIndexRequest();
 * String[] targetIndices = request.indices();
 * IndicesOptions options = request.indicesOptions();
 * }</pre>
 */
public interface IndicesRequest {

    /**
     * Returns the array of indices that this action relates to.
     *
     * <p>This may include concrete index names, index patterns with wildcards (e.g., {@code logs-*}),
     * or aliases. The actual indices targeted will be determined based on these names combined with
     * the {@link #indicesOptions()} settings.
     *
     * @return the array of index names, patterns, or aliases that this action operates on
     */
    String[] indices();

    /**
     * Returns the indices options used to resolve indices. These options control various aspects
     * of index resolution including:
     * <ul>
     *   <li>Whether a single index is required or multiple indices are accepted</li>
     *   <li>Whether an empty array will be converted to all indices (_all)</li>
     *   <li>How wildcards will be expanded (e.g., to open, closed, or hidden indices)</li>
     *   <li>How to handle missing indices (ignore or throw exception)</li>
     * </ul>
     *
     * @return the indices options for resolving and validating index names
     */
    IndicesOptions indicesOptions();

    /**
     * Determines whether the request should be applied to data streams. When {@code false}, none of
     * the names or wildcard expressions in {@link #indices()} should be applied to or expanded to
     * any data streams.
     *
     * <p>All layers involved in the request's fulfillment including security, name resolution, etc.,
     * should respect this flag to ensure consistent behavior across the system.
     *
     * @return {@code true} if data streams should be included in index resolution, {@code false} otherwise
     */
    default boolean includeDataStreams() {
        return false;
    }

    /**
     * Extension of {@link IndicesRequest} for requests that support replacing their target indices
     * after the request has been constructed. This is used during index resolution to update the
     * request with the concrete indices after wildcard expansion and alias resolution.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Implement Replaceable in a request
     * public class MyRequest extends ActionRequest implements IndicesRequest.Replaceable {
     *     private String[] indices;
     *
     *     @Override
     *     public IndicesRequest indices(String... indices) {
     *         this.indices = indices;
     *         return this;
     *     }
     *
     *     @Override
     *     public String[] indices() {
     *         return indices;
     *     }
     * }
     *
     * // Replace indices during resolution
     * IndicesRequest.Replaceable request = new MyRequest();
     * request.indices("index-*");  // Original pattern
     * // After resolution:
     * request.indices("index-1", "index-2", "index-3");  // Concrete indices
     * }</pre>
     */
    interface Replaceable extends IndicesRequest {
        /**
         * Sets the indices that this action relates to. This method is typically called during
         * index resolution to replace wildcard patterns or aliases with concrete index names.
         *
         * @param indices the array of index names to set
         * @return this request for method chaining
         */
        IndicesRequest indices(String... indices);

        /**
         * Records the results of index resolution for later inspection or auditing purposes.
         * See {@link ResolvedIndexExpressions} for details on what information is recorded.
         *
         * <p><b>Note:</b> This method does not replace {@link #indices(String...)}. The
         * {@link #indices(String...)} method must still be called to update the actual list
         * of indices the request relates to. This method only stores metadata about how the
         * indices were resolved.
         *
         * <p><b>Note:</b> The recorded information is transient and not serialized.
         *
         * @param expressions the resolved index expressions to record
         */
        default void setResolvedIndexExpressions(ResolvedIndexExpressions expressions) {}

        /**
         * Returns the results of index resolution, if previously recorded via
         * {@link #setResolvedIndexExpressions(ResolvedIndexExpressions)}.
         *
         * @return the resolved index expressions, or {@code null} if not recorded
         */
        @Nullable
        default ResolvedIndexExpressions getResolvedIndexExpressions() {
            return null;
        }

        /**
         * Determines whether the request can contain indices on a remote cluster.
         *
         * <p><b>Note:</b> In theory this method can belong to the {@link IndicesRequest} interface
         * because whether a request allowing remote indices has no inherent relationship to whether
         * it is {@link Replaceable} or not. However, we don't have an existing request that is
         * non-replaceable but allows remote indices. In addition, authorization code currently relies
         * on the fact that non-replaceable requests do not allow remote indices. That said, it is
         * possible to remove this constraint should the needs arise in the future. We just need to
         * proceed with extra caution.
         *
         * @return {@code true} if this request type allows targeting indices on remote clusters,
         *         {@code false} otherwise
         */
        default boolean allowsRemoteIndices() {
            return false;
        }

        /**
         * Determines whether the request type allows cross-project processing. Cross-project
         * processing entails cross-project search, index resolution, and error handling.
         *
         * <p><b>Note:</b> This method only determines if the request <i>supports</i> cross-project
         * processing. Whether cross-project processing is actually performed is determined by
         * {@link IndicesOptions}.
         *
         * @return {@code true} if this request type supports cross-project processing,
         *         {@code false} otherwise
         */
        default boolean allowsCrossProject() {
            return false;
        }

        /**
         * Returns the project routing hint for this request, if any. Project routing is used to
         * direct requests to specific projects in multi-project deployments.
         *
         * @return the project routing string, or {@code null} if no routing is specified
         */
        @Nullable // if no routing is specified
        default String getProjectRouting() {
            return null;
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
}
