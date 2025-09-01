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
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.rest.RestStatus;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.action.CrossProjectReplacedIndexExpressions.hasCanonicalExpressionForOrigin;

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

        default void setReplacedIndexExpressions(ReplacedIndexExpressions replacedIndexExpressions) {}

        @Nullable
        default ReplacedIndexExpressions getReplacedIndexExpressions() {
            return null;
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
        default <T extends ResponseWithReplacedIndexExpressions> void remoteFanoutErrorHandling(Map<String, T> remoteResults) {}
    }

    interface CrossProjectSearchCapable extends Replaceable {
        Logger logger = LogManager.getLogger(CrossProjectSearchCapable.class);

        @Nullable
        String getProjectRouting();

        @Override
        default boolean allowsRemoteIndices() {
            return true;
        }

        default boolean crossProjectMode() {
            return getReplacedIndexExpressions() instanceof CrossProjectReplacedIndexExpressions;
        }

        @Override
        default <T extends ResponseWithReplacedIndexExpressions> void remoteFanoutErrorHandling(Map<String, T> remoteResults) {
            logger.info("Checking if we should throw in flat world for [{}]", getReplacedIndexExpressions());
            // No CPS nothing to do
            if (false == crossProjectMode()) {
                logger.info("Skipping because no cross-project expressions found...");
                return;
            }
            if (indicesOptions().allowNoIndices() && indicesOptions().ignoreUnavailable()) {
                // nothing to do since we're in lenient mode
                logger.info("Skipping index existence check in lenient mode");
                return;
            }

            Map<String, ReplacedIndexExpression> replacedExpressions = getReplacedIndexExpressions().asMap();
            assert replacedExpressions != null;
            logger.info("Replaced expressions to check: [{}]", replacedExpressions);
            for (ReplacedIndexExpression replacedIndexExpression : replacedExpressions.values()) {
                // TODO need to handle qualified expressions here, too
                String original = replacedIndexExpression.original();
                List<ElasticsearchException> exceptions = new ArrayList<>();
                boolean exists = hasCanonicalExpressionForOrigin(replacedIndexExpression.replacedBy())
                    && replacedIndexExpression.existsAndVisible();
                if (exists) {
                    logger.info("Local cluster has canonical expression for [{}], skipping remote existence check", original);
                    continue;
                }
                if (replacedIndexExpression.authorizationError() != null) {
                    exceptions.add(replacedIndexExpression.authorizationError());
                }

                for (var remoteResponse : remoteResults.values()) {
                    logger.info("Remote response resolved: [{}]", remoteResponse);
                    Map<String, ReplacedIndexExpression> resolved = remoteResponse.getReplacedIndexExpressions().asMap();
                    assert resolved != null;
                    var r = resolved.get(original);
                    if (r != null && r.existsAndVisible() && resolved.get(original).replacedBy().isEmpty() == false) {
                        logger.info("Remote cluster has resolved entries for [{}], skipping further remote existence check", original);
                        exists = true;
                        break;
                    } else if (r != null && r.authorizationError() != null) {
                        assert r.authorized() == false : "we should never get an error if we are authorized";
                        exceptions.add(resolved.get(original).authorizationError());
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

}
