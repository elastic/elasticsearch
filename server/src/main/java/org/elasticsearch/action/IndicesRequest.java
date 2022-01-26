/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action;

import org.elasticsearch.action.support.IndicesOptions;

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

        /**
         * Determines whether the request can contain indices on a remote cluster.
         *
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
}
