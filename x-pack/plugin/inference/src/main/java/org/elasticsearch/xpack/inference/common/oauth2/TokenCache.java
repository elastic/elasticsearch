/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.common.oauth2;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.xpack.inference.common.InferenceIdAndProject;

/**
 * Surface for an OAuth2 bearer-token cache shared across the inference plugin.
 */
public interface TokenCache {

    /**
     * Returns a valid bearer token for the given inference endpoint, fetching one via
     * {@code supplier} if none is cached or the cached token is about to expire.
     */
    void getToken(String inferenceId, OAuth2TokenSupplier supplier, ActionListener<CachedToken> listener);

    /**
     * Removes any cached entry for the given key. With a real cache implementation,
     * this should broadcast to all nodes.
     */
    void invalidate(InferenceIdAndProject key, ActionListener<Void> listener);
}
