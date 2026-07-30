/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.common.secrets;

import org.apache.http.client.methods.HttpRequestBase;
import org.elasticsearch.action.ActionListener;

/**
 * Applies authentication credentials to an outbound inference HTTP request.
 * Implementations may add headers synchronously (e.g. static API key) or asynchronously
 * (e.g. OAuth2 client-credentials token fetch).
 */
public interface SecretsApplier {
    void applyTo(HttpRequestBase request, ActionListener<HttpRequestBase> listener);

    /**
     * Called when the remote service responds with a 401. Implementations that cache
     * authentication credentials (e.g. OAuth2 bearer tokens) should use this hook to
     * drop the cached credential so the next request fetches a fresh one.
     *
     * <p>The default implementation is a no-op.
     */
    default void onAuthenticationFailure() {
        // Noop
    }
}
