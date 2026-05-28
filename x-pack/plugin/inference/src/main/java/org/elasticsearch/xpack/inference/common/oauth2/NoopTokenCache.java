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
 * Temporary {@link TokenCache} implementation that bypasses caching entirely:
 * every call to {@link #getToken} invokes the supplier; every {@link #invalidate}
 * is a no-op.
 *
 * <p>This exists so the OpenAI OAuth2 feature can ship before
 * <a href="https://github.com/elastic/elasticsearch/pull/149217">#149217</a>
 * merges. <strong>Delete this class entirely</strong> once #149217 is integrated
 * and {@code OAuth2TokenCache} (which provides real caching plus cluster-wide
 * invalidation) is wired in as the {@link TokenCache} binding.
 */
public class NoopTokenCache implements TokenCache {

    public static final NoopTokenCache INSTANCE = new NoopTokenCache();

    @Override
    public void getToken(String inferenceId, OAuth2TokenSupplier supplier, ActionListener<CachedToken> listener) {
        supplier.fetch(listener);
    }

    @Override
    public void invalidate(InferenceIdAndProject key, ActionListener<Void> listener) {
        listener.onResponse(null);
    }
}
