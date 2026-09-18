/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.common.oauth2;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.xpack.inference.common.InferenceIdAndProject;

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

    @Override
    public void invalidateOnlLocalNode(String inferenceId) {}
}
