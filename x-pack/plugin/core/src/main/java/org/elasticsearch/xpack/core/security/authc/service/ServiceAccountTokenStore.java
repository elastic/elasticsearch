/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.authc.service;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.xpack.core.security.action.service.TokenInfo.TokenSource;

/**
 * The interface should be implemented by credential stores of different backends.
 */
public interface ServiceAccountTokenStore {

    /**
     * Verify the given token for encapsulated service account and credential
     */
    void authenticate(ServiceAccountToken token, ActionListener<StoreAuthenticationResult> listener);

    class StoreAuthenticationResult {
        private final boolean success;
        private final TokenSource tokenSource;

        private StoreAuthenticationResult(TokenSource tokenSource, boolean success) {
            this.success = success;
            this.tokenSource = tokenSource;
        }

        public static StoreAuthenticationResult successful(TokenSource tokenSource) {
            return new StoreAuthenticationResult(tokenSource, true);
        }

        public static StoreAuthenticationResult failed(TokenSource tokenSource) {
            return new StoreAuthenticationResult(tokenSource, false);
        }

        public static StoreAuthenticationResult fromBooleanResult(TokenSource tokenSource, boolean result) {
            return result ? successful(tokenSource) : failed(tokenSource);
        }

        public boolean isSuccess() {
            return success;
        }

        public TokenSource getTokenSource() {
            return tokenSource;
        }
    }
}
