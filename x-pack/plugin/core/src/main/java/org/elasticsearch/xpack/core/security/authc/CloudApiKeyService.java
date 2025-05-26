/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.authc;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.core.security.user.User;

import java.io.Closeable;
import java.io.IOException;

public interface CloudApiKeyService {
    record CloudApiKey(SecureString authorizationHeader) implements AuthenticationToken, Closeable {
        @Override
        public String principal() {
            return "<unauthenticated cloud api key>";
        }

        @Override
        public Object credentials() {
            return authorizationHeader;
        }

        @Override
        public void clearCredentials() {
            authorizationHeader.close();
        }

        @Override
        public void close() throws IOException {
            authorizationHeader.close();
        }
    }

    @Nullable
    CloudApiKey extractCloudApiKey(ThreadContext context);

    void authenticate(CloudApiKey cloudApiKey, ActionListener<AuthenticationResult<User>> listener);

    class Noop implements CloudApiKeyService {
        @Override
        public CloudApiKey extractCloudApiKey(ThreadContext context) {
            return null;
        }

        @Override
        public void authenticate(CloudApiKey cloudApiKey, ActionListener<AuthenticationResult<User>> listener) {
            assert false : "should never be called";
            listener.onResponse(AuthenticationResult.notHandled());
        }
    }
}
