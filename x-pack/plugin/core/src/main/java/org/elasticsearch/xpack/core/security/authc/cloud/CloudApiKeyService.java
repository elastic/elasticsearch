/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.authc.cloud;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.core.security.authc.Authentication;

public interface CloudApiKeyService {
    @Nullable
    CloudApiKey parseAsCloudApiKey(@Nullable SecureString apiKeyString);

    void authenticate(CloudApiKey cloudApiKey, String nodeName, ActionListener<Authentication> listener);

    class Noop implements CloudApiKeyService {
        @Override
        public CloudApiKey parseAsCloudApiKey(@Nullable SecureString apiKeyString) {
            return null;
        }

        @Override
        public void authenticate(CloudApiKey cloudApiKey, String nodeName, ActionListener<Authentication> listener) {
            assert false : "cloud API key authentication noop implementation should never be called";
            listener.onFailure(new IllegalStateException("cloud API key authentication is disabled"));
        }
    }
}
