/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.authc.cloud;

import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.core.security.authc.AuthenticationToken;

import java.io.Closeable;
import java.io.IOException;

public record CloudApiKey(SecureString cloudApiKeyCredentials) implements AuthenticationToken, Closeable {
    private static final String CLOUD_API_KEY_CREDENTIAL_PREFIX = "essu_";

    @Override
    public String principal() {
        return "<unauthenticated cloud api key>";
    }

    @Override
    public Object credentials() {
        return cloudApiKeyCredentials;
    }

    @Override
    public void clearCredentials() {
        cloudApiKeyCredentials.close();
    }

    @Override
    public void close() throws IOException {
        cloudApiKeyCredentials.close();
    }

    @Nullable
    public static CloudApiKey fromApiKeyString(@Nullable SecureString apiKeyString) {
        return apiKeyString != null && hasCloudApiKeyPrefix(apiKeyString) ? new CloudApiKey(apiKeyString) : null;
    }

    private static boolean hasCloudApiKeyPrefix(SecureString apiKeyString) {
        final String rawString = apiKeyString.toString();
        return rawString.length() > CLOUD_API_KEY_CREDENTIAL_PREFIX.length()
            && rawString.regionMatches(true, 0, CLOUD_API_KEY_CREDENTIAL_PREFIX, 0, CLOUD_API_KEY_CREDENTIAL_PREFIX.length());
    }
}
