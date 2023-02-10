/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authc;

import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.core.security.authc.RemoteAccessAuthentication;

import java.io.IOException;
import java.util.Objects;

public final class RemoteAccessHeaders {

    public static final String REMOTE_ACCESS_CLUSTER_CREDENTIAL_HEADER_KEY = "_remote_access_cluster_credential";
    private final EncodedApiKeyWithPrefix clusterCredentials;
    private final RemoteAccessAuthentication remoteAccessAuthentication;

    public RemoteAccessHeaders(String encodedCredentials, RemoteAccessAuthentication remoteAccessAuthentication) {
        this(new EncodedApiKeyWithPrefix(encodedCredentials), remoteAccessAuthentication);
    }

    private RemoteAccessHeaders(EncodedApiKeyWithPrefix clusterCredentials, RemoteAccessAuthentication remoteAccessAuthentication) {
        this.clusterCredentials = clusterCredentials;
        this.remoteAccessAuthentication = remoteAccessAuthentication;
    }

    public void writeToContext(final ThreadContext ctx) throws IOException {
        clusterCredentials.writeToContext(ctx);
        remoteAccessAuthentication.writeToContext(ctx);
    }

    public static RemoteAccessHeaders readFromContext(final ThreadContext ctx) throws IOException {
        return new RemoteAccessHeaders(EncodedApiKeyWithPrefix.readFromContext(ctx), RemoteAccessAuthentication.readFromContext(ctx));
    }

    public ApiKeyService.ApiKeyCredentials clusterCredentials() {
        return clusterCredentials.getOrParseApiKeyCredentials();
    }

    public RemoteAccessAuthentication remoteAccessAuthentication() {
        return remoteAccessAuthentication;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) return true;
        if (obj == null || obj.getClass() != this.getClass()) return false;
        var that = (RemoteAccessHeaders) obj;
        return Objects.equals(this.clusterCredentials, that.clusterCredentials)
            && Objects.equals(this.remoteAccessAuthentication, that.remoteAccessAuthentication);
    }

    @Override
    public int hashCode() {
        return Objects.hash(clusterCredentials, remoteAccessAuthentication);
    }

    private static final class EncodedApiKeyWithPrefix {

        private static final String PREFIX = "ApiKey";
        private final String encodedApiKey;
        @Nullable
        private final ApiKeyService.ApiKeyCredentials apiKeyCredentials;

        private EncodedApiKeyWithPrefix(String encodedApiKey, @Nullable ApiKeyService.ApiKeyCredentials apiKeyCredentials) {
            this.apiKeyCredentials = apiKeyCredentials;
            this.encodedApiKey = encodedApiKey;
        }

        private EncodedApiKeyWithPrefix(ApiKeyService.ApiKeyCredentials apiKeyCredentials) {
            this(ApiKeyService.base64Encode(apiKeyCredentials), apiKeyCredentials);
        }

        private EncodedApiKeyWithPrefix(String encodedApiKey) {
            this(encodedApiKey, null);
        }

        private void writeToContext(final ThreadContext ctx) {
            ctx.putHeader(REMOTE_ACCESS_CLUSTER_CREDENTIAL_HEADER_KEY, withApiKeyPrefix(encodedApiKey));
        }

        private static EncodedApiKeyWithPrefix readFromContext(final ThreadContext ctx) {
            final String clusterCredentialHeader = ctx.getHeader(REMOTE_ACCESS_CLUSTER_CREDENTIAL_HEADER_KEY);
            if (clusterCredentialHeader == null) {
                throw new IllegalArgumentException(
                    "remote access header [" + REMOTE_ACCESS_CLUSTER_CREDENTIAL_HEADER_KEY + "] is required"
                );
            }
            try {
                return new EncodedApiKeyWithPrefix(parseApiKeyCredentials(clusterCredentialHeader));
            } catch (Exception ex) {
                throw new IllegalArgumentException(
                    "remote access header [" + REMOTE_ACCESS_CLUSTER_CREDENTIAL_HEADER_KEY + "] value must be a valid API key credential",
                    ex
                );
            }
        }

        private static ApiKeyService.ApiKeyCredentials parseApiKeyCredentials(String clusterCredentialHeader) {
            return Objects.requireNonNull(ApiKeyService.getCredentialsFromHeader(clusterCredentialHeader));
        }

        private ApiKeyService.ApiKeyCredentials getOrParseApiKeyCredentials() {
            return apiKeyCredentials != null ? apiKeyCredentials : parseApiKeyCredentials(withApiKeyPrefix(encodedApiKey));
        }

        private static String withApiKeyPrefix(final String encodedCredentials) {
            return PREFIX + " " + encodedCredentials;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            EncodedApiKeyWithPrefix that = (EncodedApiKeyWithPrefix) o;

            if (false == encodedApiKey.equals(that.encodedApiKey)) return false;
            return Objects.equals(apiKeyCredentials, that.apiKeyCredentials);
        }

        @Override
        public int hashCode() {
            int result = encodedApiKey.hashCode();
            result = 31 * result + (apiKeyCredentials != null ? apiKeyCredentials.hashCode() : 0);
            return result;
        }
    }
}
