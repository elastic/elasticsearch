/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authc;

import org.elasticsearch.common.util.concurrent.ThreadContext;
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
        return clusterCredentials.toApiKeyCredentials();
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

    private record EncodedApiKeyWithPrefix(String encodedApiKey) {

        private static final String PREFIX = "ApiKey";

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
            return new EncodedApiKeyWithPrefix(stripApiKeyPrefix(clusterCredentialHeader));
        }

        private ApiKeyService.ApiKeyCredentials toApiKeyCredentials() {
            return ApiKeyService.getCredentialsFromHeader(withApiKeyPrefix(encodedApiKey));
        }

        private static String withApiKeyPrefix(final String encodedCredentials) {
            return PREFIX + " " + encodedCredentials;
        }

        private static String stripApiKeyPrefix(final String encodedWithPrefix) {
            if (false == encodedWithPrefix.startsWith(PREFIX)) {
                throw new IllegalArgumentException("must start with [" + PREFIX + "]");
            }
            // +1 for space
            return encodedWithPrefix.substring(PREFIX.length() + 1);
        }
    }
}
