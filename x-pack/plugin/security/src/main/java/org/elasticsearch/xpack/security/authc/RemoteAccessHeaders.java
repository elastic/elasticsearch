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

import static org.elasticsearch.xpack.core.security.authc.RemoteAccessAuthentication.REMOTE_ACCESS_AUTHENTICATION_HEADER_KEY;

public record RemoteAccessHeaders(String clusterCredential, RemoteAccessAuthentication remoteAccessAuthentication) {

    public static final String REMOTE_ACCESS_CLUSTER_CREDENTIAL_HEADER_KEY = "_remote_access_cluster_credential";
    private static final String API_KEY_PREFIX = "ApiKey ";

    public void writeToContext(final ThreadContext ctx) throws IOException {
        ctx.putHeader(REMOTE_ACCESS_CLUSTER_CREDENTIAL_HEADER_KEY, withPrefix(clusterCredential));
        remoteAccessAuthentication.writeToContext(ctx);
    }

    public static RemoteAccessHeaders readFromContext(final ThreadContext ctx) throws IOException {
        final String clusterCredentialHeader = ctx.getHeader(REMOTE_ACCESS_CLUSTER_CREDENTIAL_HEADER_KEY);
        if (clusterCredentialHeader == null) {
            throw new IllegalArgumentException("remote access header [" + REMOTE_ACCESS_CLUSTER_CREDENTIAL_HEADER_KEY + "] is required");
        }
        if (ctx.getHeader(REMOTE_ACCESS_AUTHENTICATION_HEADER_KEY) == null) {
            throw new IllegalArgumentException("remote access header [" + REMOTE_ACCESS_AUTHENTICATION_HEADER_KEY + "] is required");
        }
        return new RemoteAccessHeaders(stripPrefix(clusterCredentialHeader), RemoteAccessAuthentication.readFromContext(ctx));
    }

    public ApiKeyService.ApiKeyCredentials getApiKeyCredentials() {
        final ApiKeyService.ApiKeyCredentials apiKeyCredential = ApiKeyService.getCredentialsFromHeader(withPrefix(clusterCredential));
        if (apiKeyCredential == null) {
            throw new IllegalArgumentException(
                "remote access header [" + REMOTE_ACCESS_CLUSTER_CREDENTIAL_HEADER_KEY + "] value must be a valid API key credential"
            );
        }
        return apiKeyCredential;
    }

    private static String withPrefix(final String clusterCredential) {
        return API_KEY_PREFIX + clusterCredential;
    }

    private static String stripPrefix(final String clusterCredential) {
        if (false == clusterCredential.startsWith(API_KEY_PREFIX)) {
            throw new IllegalArgumentException("credential must start with [" + API_KEY_PREFIX + "]");
        }
        return clusterCredential.substring(API_KEY_PREFIX.length());
    }
}
