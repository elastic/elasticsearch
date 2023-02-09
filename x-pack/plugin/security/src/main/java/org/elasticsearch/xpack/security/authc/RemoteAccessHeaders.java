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

    public RemoteAccessHeaders(RemoteAccessCredentials credentials, RemoteAccessAuthentication remoteAccessAuthentication) {
        this(credentials.credential, remoteAccessAuthentication);
    }

    public void writeToContext(final ThreadContext ctx) {
        ctx.putHeader(REMOTE_ACCESS_CLUSTER_CREDENTIAL_HEADER_KEY, withApiKeyPrefix(clusterCredential));
    }

    public static RemoteAccessHeaders readFromContext(final ThreadContext ctx) throws IOException {
        final String clusterCredentialHeader = ctx.getHeader(REMOTE_ACCESS_CLUSTER_CREDENTIAL_HEADER_KEY);
        if (clusterCredentialHeader == null) {
            throw new IllegalArgumentException("remote access header [" + REMOTE_ACCESS_CLUSTER_CREDENTIAL_HEADER_KEY + "] is required");
        }
        if (ctx.getHeader(REMOTE_ACCESS_AUTHENTICATION_HEADER_KEY) == null) {
            throw new IllegalArgumentException("remote access header [" + REMOTE_ACCESS_AUTHENTICATION_HEADER_KEY + "] is required");
        }
        return new RemoteAccessHeaders(clusterCredentialHeader, RemoteAccessAuthentication.readFromContext(ctx));
    }

    public ApiKeyService.ApiKeyCredentials getApiKeyCredentials() {
        final ApiKeyService.ApiKeyCredentials apiKeyCredential = ApiKeyService.getCredentialsFromHeader(clusterCredential);
        if (apiKeyCredential == null) {
            throw new IllegalArgumentException(
                "remote access header [" + REMOTE_ACCESS_CLUSTER_CREDENTIAL_HEADER_KEY + "] value must be a valid API key credential"
            );
        }
        return apiKeyCredential;
    }

    public record RemoteAccessCredentials(String clusterAlias, String credential) {}

    private String withApiKeyPrefix(final String clusterCredential) {
        return "ApiKey " + clusterCredential;
    }
}
