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

public record RemoteAccessHeaders(
    ApiKeyService.ApiKeyCredentials clusterCredentials,
    RemoteAccessAuthentication remoteAccessAuthentication
) {

    public static final String REMOTE_ACCESS_CLUSTER_CREDENTIAL_HEADER_KEY = "_remote_access_cluster_credential";

    public RemoteAccessHeaders(String encodedCredentials, RemoteAccessAuthentication remoteAccessAuthentication) {
        this(ApiKeyService.getCredentialsFromHeader(withApiKeyPrefix(encodedCredentials)), remoteAccessAuthentication);
    }

    public void writeToContext(final ThreadContext ctx) throws IOException {
        writeCredentialToContext(ctx);
        remoteAccessAuthentication.writeToContext(ctx);
    }

    public static RemoteAccessHeaders readFromContext(final ThreadContext ctx) throws IOException {
        return new RemoteAccessHeaders(readCredentialFromContext(ctx), RemoteAccessAuthentication.readFromContext(ctx));
    }

    private void writeCredentialToContext(final ThreadContext ctx) {
        ctx.putHeader(REMOTE_ACCESS_CLUSTER_CREDENTIAL_HEADER_KEY, withApiKeyPrefix(ApiKeyService.base64Encode(clusterCredentials)));
    }

    private static ApiKeyService.ApiKeyCredentials readCredentialFromContext(final ThreadContext ctx) {
        final String clusterCredentialHeader = ctx.getHeader(REMOTE_ACCESS_CLUSTER_CREDENTIAL_HEADER_KEY);
        if (clusterCredentialHeader == null) {
            throw new IllegalArgumentException("remote access header [" + REMOTE_ACCESS_CLUSTER_CREDENTIAL_HEADER_KEY + "] is required");
        }
        final ApiKeyService.ApiKeyCredentials apiKeyCredential = ApiKeyService.getCredentialsFromHeader(clusterCredentialHeader);
        if (apiKeyCredential == null) {
            throw new IllegalArgumentException(
                "remote access header [" + REMOTE_ACCESS_CLUSTER_CREDENTIAL_HEADER_KEY + "] value must be a valid API key credential"
            );
        }
        return apiKeyCredential;
    }

    private static String withApiKeyPrefix(final String encodedCredentials) {
        return "ApiKey " + encodedCredentials;
    }
}
