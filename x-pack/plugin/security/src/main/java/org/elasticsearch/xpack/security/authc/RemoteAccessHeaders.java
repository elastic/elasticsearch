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
    private final String clusterCredentialsHeader;
    private final RemoteAccessAuthentication remoteAccessAuthentication;

    public RemoteAccessHeaders(String clusterCredentialsHeader, RemoteAccessAuthentication remoteAccessAuthentication) {
        assert clusterCredentialsHeader.startsWith("ApiKey ") : "cluster credentials headers must start with [ApiKey ]";
        this.clusterCredentialsHeader = clusterCredentialsHeader;
        this.remoteAccessAuthentication = remoteAccessAuthentication;
    }

    public void writeToContext(final ThreadContext ctx) throws IOException {
        ctx.putHeader(REMOTE_ACCESS_CLUSTER_CREDENTIAL_HEADER_KEY, clusterCredentialsHeader);
        remoteAccessAuthentication.writeToContext(ctx);
    }

    public static RemoteAccessHeaders readFromContext(final ThreadContext ctx) throws IOException {
        final String clusterCredentialsHeader = ctx.getHeader(REMOTE_ACCESS_CLUSTER_CREDENTIAL_HEADER_KEY);
        if (clusterCredentialsHeader == null) {
            throw new IllegalArgumentException("remote access header [" + REMOTE_ACCESS_CLUSTER_CREDENTIAL_HEADER_KEY + "] is required");
        }
        // Invoke parsing logic to validate that the header decodes to a valid API key credential
        // Call `close` since the returned value is an auto-closable
        parseClusterCredentialsHeader(clusterCredentialsHeader).close();
        return new RemoteAccessHeaders(clusterCredentialsHeader, RemoteAccessAuthentication.readFromContext(ctx));
    }

    public ApiKeyService.ApiKeyCredentials clusterCredentials() {
        return parseClusterCredentialsHeader(clusterCredentialsHeader);
    }

    private static ApiKeyService.ApiKeyCredentials parseClusterCredentialsHeader(final String header) {
        try {
            return Objects.requireNonNull(ApiKeyService.getCredentialsFromHeader(header));
        } catch (Exception ex) {
            throw new IllegalArgumentException(
                "remote access header [" + REMOTE_ACCESS_CLUSTER_CREDENTIAL_HEADER_KEY + "] value must be a valid API key credential",
                ex
            );
        }
    }

    public RemoteAccessAuthentication remoteAccessAuthentication() {
        return remoteAccessAuthentication;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) return true;
        if (obj == null || obj.getClass() != this.getClass()) return false;
        var that = (RemoteAccessHeaders) obj;
        return Objects.equals(this.clusterCredentialsHeader, that.clusterCredentialsHeader)
            && Objects.equals(this.remoteAccessAuthentication, that.remoteAccessAuthentication);
    }

    @Override
    public int hashCode() {
        return Objects.hash(clusterCredentialsHeader, remoteAccessAuthentication);
    }
}
