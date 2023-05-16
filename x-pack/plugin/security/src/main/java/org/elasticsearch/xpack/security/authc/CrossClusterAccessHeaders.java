/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authc;

import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.xpack.core.security.action.apikey.ApiKey;
import org.elasticsearch.xpack.core.security.authc.CrossClusterAccessSubjectInfo;

import java.io.IOException;
import java.util.Objects;

public final class CrossClusterAccessHeaders {

    public static final String CROSS_CLUSTER_ACCESS_CREDENTIALS_HEADER_KEY = "_cross_cluster_access_credentials";
    private final String credentialsHeader;
    private final CrossClusterAccessSubjectInfo crossClusterAccessSubjectInfo;

    public CrossClusterAccessHeaders(String credentialsHeader, CrossClusterAccessSubjectInfo crossClusterAccessSubjectInfo) {
        assert credentialsHeader.startsWith("ApiKey ") : "credentials header must start with [ApiKey ]";
        this.credentialsHeader = credentialsHeader;
        this.crossClusterAccessSubjectInfo = crossClusterAccessSubjectInfo;
    }

    public void writeToContext(final ThreadContext ctx) throws IOException {
        ctx.putHeader(CROSS_CLUSTER_ACCESS_CREDENTIALS_HEADER_KEY, credentialsHeader);
        crossClusterAccessSubjectInfo.writeToContext(ctx);
    }

    public static CrossClusterAccessHeaders readFromContext(final ThreadContext ctx) throws IOException {
        final String credentialsHeader = ctx.getHeader(CROSS_CLUSTER_ACCESS_CREDENTIALS_HEADER_KEY);
        if (credentialsHeader == null) {
            throw new IllegalArgumentException(
                "cross cluster access header [" + CROSS_CLUSTER_ACCESS_CREDENTIALS_HEADER_KEY + "] is required"
            );
        }
        // Invoke parsing logic to validate that the header decodes to a valid API key credential
        // Call `close` since the returned value is an auto-closable
        parseCredentialsHeader(credentialsHeader).close();
        return new CrossClusterAccessHeaders(credentialsHeader, CrossClusterAccessSubjectInfo.readFromContext(ctx));
    }

    public ApiKeyService.ApiKeyCredentials credentials() {
        return parseCredentialsHeader(credentialsHeader);
    }

    private static ApiKeyService.ApiKeyCredentials parseCredentialsHeader(final String header) {
        try {
            return Objects.requireNonNull(ApiKeyService.getCredentialsFromHeader(header, ApiKey.Type.CROSS_CLUSTER));
        } catch (Exception ex) {
            throw new IllegalArgumentException(
                "cross cluster access header ["
                    + CROSS_CLUSTER_ACCESS_CREDENTIALS_HEADER_KEY
                    + "] value must be a valid API key credential",
                ex
            );
        }
    }

    public CrossClusterAccessSubjectInfo getCleanAndValidatedSubjectInfo() {
        return crossClusterAccessSubjectInfo.cleanAndValidate();
    }

    // package-private for testing
    CrossClusterAccessSubjectInfo getSubjectInfo() {
        return crossClusterAccessSubjectInfo;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) return true;
        if (obj == null || obj.getClass() != this.getClass()) return false;
        var that = (CrossClusterAccessHeaders) obj;
        return Objects.equals(this.credentialsHeader, that.credentialsHeader)
            && Objects.equals(this.crossClusterAccessSubjectInfo, that.crossClusterAccessSubjectInfo);
    }

    @Override
    public int hashCode() {
        return Objects.hash(credentialsHeader, crossClusterAccessSubjectInfo);
    }
}
