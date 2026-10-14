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

import static org.elasticsearch.xpack.core.security.authc.CrossClusterAccessSubjectInfo.CROSS_CLUSTER_ACCESS_SUBJECT_INFO_HEADER_KEY;
import static org.elasticsearch.xpack.security.authc.CrossClusterAccessHeaders.CROSS_CLUSTER_ACCESS_CREDENTIALS_HEADER_KEY;

/**
 * Incoming cross-cluster headers with subject info kept encoded so credentials can be verified before decoding it.
 */
final class CrossClusterAccessRequestHeaders {

    private final String credentialsHeader;
    private final String encodedSubjectInfoHeader;

    private CrossClusterAccessRequestHeaders(String credentialsHeader, String encodedSubjectInfoHeader) {
        this.credentialsHeader = credentialsHeader;
        this.encodedSubjectInfoHeader = encodedSubjectInfoHeader;
    }

    static CrossClusterAccessRequestHeaders readFromContext(final ThreadContext ctx) {
        final String credentialsHeader = requiredHeader(ctx, CROSS_CLUSTER_ACCESS_CREDENTIALS_HEADER_KEY);
        // Invoke parsing logic to validate that the header decodes to a valid API key credential.
        // Call `close` since the returned value is an auto-closable.
        parseCredentialsHeader(credentialsHeader).close();

        final String subjectInfoHeader = requiredHeader(ctx, CROSS_CLUSTER_ACCESS_SUBJECT_INFO_HEADER_KEY);
        return new CrossClusterAccessRequestHeaders(credentialsHeader, subjectInfoHeader);
    }

    private static String requiredHeader(final ThreadContext ctx, final String headerKey) {
        final String value = ctx.getHeader(headerKey);
        if (value == null) {
            throw new IllegalArgumentException("cross cluster access header [" + headerKey + "] is required");
        }
        return value;
    }

    ApiKeyService.ApiKeyCredentials credentials() {
        return parseCredentialsHeader(credentialsHeader);
    }

    /**
     * Decodes the sender-controlled subject info. Incoming authentication must call this only after verifying the API key.
     */
    CrossClusterAccessSubjectInfo decodeSubjectInfo() throws IOException {
        return CrossClusterAccessSubjectInfo.decode(encodedSubjectInfoHeader);
    }

    static ApiKeyService.ApiKeyCredentials parseCredentialsHeader(final String header) {
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
}
