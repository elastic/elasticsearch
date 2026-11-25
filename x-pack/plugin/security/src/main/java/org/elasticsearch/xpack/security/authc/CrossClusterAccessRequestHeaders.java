/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authc;

import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.core.security.action.apikey.ApiKey;
import org.elasticsearch.xpack.core.security.action.apikey.ApiKeyCredentials;
import org.elasticsearch.xpack.core.security.authc.CrossClusterAccessSubjectInfo;
import org.elasticsearch.xpack.security.transport.X509CertificateSignature;

import java.io.IOException;
import java.util.Objects;

import javax.security.auth.x500.X500Principal;

import static org.elasticsearch.xpack.core.security.authc.CrossClusterAccessSubjectInfo.CROSS_CLUSTER_ACCESS_SUBJECT_INFO_HEADER_KEY;
import static org.elasticsearch.xpack.security.authc.CrossClusterAccessHeaders.CROSS_CLUSTER_ACCESS_CREDENTIALS_HEADER_KEY;

/**
 * Incoming cross-cluster headers with subject info kept encoded so credentials and signatures can be verified before decoding it.
 */
final class CrossClusterAccessRequestHeaders {

    private final String credentialsHeader;
    private final String encodedSubjectInfoHeader;
    private final X509CertificateSignature signature;

    private CrossClusterAccessRequestHeaders(
        String credentialsHeader,
        String encodedSubjectInfoHeader,
        @Nullable X509CertificateSignature signature
    ) {
        this.credentialsHeader = credentialsHeader;
        this.encodedSubjectInfoHeader = encodedSubjectInfoHeader;
        this.signature = signature;
    }

    static CrossClusterAccessRequestHeaders readFromContext(final ThreadContext ctx) throws IOException {
        final String credentialsHeader = requiredHeader(ctx, CROSS_CLUSTER_ACCESS_CREDENTIALS_HEADER_KEY);
        // Invoke parsing logic to validate that the header decodes to a valid API key credential.
        // Call `close` since the returned value is an auto-closable.
        parseCredentialsHeader(credentialsHeader, null).close();

        final String subjectInfoHeader = requiredHeader(ctx, CROSS_CLUSTER_ACCESS_SUBJECT_INFO_HEADER_KEY);
        return new CrossClusterAccessRequestHeaders(credentialsHeader, subjectInfoHeader, X509CertificateSignature.readFromContext(ctx));
    }

    private static String requiredHeader(final ThreadContext ctx, final String headerKey) {
        final String value = ctx.getHeader(headerKey);
        if (value == null) {
            throw new IllegalArgumentException("cross cluster access header [" + headerKey + "] is required");
        }
        return value;
    }

    ApiKeyCredentials credentials() {
        return parseCredentials(credentialsHeader, signature);
    }

    @Nullable
    X509CertificateSignature signature() {
        return signature;
    }

    String[] signablePayload() {
        return new String[] { encodedSubjectInfoHeader, credentialsHeader };
    }

    /**
     * Decodes the sender-controlled subject info. Incoming authentication must call this only after verifying the API key.
     */
    CrossClusterAccessSubjectInfo decodeSubjectInfo() throws IOException {
        return CrossClusterAccessSubjectInfo.decode(encodedSubjectInfoHeader);
    }

    /**
     * Parses the credentials header into cross-cluster API key credentials, binding the expected certificate identity from the
     * optional signature so that it is checked against the API key during authentication.
     */
    static ApiKeyCredentials parseCredentials(final String credentialsHeader, @Nullable X509CertificateSignature signature) {
        return parseCredentialsHeader(credentialsHeader, getCertificateIdentity(signature));
    }

    @Nullable
    static String getCertificateIdentity(@Nullable X509CertificateSignature signature) {
        if (signature != null) {
            if (signature.certificates().length == 0) {
                throw new IllegalArgumentException("Provided signature does not contain any certificates");
            }
            return signature.leafCertificate().getSubjectX500Principal().getName(X500Principal.RFC2253);
        }
        return null;
    }

    private static ApiKeyCredentials parseCredentialsHeader(final String header, @Nullable String expectedCertificateIdentity) {
        try {
            return Objects.requireNonNull(
                ApiKeyService.getCredentialsFromHeader(header, expectedCertificateIdentity, ApiKey.Type.CROSS_CLUSTER)
            );
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
