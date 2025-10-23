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
import org.elasticsearch.xpack.core.security.authc.CrossClusterAccessSubjectInfo;
import org.elasticsearch.xpack.security.transport.CrossClusterApiKeySignatureManager;
import org.elasticsearch.xpack.security.transport.X509CertificateSignature;

import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;

import javax.security.auth.x500.X500Principal;

import static org.elasticsearch.xpack.core.security.authc.CrossClusterAccessSubjectInfo.CROSS_CLUSTER_ACCESS_SUBJECT_INFO_HEADER_KEY;

public final class CrossClusterAccessHeaders {

    public static final String CROSS_CLUSTER_ACCESS_CREDENTIALS_HEADER_KEY = "_cross_cluster_access_credentials";
    private final String credentialsHeader;
    private final CrossClusterAccessSubjectInfo crossClusterAccessSubjectInfo;
    private final X509CertificateSignature signature;
    private final String[] signablePayload;

    public CrossClusterAccessHeaders(String credentialsHeader, CrossClusterAccessSubjectInfo crossClusterAccessSubjectInfo) {
        this(credentialsHeader, crossClusterAccessSubjectInfo, null);
    }

    private CrossClusterAccessHeaders(
        String credentialsHeader,
        CrossClusterAccessSubjectInfo crossClusterAccessSubjectInfo,
        @Nullable X509CertificateSignature signature,
        String... signablePayload
    ) {
        assert credentialsHeader.startsWith("ApiKey ") : "credentials header must start with [ApiKey ]";
        this.credentialsHeader = credentialsHeader;
        this.crossClusterAccessSubjectInfo = crossClusterAccessSubjectInfo;
        this.signature = signature;
        this.signablePayload = signablePayload;
    }

    public void writeToContext(final ThreadContext ctx, @Nullable CrossClusterApiKeySignatureManager.Signer signer) throws IOException {
        var encodedSubjectInfo = crossClusterAccessSubjectInfo.encode();
        if (signer != null) {
            var signature = signer.sign(encodedSubjectInfo, credentialsHeader);
            X509CertificateSignature.writeToContext(ctx, signature);
        }

        ctx.putHeader(CROSS_CLUSTER_ACCESS_SUBJECT_INFO_HEADER_KEY, encodedSubjectInfo);
        ctx.putHeader(CROSS_CLUSTER_ACCESS_CREDENTIALS_HEADER_KEY, credentialsHeader);
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

        final String subjectInfoHeader = ctx.getHeader(CROSS_CLUSTER_ACCESS_SUBJECT_INFO_HEADER_KEY);
        if (subjectInfoHeader == null) {
            throw new IllegalArgumentException(
                "cross cluster access header [" + CROSS_CLUSTER_ACCESS_SUBJECT_INFO_HEADER_KEY + "] is required"
            );
        }
        var subjectInfo = CrossClusterAccessSubjectInfo.decode(subjectInfoHeader);

        return new CrossClusterAccessHeaders(
            credentialsHeader,
            subjectInfo,
            X509CertificateSignature.readFromContext(ctx),
            subjectInfoHeader,
            credentialsHeader
        );
    }

    public ApiKeyService.ApiKeyCredentials credentials() {
        return parseCredentialsHeader(credentialsHeader, getCertificateIdentity(signature));
    }

    public static String getCertificateIdentity(X509CertificateSignature signature) {
        if (signature != null) {
            if (signature.certificates().length == 0) {
                throw new IllegalArgumentException("Provided signature does not contain any certificates");
            }
            return signature.leafCertificate().getSubjectX500Principal().getName(X500Principal.RFC2253);
        }
        return null;
    }

    public X509CertificateSignature signature() {
        return signature;
    }

    public String[] signablePayload() {
        return signablePayload;
    }

    static ApiKeyService.ApiKeyCredentials parseCredentialsHeader(String credentialsHeader) {
        return parseCredentialsHeader(credentialsHeader, null);
    }

    static ApiKeyService.ApiKeyCredentials parseCredentialsHeader(final String header, @Nullable String expectedCertificateIdentity) {
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
            && Objects.equals(this.crossClusterAccessSubjectInfo, that.crossClusterAccessSubjectInfo)
            && Objects.equals(this.signature, that.signature)
            && Arrays.equals(this.signablePayload, that.signablePayload);
    }

    @Override
    public int hashCode() {
        return Objects.hash(credentialsHeader, crossClusterAccessSubjectInfo, this.signature, Arrays.hashCode(this.signablePayload));
    }
}
