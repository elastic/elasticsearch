/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authc;

import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.ssl.PemUtils;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.security.action.apikey.ApiKeyCredentials;
import org.elasticsearch.xpack.core.security.authc.AuthenticationTestHelper;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptorsIntersection;
import org.elasticsearch.xpack.security.transport.CrossClusterApiKeySignatureManager;
import org.elasticsearch.xpack.security.transport.X509CertificateSignature;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.Base64;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.xpack.core.security.authc.CrossClusterAccessSubjectInfo.CROSS_CLUSTER_ACCESS_SUBJECT_INFO_HEADER_KEY;
import static org.elasticsearch.xpack.core.security.authz.RoleDescriptorTestHelper.randomUniquelyNamedRoleDescriptors;
import static org.elasticsearch.xpack.security.authc.CrossClusterAccessHeaders.CROSS_CLUSTER_ACCESS_CREDENTIALS_HEADER_KEY;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** Covers incoming header parsing and round trips from the outbound header writer. */
public class CrossClusterAccessRequestHeadersTests extends ESTestCase {

    public void testWriteReadContextRoundtrip() throws IOException {
        final ThreadContext ctx = new ThreadContext(Settings.EMPTY);
        final String encodedApiKeyHeader = randomEncodedApiKeyHeader();
        final var subjectInfo = AuthenticationTestHelper.randomCrossClusterAccessSubjectInfo(randomRoleDescriptorsIntersection());
        final var toWrite = new CrossClusterAccessHeaders(encodedApiKeyHeader, subjectInfo);

        toWrite.writeToContext(ctx, null);
        final CrossClusterAccessRequestHeaders actual = CrossClusterAccessRequestHeaders.readFromContext(ctx);

        assertThat(actual.decodeSubjectInfo(), equalTo(subjectInfo));
        assertThat(actual.decodeSubjectInfo().cleanAndValidate(), equalTo(subjectInfo.cleanAndValidate()));
        assertCredentialsMatch(actual.credentials(), encodedApiKeyHeader);
        assertThat(actual.credentials().getCertificateIdentity(), nullValue());
        assertThat(actual.signature(), nullValue());
        assertThat(actual.signablePayload(), equalTo(new String[] { subjectInfo.encode(), encodedApiKeyHeader }));
    }

    public void testWriteReadContextRoundtripWithSignature() throws IOException, CertificateException {
        final ThreadContext ctx = new ThreadContext(Settings.EMPTY);
        final String encodedApiKeyHeader = randomEncodedApiKeyHeader();
        final var subjectInfo = AuthenticationTestHelper.randomCrossClusterAccessSubjectInfo(randomRoleDescriptorsIntersection());
        final var toWrite = new CrossClusterAccessHeaders(encodedApiKeyHeader, subjectInfo);
        final X509Certificate[] certificates = getTestCertificates();
        final var testSignature = new X509CertificateSignature(certificates, "MOCK", new BytesArray(new byte[] { 1, 2, 3 }));
        final var signer = mock(CrossClusterApiKeySignatureManager.Signer.class);
        when(signer.sign(subjectInfo.encode(), encodedApiKeyHeader)).thenReturn(testSignature);

        toWrite.writeToContext(ctx, signer);
        final CrossClusterAccessRequestHeaders actual = CrossClusterAccessRequestHeaders.readFromContext(ctx);

        assertThat(actual.decodeSubjectInfo(), equalTo(subjectInfo));
        assertThat(actual.decodeSubjectInfo().cleanAndValidate(), equalTo(subjectInfo.cleanAndValidate()));
        assertCredentialsMatch(actual.credentials(), encodedApiKeyHeader);
        // The leaf certificate's subject is bound to the credentials so it can be checked against the API key during authentication.
        assertThat(
            actual.credentials().getCertificateIdentity(),
            equalTo(CrossClusterAccessRequestHeaders.getCertificateIdentity(testSignature))
        );
        assertThat(actual.signature(), equalTo(testSignature));
        assertThat(actual.signablePayload(), equalTo(new String[] { subjectInfo.encode(), encodedApiKeyHeader }));
    }

    /** Reading incoming headers must preserve the signed payload without decoding subject info. */
    public void testRequestHeadersDoNotDecodeSubjectInfo() throws IOException {
        final ThreadContext ctx = new ThreadContext(Settings.EMPTY);
        final String credentialsHeader = randomEncodedApiKeyHeader();
        ctx.putHeader(CROSS_CLUSTER_ACCESS_CREDENTIALS_HEADER_KEY, credentialsHeader);
        ctx.putHeader(CROSS_CLUSTER_ACCESS_SUBJECT_INFO_HEADER_KEY, "%%%%");

        final CrossClusterAccessRequestHeaders headers = CrossClusterAccessRequestHeaders.readFromContext(ctx);

        assertThat(headers.signablePayload(), equalTo(new String[] { "%%%%", credentialsHeader }));
        expectThrows(IllegalArgumentException.class, headers::decodeSubjectInfo);
    }

    public void testThrowsOnMissingCredentialsHeader() throws IOException {
        final ThreadContext ctx = new ThreadContext(Settings.EMPTY);
        if (randomBoolean()) {
            AuthenticationTestHelper.randomCrossClusterAccessSubjectInfo(randomRoleDescriptorsIntersection()).writeToContext(ctx);
        }

        var actual = expectThrows(IllegalArgumentException.class, () -> CrossClusterAccessRequestHeaders.readFromContext(ctx));

        assertThat(
            actual.getMessage(),
            equalTo("cross cluster access header [" + CROSS_CLUSTER_ACCESS_CREDENTIALS_HEADER_KEY + "] is required")
        );
    }

    public void testThrowsOnMissingSubjectInfoHeader() {
        final ThreadContext ctx = new ThreadContext(Settings.EMPTY);
        ctx.putHeader(CROSS_CLUSTER_ACCESS_CREDENTIALS_HEADER_KEY, randomEncodedApiKeyHeader());

        var actual = expectThrows(IllegalArgumentException.class, () -> CrossClusterAccessRequestHeaders.readFromContext(ctx));

        assertThat(
            actual.getMessage(),
            equalTo("cross cluster access header [" + CROSS_CLUSTER_ACCESS_SUBJECT_INFO_HEADER_KEY + "] is required")
        );
    }

    public void testClusterCredentialsReturnsValidApiKey() throws IOException {
        final String id = UUIDs.randomBase64UUID();
        final String key = UUIDs.randomBase64UUID();
        final String encodedApiKey = encodedApiKeyWithPrefix(id, key);
        final var headers = new CrossClusterAccessHeaders(
            encodedApiKey,
            AuthenticationTestHelper.randomCrossClusterAccessSubjectInfo(randomRoleDescriptorsIntersection())
        );

        final ThreadContext ctx = new ThreadContext(Settings.EMPTY);
        headers.writeToContext(ctx, null);
        try (ApiKeyCredentials actual = CrossClusterAccessRequestHeaders.readFromContext(ctx).credentials()) {
            assertThat(actual.getId(), equalTo(id));
            assertThat(actual.getKey().toString(), equalTo(key));
        }
    }

    public void testReadOnInvalidApiKeyValueThrows() throws IOException {
        final ThreadContext ctx = new ThreadContext(Settings.EMPTY);
        final var expected = new CrossClusterAccessHeaders(
            randomFrom("ApiKey abc", "ApiKey id:key", "ApiKey ", "ApiKey  "),
            AuthenticationTestHelper.randomCrossClusterAccessSubjectInfo(randomRoleDescriptorsIntersection())
        );

        expected.writeToContext(ctx, null);
        var actual = expectThrows(IllegalArgumentException.class, () -> CrossClusterAccessRequestHeaders.readFromContext(ctx));

        assertThat(
            actual.getMessage(),
            equalTo(
                "cross cluster access header [" + CROSS_CLUSTER_ACCESS_CREDENTIALS_HEADER_KEY + "] value must be a valid API key credential"
            )
        );
    }

    public void testReadOnHeaderWithMalformedPrefixThrows() throws IOException {
        final ThreadContext ctx = new ThreadContext(Settings.EMPTY);
        AuthenticationTestHelper.randomCrossClusterAccessSubjectInfo(randomRoleDescriptorsIntersection()).writeToContext(ctx);
        final String encodedApiKey = encodedApiKey(UUIDs.randomBase64UUID(), UUIDs.randomBase64UUID());
        ctx.putHeader(
            CROSS_CLUSTER_ACCESS_CREDENTIALS_HEADER_KEY,
            randomFrom(
                // missing space
                "ApiKey" + encodedApiKey,
                // no prefix
                encodedApiKey,
                // wrong prefix
                "Bearer " + encodedApiKey
            )
        );

        var actual = expectThrows(IllegalArgumentException.class, () -> CrossClusterAccessRequestHeaders.readFromContext(ctx));

        assertThat(
            actual.getMessage(),
            equalTo(
                "cross cluster access header [" + CROSS_CLUSTER_ACCESS_CREDENTIALS_HEADER_KEY + "] value must be a valid API key credential"
            )
        );
    }

    private static void assertCredentialsMatch(ApiKeyCredentials actual, String encodedApiKeyHeader) {
        try (ApiKeyCredentials expected = CrossClusterAccessRequestHeaders.parseCredentials(encodedApiKeyHeader, null)) {
            assertThat(actual.getId(), equalTo(expected.getId()));
            assertThat(actual.getKey().toString(), equalTo(expected.getKey().toString()));
        }
    }

    private X509Certificate[] getTestCertificates() throws CertificateException, IOException {
        return PemUtils.readCertificates(List.of(getDataPath("/org/elasticsearch/xpack/security/signature/signing_rsa.crt")))
            .stream()
            .map(cert -> (X509Certificate) cert)
            .toArray(X509Certificate[]::new);
    }

    private static RoleDescriptorsIntersection randomRoleDescriptorsIntersection() {
        return new RoleDescriptorsIntersection(randomList(0, 3, () -> Set.copyOf(randomUniquelyNamedRoleDescriptors(0, 1))));
    }

    // TODO centralize common usage of this across all tests
    public static String randomEncodedApiKeyHeader() {
        return encodedApiKeyWithPrefix(UUIDs.randomBase64UUID(), UUIDs.randomBase64UUID());
    }

    private static String encodedApiKeyWithPrefix(String id, String key) {
        return "ApiKey " + encodedApiKey(id, key);
    }

    private static String encodedApiKey(String id, String key) {
        return Base64.getEncoder().encodeToString((id + ":" + key).getBytes(StandardCharsets.UTF_8));
    }
}
