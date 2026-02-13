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

import static org.elasticsearch.xpack.core.security.authz.RoleDescriptorTestHelper.randomUniquelyNamedRoleDescriptors;
import static org.elasticsearch.xpack.security.authc.CrossClusterAccessHeaders.CROSS_CLUSTER_ACCESS_CREDENTIALS_HEADER_KEY;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class CrossClusterAccessHeadersTests extends ESTestCase {

    public void testWriteReadContextRoundtrip() throws IOException {
        final ThreadContext ctx = new ThreadContext(Settings.EMPTY);
        final var expected = new CrossClusterAccessHeaders(
            randomEncodedApiKeyHeader(),
            AuthenticationTestHelper.randomCrossClusterAccessSubjectInfo(randomRoleDescriptorsIntersection())
        );

        expected.writeToContext(ctx, null);
        final CrossClusterAccessHeaders actual = CrossClusterAccessHeaders.readFromContext(ctx);

        assertThat(actual.getSubjectInfo(), equalTo(expected.getSubjectInfo()));
        assertThat(actual.getCleanAndValidatedSubjectInfo(), equalTo(expected.getCleanAndValidatedSubjectInfo()));
        assertThat(actual.credentials().getId(), equalTo(expected.credentials().getId()));
        assertThat(actual.credentials().getKey().toString(), equalTo(expected.credentials().getKey().toString()));
    }

    public void testWriteReadContextRoundtripWithSignature() throws IOException, CertificateException {
        final ThreadContext ctx = new ThreadContext(Settings.EMPTY);
        var encodedApiKeyHeader = randomEncodedApiKeyHeader();
        var subjectInfo = AuthenticationTestHelper.randomCrossClusterAccessSubjectInfo(randomRoleDescriptorsIntersection());
        final var toWrite = new CrossClusterAccessHeaders(encodedApiKeyHeader, subjectInfo);
        var testSignature = new X509CertificateSignature(getTestCertificates(), "MOCK", new BytesArray(new byte[] { 1, 2, 3 }));
        var signer = mock(CrossClusterApiKeySignatureManager.Signer.class);
        when(signer.sign(subjectInfo.encode(), encodedApiKeyHeader)).thenReturn(testSignature);

        toWrite.writeToContext(ctx, signer);
        final CrossClusterAccessHeaders actual = CrossClusterAccessHeaders.readFromContext(ctx);

        assertThat(actual.getSubjectInfo(), equalTo(toWrite.getSubjectInfo()));
        assertThat(actual.getCleanAndValidatedSubjectInfo(), equalTo(toWrite.getCleanAndValidatedSubjectInfo()));
        assertThat(actual.credentials().getId(), equalTo(toWrite.credentials().getId()));
        assertThat(actual.credentials().getKey().toString(), equalTo(toWrite.credentials().getKey().toString()));
        assertThat(actual.signature(), equalTo(testSignature));
        assertThat(actual.signablePayload(), equalTo(new String[] { subjectInfo.encode(), encodedApiKeyHeader }));
    }

    public void testThrowsOnMissingEntry() {
        var actual = expectThrows(
            IllegalArgumentException.class,
            () -> CrossClusterAccessHeaders.readFromContext(new ThreadContext(Settings.EMPTY))
        );
        assertThat(
            actual.getMessage(),
            equalTo("cross cluster access header [" + CROSS_CLUSTER_ACCESS_CREDENTIALS_HEADER_KEY + "] is required")
        );
    }

    public void testClusterCredentialsReturnsValidApiKey() {
        final String id = UUIDs.randomBase64UUID();
        final String key = UUIDs.randomBase64UUID();
        final String encodedApiKey = encodedApiKeyWithPrefix(id, key);
        final var headers = new CrossClusterAccessHeaders(
            encodedApiKey,
            AuthenticationTestHelper.randomCrossClusterAccessSubjectInfo(randomRoleDescriptorsIntersection())
        );

        final ApiKeyService.ApiKeyCredentials actual = headers.credentials();

        assertThat(actual.getId(), equalTo(id));
        assertThat(actual.getKey().toString(), equalTo(key));
    }

    public void testReadOnInvalidApiKeyValueThrows() throws IOException {
        final ThreadContext ctx = new ThreadContext(Settings.EMPTY);
        final var expected = new CrossClusterAccessHeaders(
            randomFrom("ApiKey abc", "ApiKey id:key", "ApiKey ", "ApiKey  "),
            AuthenticationTestHelper.randomCrossClusterAccessSubjectInfo(randomRoleDescriptorsIntersection())
        );

        expected.writeToContext(ctx, null);
        var actual = expectThrows(IllegalArgumentException.class, () -> CrossClusterAccessHeaders.readFromContext(ctx));

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

        var actual = expectThrows(IllegalArgumentException.class, () -> CrossClusterAccessHeaders.readFromContext(ctx));

        assertThat(
            actual.getMessage(),
            equalTo(
                "cross cluster access header [" + CROSS_CLUSTER_ACCESS_CREDENTIALS_HEADER_KEY + "] value must be a valid API key credential"
            )
        );
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
