/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.security.authc.kerberos;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.EqualsHashCodeTestUtils;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Base64;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.notNullValue;

public class KerberosAuthenticationTokenTests extends ESTestCase {

    private static final String UNAUTHENTICATED_PRINCIPAL_NAME = "<Kerberos Token>";

    public void testExtractTokenForValidAuthorizationHeader() throws IOException {
        final String base64Token = Base64.getEncoder().encodeToString(randomAlphaOfLength(5).getBytes(StandardCharsets.UTF_8));
        final String negotiate = randomBoolean() ? KerberosAuthenticationToken.NEGOTIATE_AUTH_HEADER_PREFIX : "negotiate ";
        final String authzHeader = negotiate + base64Token;

        final KerberosAuthenticationToken kerbAuthnToken = KerberosAuthenticationToken.extractToken(authzHeader);
        assertNotNull(kerbAuthnToken);
        assertEquals(UNAUTHENTICATED_PRINCIPAL_NAME, kerbAuthnToken.principal());
        assertThat(kerbAuthnToken.credentials(), instanceOf((byte[].class)));
        assertArrayEquals(Base64.getDecoder().decode(base64Token), (byte[]) kerbAuthnToken.credentials());
    }

    public void testExtractTokenForInvalidNegotiateAuthorizationHeaderShouldReturnNull() throws IOException {
        final String header = randomFrom("negotiate", "Negotiate", " Negotiate", "NegotiateToken", "Basic ", " Custom ", null);
        assertNull(KerberosAuthenticationToken.extractToken(header));
    }

    public void testExtractTokenForNegotiateAuthorizationHeaderWithNoTokenShouldThrowException() throws IOException {
        final String header = randomFrom(KerberosAuthenticationToken.NEGOTIATE_AUTH_HEADER_PREFIX, "negotiate ", "Negotiate      ");
        final ElasticsearchSecurityException e =
                expectThrows(ElasticsearchSecurityException.class, () -> KerberosAuthenticationToken.extractToken(header));
        assertThat(e.getMessage(),
                equalTo("invalid negotiate authentication header value, expected base64 encoded token but value is empty"));
        assertContainsAuthenticateHeader(e);
    }

    public void testExtractTokenForNotBase64EncodedTokenThrowsException() throws IOException {
        final String notBase64Token = "[B@6499375d";

        final ElasticsearchSecurityException e = expectThrows(ElasticsearchSecurityException.class,
                () -> KerberosAuthenticationToken.extractToken(KerberosAuthenticationToken.NEGOTIATE_AUTH_HEADER_PREFIX + notBase64Token));
        assertThat(e.getMessage(),
                equalTo("invalid negotiate authentication header value, could not decode base64 token " + notBase64Token));
        assertContainsAuthenticateHeader(e);
    }

    public void testKerberoAuthenticationTokenClearCredentials() {
        byte[] inputBytes = randomByteArrayOfLength(5);
        final String base64Token = Base64.getEncoder().encodeToString(inputBytes);
        final KerberosAuthenticationToken kerbAuthnToken =
                KerberosAuthenticationToken.extractToken(KerberosAuthenticationToken.NEGOTIATE_AUTH_HEADER_PREFIX + base64Token);
        kerbAuthnToken.clearCredentials();
        Arrays.fill(inputBytes, (byte) 0);
        assertArrayEquals(inputBytes, (byte[]) kerbAuthnToken.credentials());
    }

    public void testEqualsHashCode() {
        final KerberosAuthenticationToken kerberosAuthenticationToken =
                new KerberosAuthenticationToken("base64EncodedToken".getBytes(StandardCharsets.UTF_8));
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(kerberosAuthenticationToken, (original) -> {
            return new KerberosAuthenticationToken((byte[]) original.credentials());
        });
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(kerberosAuthenticationToken, (original) -> {
            byte[] originalCreds = (byte[]) original.credentials();
            return new KerberosAuthenticationToken(Arrays.copyOf(originalCreds, originalCreds.length));
        });
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(kerberosAuthenticationToken, (original) -> {
            return new KerberosAuthenticationToken((byte[]) original.credentials());
        }, KerberosAuthenticationTokenTests::mutateTestItem);
    }

    private static KerberosAuthenticationToken mutateTestItem(KerberosAuthenticationToken original) {
        switch (randomIntBetween(0, 2)) {
            case 0:
                return new KerberosAuthenticationToken(randomByteArrayOfLength(10));
            case 1:
                return new KerberosAuthenticationToken("base64EncodedToken".getBytes(StandardCharsets.UTF_16));
            case 2:
                return new KerberosAuthenticationToken("[B@6499375d".getBytes(StandardCharsets.UTF_8));
            default:
                throw new IllegalArgumentException("unknown option");
        }
    }

    private static void assertContainsAuthenticateHeader(ElasticsearchSecurityException e) {
        assertThat(e.status(), is(RestStatus.UNAUTHORIZED));
        assertThat(e.getHeaderKeys(), hasSize(1));
        assertThat(e.getHeader(KerberosAuthenticationToken.WWW_AUTHENTICATE), notNullValue());
        assertThat(e.getHeader(KerberosAuthenticationToken.WWW_AUTHENTICATE), contains(KerberosAuthenticationToken.NEGOTIATE_SCHEME_NAME));
    }
}
