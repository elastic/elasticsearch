/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.security.authc.kerberos;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.EqualsHashCodeTestUtils;
import org.elasticsearch.xpack.security.authc.kerberos.support.KerberosTestCase;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.ExpectedException;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Base64;
import java.util.List;

public class KerberosAuthenticationTokenTests extends ESTestCase {

    private static final String UNAUTHENTICATED_PRINCIPAL_NAME = "<Unauthenticated Principal Name>";

    private ThreadContext threadContext;
    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Before
    public void setup() throws IOException {
        final Path dir = createTempDir();
        final Path ktab = KerberosTestCase.writeKeyTab(dir, "http.keytab", null);
        final Settings settings = KerberosTestCase.buildKerberosRealmSettings(ktab.toString());
        threadContext = new ThreadContext(settings);
    }

    @After
    public void cleanup() throws IOException {
        threadContext.close();
        threadContext = null;
    }

    public void testExtractTokenForValidAuthorizationHeader() throws IOException {
        final String base64Token = Base64.getEncoder().encodeToString(randomAlphaOfLength(5).getBytes(StandardCharsets.UTF_8));
        final String negotiate = randomBoolean() ? KerberosAuthenticationToken.NEGOTIATE_AUTH_HEADER : "negotiate ";
        threadContext.putHeader(KerberosAuthenticationToken.AUTH_HEADER, negotiate + base64Token);

        final KerberosAuthenticationToken kerbAuthnToken = KerberosAuthenticationToken.extractToken(threadContext);
        assertNotNull(kerbAuthnToken);
        assertEquals(UNAUTHENTICATED_PRINCIPAL_NAME, kerbAuthnToken.principal());
        assertTrue(kerbAuthnToken.credentials() instanceof byte[]);
        assertArrayEquals(Base64.getDecoder().decode(base64Token), (byte[]) kerbAuthnToken.credentials());
    }

    public void testExtractTokenForInvalidAuthorizationHeaderThrowsException() throws IOException {
        final String header =
                randomFrom(KerberosAuthenticationToken.NEGOTIATE_AUTH_HEADER, "negotiate", "negotiate ", "Negotiate", "Negotiate        ");
        threadContext.putHeader(KerberosAuthenticationToken.AUTH_HEADER, header);

        thrown.expect(ElasticsearchSecurityException.class);
        thrown.expectMessage(
                Matchers.equalTo("invalid negotiate authentication header value, expected base64 encoded token but value is empty"));
        thrown.expect(new ESEMatcher());
        KerberosAuthenticationToken.extractToken(threadContext);
        fail("Expected exception not thrown");
    }

    public void testExtractTokenForNotBase64EncodedTokenThrowsException() throws IOException {
        final String notBase64Token = "[B@6499375d";
        threadContext.putHeader(KerberosAuthenticationToken.AUTH_HEADER,
                KerberosAuthenticationToken.NEGOTIATE_AUTH_HEADER + notBase64Token);

        thrown.expect(ElasticsearchSecurityException.class);
        thrown.expectMessage(
                Matchers.equalTo("invalid negotiate authentication header value, could not decode base64 token " + notBase64Token));
        thrown.expect(new ESEMatcher());
        KerberosAuthenticationToken.extractToken(threadContext);
        fail("Expected exception not thrown");
    }

    public void testExtractTokenForNoAuthorizationHeaderShouldReturnNull() throws IOException {
        final String header = randomFrom(" Negotiate", "Basic ", " Random ", null);
        if (header != null) {
            threadContext.putHeader(KerberosAuthenticationToken.AUTH_HEADER, header);
        }
        final KerberosAuthenticationToken kerbAuthnToken = KerberosAuthenticationToken.extractToken(threadContext);
        assertNull(kerbAuthnToken);
    }

    public void testKerberoAuthenticationTokenClearCredentials() {
        final String base64Token = Base64.getEncoder().encodeToString(randomAlphaOfLength(5).getBytes(StandardCharsets.UTF_8));
        threadContext.putHeader(KerberosAuthenticationToken.AUTH_HEADER, KerberosAuthenticationToken.NEGOTIATE_AUTH_HEADER + base64Token);
        final KerberosAuthenticationToken kerbAuthnToken = KerberosAuthenticationToken.extractToken(threadContext);
        kerbAuthnToken.clearCredentials();
        assertNull(kerbAuthnToken.credentials());
    }

    public void testEqualsHashCode() {
        final KerberosAuthenticationToken kerberosAuthenticationToken =
                new KerberosAuthenticationToken("base64EncodedToken".getBytes(StandardCharsets.UTF_8));
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(kerberosAuthenticationToken, (original) -> {
            return new KerberosAuthenticationToken((byte[]) original.credentials());
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

    private static class ESEMatcher extends BaseMatcher<ElasticsearchSecurityException> {
        @Override
        public boolean matches(Object item) {
            if (item instanceof ElasticsearchSecurityException) {
                List<String> authHeaderValue =
                        ((ElasticsearchSecurityException) item).getHeader(KerberosAuthenticationToken.WWW_AUTHENTICATE);
                if (authHeaderValue.size() == 1 && KerberosAuthenticationToken.NEGOTIATE_AUTH_HEADER.contains(authHeaderValue.get(0))) {
                    return true;
                }
            }
            return false;
        }

        @Override
        public void describeTo(Description description) {
        }
    }
}
