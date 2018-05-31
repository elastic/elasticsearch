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
        threadContext.putHeader(KerberosAuthenticationToken.AUTH_HEADER, KerberosAuthenticationToken.NEGOTIATE_AUTH_HEADER + base64Token);

        final KerberosAuthenticationToken kerbAuthnToken = KerberosAuthenticationToken.extractToken(threadContext);
        assertNotNull(kerbAuthnToken);
        assertEquals(UNAUTHENTICATED_PRINCIPAL_NAME, kerbAuthnToken.principal());
        assertEquals(base64Token, kerbAuthnToken.credentials());
    }

    public void testExtractTokenForInvalidAuthorizationHeaderThrowsException() throws IOException {
        threadContext.putHeader(KerberosAuthenticationToken.AUTH_HEADER, KerberosAuthenticationToken.NEGOTIATE_AUTH_HEADER);

        thrown.expect(ElasticsearchSecurityException.class);
        thrown.expectMessage(
                Matchers.equalTo("invalid negotiate authentication header value, expected base64 encoded token but value is empty"));
        thrown.expect(new BaseMatcher<ElasticsearchSecurityException>() {

            @Override
            public boolean matches(Object item) {
                if (item instanceof ElasticsearchSecurityException) {
                    List<String> authHeaderValue =
                            ((ElasticsearchSecurityException) item).getHeader(KerberosAuthenticationToken.WWW_AUTHENTICATE);
                    if (authHeaderValue.size() == 1 && KerberosAuthenticationToken.NEGOTIATE_AUTH_HEADER.equals(authHeaderValue.get(0))) {
                        return true;
                    }
                }
                return false;
            }

            @Override
            public void describeTo(Description description) {
            }
        });
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
        thrown.expect(new BaseMatcher<ElasticsearchSecurityException>() {

            @Override
            public boolean matches(Object item) {
                if (item instanceof ElasticsearchSecurityException) {
                    List<String> authHeaderValue =
                            ((ElasticsearchSecurityException) item).getHeader(KerberosAuthenticationToken.WWW_AUTHENTICATE);
                    if (authHeaderValue.size() == 1 && KerberosAuthenticationToken.NEGOTIATE_AUTH_HEADER.equals(authHeaderValue.get(0))) {
                        return true;
                    }
                }
                return false;
            }

            @Override
            public void describeTo(Description description) {
            }
        });
        KerberosAuthenticationToken.extractToken(threadContext);
        fail("Expected exception not thrown");
    }

    public void testExtractTokenForNoAuthorizationHeaderShouldReturnNull() throws IOException {
        final KerberosAuthenticationToken kerbAuthnToken = KerberosAuthenticationToken.extractToken(threadContext);
        assertNull(kerbAuthnToken);
    }

    public void testExtractTokenForBasicAuthorizationHeaderShouldReturnNull() throws IOException {
        threadContext.putHeader(KerberosAuthenticationToken.AUTH_HEADER, "Basic ");
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
        final KerberosAuthenticationToken kerberosAuthenticationToken = new KerberosAuthenticationToken("base64EncodedToken");
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(kerberosAuthenticationToken, (original) -> {
            return new KerberosAuthenticationToken((String) original.credentials());
        });
    }
}
