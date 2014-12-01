/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.authc.support;

import com.google.common.base.Charsets;
import org.apache.commons.codec.binary.Base64;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.shield.authc.AuthenticationException;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.elasticsearch.transport.TransportRequest;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static org.elasticsearch.shield.test.ShieldAssertions.assertContainsWWWAuthenticateHeader;
import static org.hamcrest.Matchers.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 *
 */
public class UsernamePasswordTokenTests extends ElasticsearchTestCase {

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Test
    public void testPutToken() throws Exception {
        TransportRequest request = new TransportRequest() {};
        UsernamePasswordToken.putTokenHeader(request, new UsernamePasswordToken("user1", SecuredStringTests.build("test123")));
        String header = request.getHeader(UsernamePasswordToken.BASIC_AUTH_HEADER);
        assertThat(header, notNullValue());
        assertTrue(header.startsWith("Basic "));
        String token = header.substring("Basic ".length());
        token = new String(Base64.decodeBase64(token), Charsets.UTF_8);
        int i = token.indexOf(":");
        assertTrue(i > 0);
        String username = token.substring(0, i);
        String password = token.substring(i + 1);
        assertThat(username, equalTo("user1"));
        assertThat(password, equalTo("test123"));
    }

    @Test
    public void testExtractToken() throws Exception {
        TransportRequest request = new TransportRequest() {};
        String header = "Basic " + new String(Base64.encodeBase64("user1:test123".getBytes(Charsets.UTF_8)), Charsets.UTF_8);
        request.putHeader(UsernamePasswordToken.BASIC_AUTH_HEADER, header);
        UsernamePasswordToken token = UsernamePasswordToken.extractToken(request, null);
        assertThat(token, notNullValue());
        assertThat(token.principal(), equalTo("user1"));
        assertThat(new String(token.credentials().internalChars()), equalTo("test123"));
    }

    @Test
    public void testExtractToken_Invalid() throws Exception {
        String[] invalidValues = { "Basic", "Basic ", "Basic f" };
        for (String value : invalidValues) {
            TransportRequest request = new TransportRequest() {};
            request.putHeader(UsernamePasswordToken.BASIC_AUTH_HEADER, value);
            try {
                UsernamePasswordToken.extractToken(request, null);
                fail("Expected an authentication exception for invalid basic auth token [" + value + "]");
            } catch (AuthenticationException ae) {
                // expected
            }
        }
    }

    @Test
    public void testThatAuthorizationExceptionContainsResponseHeaders() {
        TransportRequest request = new TransportRequest() {};
        String header = "BasicBroken";
        request.putHeader(UsernamePasswordToken.BASIC_AUTH_HEADER, header);
        try {
            UsernamePasswordToken.extractToken(request, null);
            fail("Expected exception but did not happen");
        } catch (AuthenticationException e) {
            assertContainsWWWAuthenticateHeader(e);
        }
    }

    @Test
    public void testExtractTokenRest() throws Exception {
        RestRequest request = mock(RestRequest.class);
        UsernamePasswordToken token = new UsernamePasswordToken("username", SecuredStringTests.build("changeme"));
        when(request.header(UsernamePasswordToken.BASIC_AUTH_HEADER)).thenReturn(UsernamePasswordToken.basicAuthHeaderValue("username", SecuredStringTests.build("changeme")));
        assertThat(UsernamePasswordToken.extractToken(request, null), equalTo(token));
    }

    @Test
    public void testExtractTokenRest_Missing() throws Exception {
        RestRequest request = mock(RestRequest.class);
        when(request.header(UsernamePasswordToken.BASIC_AUTH_HEADER)).thenReturn(null);
        assertThat(UsernamePasswordToken.extractToken(request, null), nullValue());
    }

    @Test
    public void testExtractTokenRest_WithInvalidToken1() throws Exception {
        thrown.expect(AuthenticationException.class);
        RestRequest request = mock(RestRequest.class);
        when(request.header(UsernamePasswordToken.BASIC_AUTH_HEADER)).thenReturn("invalid");
        UsernamePasswordToken.extractToken(request, null);
    }

    @Test
    public void testExtractTokenRest_WithInvalidToken2() throws Exception {
        thrown.expect(AuthenticationException.class);
        RestRequest request = mock(RestRequest.class);
        when(request.header(UsernamePasswordToken.BASIC_AUTH_HEADER)).thenReturn("Basic");
        UsernamePasswordToken.extractToken(request, null);
    }

    @Test
    public void testEqualsWithDifferentPasswords() {
        UsernamePasswordToken token1 = new UsernamePasswordToken("username", new SecuredString("password".toCharArray()));
        UsernamePasswordToken token2 = new UsernamePasswordToken("username", new SecuredString("new password".toCharArray()));
        assertThat(token1, not(equalTo(token2)));
    }

    @Test
    public void testEqualsWithDifferentUsernames() {
        UsernamePasswordToken token1 = new UsernamePasswordToken("username", new SecuredString("password".toCharArray()));
        UsernamePasswordToken token2 = new UsernamePasswordToken("username1", new SecuredString("password".toCharArray()));
        assertThat(token1, not(equalTo(token2)));
    }

    @Test
    public void testEquals() {
        UsernamePasswordToken token1 = new UsernamePasswordToken("username", new SecuredString("password".toCharArray()));
        UsernamePasswordToken token2 = new UsernamePasswordToken("username", new SecuredString("password".toCharArray()));
        assertThat(token1, equalTo(token2));
    }
}
