/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.authc.support;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken;
import org.junit.Rule;
import org.junit.rules.ExpectedException;

import java.nio.charset.StandardCharsets;
import java.util.Base64;

import static org.elasticsearch.test.SecurityTestsUtils.assertAuthenticationException;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class UsernamePasswordTokenTests extends ESTestCase {
    @Rule
    public ExpectedException thrown = ExpectedException.none();

    public void testPutToken() throws Exception {
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        UsernamePasswordToken.putTokenHeader(threadContext, new UsernamePasswordToken("user1", new SecureString("test123")));
        String header = threadContext.getHeader(UsernamePasswordToken.BASIC_AUTH_HEADER);
        assertThat(header, notNullValue());
        assertTrue(header.startsWith("Basic "));
        String token = header.substring("Basic ".length());
        token = new String(Base64.getDecoder().decode(token), StandardCharsets.UTF_8);
        int i = token.indexOf(":");
        assertTrue(i > 0);
        String username = token.substring(0, i);
        String password = token.substring(i + 1);
        assertThat(username, equalTo("user1"));
        assertThat(password, equalTo("test123"));
    }

    public void testExtractToken() throws Exception {
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        final String header = randomFrom("Basic ", "basic ", "BASIC ")
                + Base64.getEncoder().encodeToString("user1:test123".getBytes(StandardCharsets.UTF_8));
        threadContext.putHeader(UsernamePasswordToken.BASIC_AUTH_HEADER, header);
        UsernamePasswordToken token = UsernamePasswordToken.extractToken(threadContext);
        assertThat(token, notNullValue());
        assertThat(token.principal(), equalTo("user1"));
        assertThat(new String(token.credentials().getChars()), equalTo("test123"));
    }

    public void testExtractTokenInvalid() throws Exception {
        final String[] invalidValues = { "Basic ", "Basic f", "basic " };
        for (String value : invalidValues) {
            ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
            threadContext.putHeader(UsernamePasswordToken.BASIC_AUTH_HEADER, value);
            try {
                UsernamePasswordToken.extractToken(threadContext);
                fail("Expected an authentication exception for invalid basic auth token [" + value + "]");
            } catch (ElasticsearchSecurityException e) {
                // expected
                assertAuthenticationException(e);
            }
        }
    }

    public void testHeaderNotMatchingReturnsNull() {
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        final String header = randomFrom("Basic", "BasicBroken", "invalid", "   basic   ");
        threadContext.putHeader(UsernamePasswordToken.BASIC_AUTH_HEADER, header);
        UsernamePasswordToken extracted = UsernamePasswordToken.extractToken(threadContext);
        assertThat(extracted, nullValue());
    }

    public void testExtractTokenMissing() throws Exception {
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        assertThat(UsernamePasswordToken.extractToken(threadContext), nullValue());
    }

    public void testEqualsWithDifferentPasswords() {
        UsernamePasswordToken token1 = new UsernamePasswordToken("username", new SecureString("password".toCharArray()));
        UsernamePasswordToken token2 = new UsernamePasswordToken("username", new SecureString("new password".toCharArray()));
        assertThat(token1, not(equalTo(token2)));
    }

    public void testEqualsWithDifferentUsernames() {
        UsernamePasswordToken token1 = new UsernamePasswordToken("username", new SecureString("password".toCharArray()));
        UsernamePasswordToken token2 = new UsernamePasswordToken("username1", new SecureString("password".toCharArray()));
        assertThat(token1, not(equalTo(token2)));
    }

    public void testEquals() {
        UsernamePasswordToken token1 = new UsernamePasswordToken("username", new SecureString("password".toCharArray()));
        UsernamePasswordToken token2 = new UsernamePasswordToken("username", new SecureString("password".toCharArray()));
        assertThat(token1, equalTo(token2));
    }

    public static String basicAuthHeaderValue(String username, String passwd) {
        return UsernamePasswordToken.basicAuthHeaderValue(username, new SecureString(passwd.toCharArray()));
    }
}
