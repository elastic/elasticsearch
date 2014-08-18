/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.authc.support;

import com.google.common.base.Charsets;
import org.apache.commons.codec.binary.Base64;
import org.elasticsearch.shield.authc.AuthenticationException;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.elasticsearch.transport.TransportRequest;
import org.junit.Test;

import static org.elasticsearch.shield.test.ShieldAssertions.assertContainsWWWAuthenticateHeader;
import static org.hamcrest.Matchers.*;

/**
 *
 */
public class UsernamePasswordTokenTests extends ElasticsearchTestCase {

    @Test
    public void testPutToken() throws Exception {
        TransportRequest request = new TransportRequest() {};
        UsernamePasswordToken.putTokenHeader(request, new UsernamePasswordToken("user1", "test123".toCharArray()));
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
        assertThat(new String(token.credentials()), equalTo("test123"));

        // making sure that indeed, once resolved the instance is reused across multiple resolve calls
        UsernamePasswordToken token2 = UsernamePasswordToken.extractToken(request, null);
        assertThat(token, is(token2));
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
}
