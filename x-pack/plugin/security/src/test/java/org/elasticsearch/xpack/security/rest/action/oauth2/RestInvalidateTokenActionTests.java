/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.rest.action.oauth2;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.security.action.token.InvalidateTokenRequest;

import static org.hamcrest.Matchers.containsString;

public class RestInvalidateTokenActionTests extends ESTestCase {

    public void testParserForUserAndRealm() throws Exception {
        final String request = """
            {"username": "user1","realm_name": "realm1"}""";
        try (XContentParser parser = XContentType.JSON.xContent().createParser(XContentParserConfiguration.EMPTY, request)) {
            InvalidateTokenRequest invalidateTokenRequest = RestInvalidateTokenAction.PARSER.parse(parser, null);
            assertEquals("user1", invalidateTokenRequest.getUserName());
            assertEquals("realm1", invalidateTokenRequest.getRealmName());
            assertNull(invalidateTokenRequest.getTokenString());
            assertNull(invalidateTokenRequest.getTokenType());
        }
    }

    public void testParserForToken() throws Exception {
        final String request = """
            {"refresh_token": "refresh_token_string"}""";
        try (XContentParser parser = XContentType.JSON.xContent().createParser(XContentParserConfiguration.EMPTY, request)) {
            InvalidateTokenRequest invalidateTokenRequest = RestInvalidateTokenAction.PARSER.parse(parser, null);
            assertEquals("refresh_token_string", invalidateTokenRequest.getTokenString());
            assertEquals("refresh_token", invalidateTokenRequest.getTokenType().getValue());
            assertNull(invalidateTokenRequest.getRealmName());
            assertNull(invalidateTokenRequest.getUserName());
        }
    }

    public void testParserForIncorrectInput() throws Exception {
        final String request = """
            {"refresh_token": "refresh_token_string","token": "access_token_string"}""";
        try (XContentParser parser = XContentType.JSON.xContent().createParser(XContentParserConfiguration.EMPTY, request)) {
            IllegalArgumentException e = expectThrows(
                IllegalArgumentException.class,
                () -> RestInvalidateTokenAction.PARSER.parse(parser, null)
            );
            assertThat(e.getCause().getMessage(), containsString("only one of [token, refresh_token] may be sent per request"));

        }
    }
}
