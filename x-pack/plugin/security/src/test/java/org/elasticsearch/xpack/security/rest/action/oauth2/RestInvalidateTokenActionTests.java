/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.rest.action.oauth2;

import org.elasticsearch.common.xcontent.DeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.security.action.token.InvalidateTokenRequest;

public class RestInvalidateTokenActionTests extends ESTestCase {

    public void testParser() throws Exception {
        final String request = "{" +
            "\"username\": \"user1\"," +
            "\"realm_name\": \"realm1\"" +
            "}";
        try (XContentParser parser = XContentType.JSON.xContent()
            .createParser(NamedXContentRegistry.EMPTY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION, request)) {
            InvalidateTokenRequest invalidateTokenRequest = RestInvalidateTokenAction.PARSER.parse(parser, null);
            assertEquals("user1", invalidateTokenRequest.getUserName());
            assertEquals("realm1", invalidateTokenRequest.getRealmName());
        }
    }
}
