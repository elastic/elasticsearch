/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.rest;

import org.elasticsearch.core.RestApiVersion;
import org.elasticsearch.test.ESTestCase;

import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.rest.RestRequest.Method.PUT;
import static org.hamcrest.Matchers.sameInstance;

public class MethodHandlersTests extends ESTestCase {

    private final RestApiVersion current = RestApiVersion.current();
    private final RestApiVersion previous = RestApiVersion.previous();

    public void testLookupForDifferentMethodsSameVersion() {
        RestHandler putHandler = (request, channel, client) -> {};
        RestHandler postHandler = (request, channel, client) -> {};
        MethodHandlers methodHandlers = new MethodHandlers("path").addMethod(PUT, current, putHandler)
            .addMethod(POST, current, postHandler);

        RestHandler found = methodHandlers.getHandler(PUT, current);
        assertThat(found, sameInstance(putHandler));
    }

    public void testLookupForHandlerUnderMultipleMethods() {
        RestHandler handler = (request, channel, client) -> {};
        MethodHandlers methodHandlers = new MethodHandlers("path").addMethod(PUT, current, handler).addMethod(POST, current, handler);

        RestHandler found = methodHandlers.getHandler(PUT, current);
        assertThat(found, sameInstance(handler));

        found = methodHandlers.getHandler(POST, current);
        assertThat(found, sameInstance(handler));
    }

    public void testLookupForHandlersUnderDifferentVersions() {
        RestHandler handler = (request, channel, client) -> {};
        MethodHandlers methodHandlers = new MethodHandlers("path").addMethod(PUT, current, handler).addMethod(PUT, previous, handler);

        RestHandler found = methodHandlers.getHandler(PUT, current);
        assertThat(found, sameInstance(handler));

        found = methodHandlers.getHandler(PUT, previous);
        assertThat(found, sameInstance(handler));
    }

    public void testExceptionOnOverride() {
        RestHandler handler = (request, channel, client) -> {};
        MethodHandlers methodHandlers = new MethodHandlers("path").addMethod(PUT, current, handler);

        expectThrows(IllegalArgumentException.class, () -> methodHandlers.addMethod(PUT, current, handler));
    }

    public void testMissingCurrentHandler() {
        RestHandler handler = (request, channel, client) -> {};
        MethodHandlers methodHandlers = new MethodHandlers("path").addMethod(PUT, previous, handler).addMethod(POST, previous, handler);

        RestHandler found = methodHandlers.getHandler(PUT, current);
        assertNull(found);
    }

    public void testMissingPriorHandlerReturnsCurrentHandler() {
        RestHandler handler = (request, channel, client) -> {};
        MethodHandlers methodHandlers = new MethodHandlers("path").addMethod(PUT, current, handler).addMethod(POST, current, handler);

        RestHandler found = methodHandlers.getHandler(PUT, previous);
        assertThat(found, sameInstance(handler));
    }
}
