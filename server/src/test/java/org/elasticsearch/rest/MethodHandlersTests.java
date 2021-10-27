/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.rest;

import org.elasticsearch.test.ESTestCase;

import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.rest.RestRequest.Method.PUT;
import static org.hamcrest.Matchers.sameInstance;

public class MethodHandlersTests extends ESTestCase {

    public void testLookupForDifferentMethods() {
        RestHandler putHandler = (request, channel, client) -> {};
        RestHandler postHandler = (request, channel, client) -> {};
        MethodHandlers methodHandlers = new MethodHandlers("path").addMethod(PUT, putHandler).addMethod(POST, postHandler);

        RestHandler found = methodHandlers.getHandler(PUT);
        assertThat(found, sameInstance(putHandler));
    }

    public void testLookupForHandlerUnderMultipleMethods() {
        RestHandler handler = (request, channel, client) -> {};
        MethodHandlers methodHandlers = new MethodHandlers("path").addMethod(PUT, handler).addMethod(POST, handler);

        RestHandler found = methodHandlers.getHandler(PUT);
        assertThat(found, sameInstance(handler));

        found = methodHandlers.getHandler(POST);
        assertThat(found, sameInstance(handler));
    }

    public void testExceptionOnOverride() {
        RestHandler handler = (request, channel, client) -> {};
        MethodHandlers methodHandlers = new MethodHandlers("path").addMethod(PUT, handler);

        expectThrows(IllegalArgumentException.class, () -> methodHandlers.addMethod(PUT, handler));
    }
}
