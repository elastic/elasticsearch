/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.rest;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.compatibility.RestApiCompatibleVersion;
import org.elasticsearch.test.ESTestCase;

import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.rest.RestRequest.Method.PUT;
import static org.hamcrest.Matchers.sameInstance;

public class MethodHandlersTests extends ESTestCase {

    private final RestApiCompatibleVersion current = RestApiCompatibleVersion.currentVersion();
    private final RestApiCompatibleVersion previous = current.previousMajor();

    public void testLookupForDifferentMethodsSameVersion() {
        RestHandler putHandler = new CurrentVersionHandler();
        RestHandler postHandler = new CurrentVersionHandler();
        MethodHandlers methodHandlers = new MethodHandlers("path")
            .addMethod(PUT, putHandler)
            .addMethod(POST, postHandler);

        RestHandler found = methodHandlers.getHandler(PUT, current);
        assertThat(found, sameInstance(putHandler));
    }

    public void testLookupForHandlerUnderMultipleMethods() {
        RestHandler handler = new CurrentVersionHandler();
        MethodHandlers methodHandlers = new MethodHandlers("path")
            .addMethod(PUT, handler)
            .addMethod(POST, handler);

        RestHandler found = methodHandlers.getHandler(PUT, current);
        assertThat(found, sameInstance(handler));

        found = methodHandlers.getHandler(POST, current);
        assertThat(found, sameInstance(handler));
    }

    public void testLookupForHandlersUnderDifferentVersions() {
        RestHandler currentVersionHandler = new CurrentVersionHandler();
        RestHandler previousVersionHandler = new PreviousVersionHandler();
        MethodHandlers methodHandlers = new MethodHandlers("path")
            .addMethod(PUT, currentVersionHandler)
            .addMethod(PUT, previousVersionHandler);

        RestHandler found = methodHandlers.getHandler(PUT, current);
        assertThat(found, sameInstance(currentVersionHandler));

        found = methodHandlers.getHandler(PUT, previous);
        assertThat(found, sameInstance(previousVersionHandler));
    }

    public void testExceptionOnOverride() {
        RestHandler handler = (request, channel, client) -> {};
        MethodHandlers methodHandlers = new MethodHandlers("path")
            .addMethod(PUT, handler);

        expectThrows(IllegalArgumentException.class, () -> methodHandlers.addMethod(PUT, handler));
    }

    public void testMissingCurrentHandler() {
        RestHandler previousVersionHandler = new PreviousVersionHandler();
        MethodHandlers methodHandlers = new MethodHandlers("path")
            .addMethod(PUT, previousVersionHandler)
            .addMethod(POST, previousVersionHandler);

        RestHandler found = methodHandlers.getHandler(PUT, current);
        assertNull(found);
    }

    public void testMissingPriorHandlerReturnsCurrentHandler() {
        RestHandler currentVersionHandler = new CurrentVersionHandler();
        MethodHandlers methodHandlers = new MethodHandlers("path")
            .addMethod(PUT, currentVersionHandler)
            .addMethod(POST, currentVersionHandler);

        RestHandler found = methodHandlers.getHandler(PUT, previous);
        assertThat(found, sameInstance(currentVersionHandler));
    }

    static class CurrentVersionHandler implements RestHandler {

        @Override
        public void handleRequest(RestRequest request, RestChannel channel, NodeClient client) throws Exception {

        }
    }

    static class PreviousVersionHandler implements RestHandler {
        @Override
        public void handleRequest(RestRequest request, RestChannel channel, NodeClient client) throws Exception {
        }

        @Override
        public RestApiCompatibleVersion compatibleWithVersion() {
            return RestApiCompatibleVersion.currentVersion().previousMajor();
        }
    }
}
