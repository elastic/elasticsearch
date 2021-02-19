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

import static org.hamcrest.Matchers.sameInstance;

public class MethodHandlersTests extends ESTestCase {

    public void testLookupForDifferentMethodsSameVersion() {
        RestHandler putHandler = new CurrentVersionHandler();
        RestHandler postHandler = new CurrentVersionHandler();
        MethodHandlers methodHandlers = new MethodHandlers("path", putHandler, RestRequest.Method.PUT);
        methodHandlers.addMethods(postHandler, RestRequest.Method.POST);

        RestHandler handler = methodHandlers.getHandler(RestRequest.Method.PUT, RestApiCompatibleVersion.currentVersion());
        assertThat(handler, sameInstance(putHandler));
    }

    public void testLookupForHandlerUnderMultipleMethods() {
        RestHandler handler = new CurrentVersionHandler();
        MethodHandlers methodHandlers = new MethodHandlers("path", handler, RestRequest.Method.PUT, RestRequest.Method.POST);

        RestHandler handlerFound = methodHandlers.getHandler(RestRequest.Method.PUT, RestApiCompatibleVersion.currentVersion());
        assertThat(handlerFound, sameInstance(handler));

        handlerFound = methodHandlers.getHandler(RestRequest.Method.POST, RestApiCompatibleVersion.currentVersion());
        assertThat(handlerFound, sameInstance(handler));
    }

    public void testLookupForHandlersUnderDifferentVersions() {
        RestHandler currentVersionHandler = new CurrentVersionHandler();
        RestHandler previousVersionHandler = new PreviousVersionHandler();
        MethodHandlers methodHandlers = new MethodHandlers("path", currentVersionHandler, RestRequest.Method.PUT);
        methodHandlers.addMethods(previousVersionHandler, RestRequest.Method.PUT);

        RestHandler handler = methodHandlers.getHandler(RestRequest.Method.PUT, RestApiCompatibleVersion.currentVersion());
        assertThat(handler, sameInstance(currentVersionHandler));

        handler = methodHandlers.getHandler(RestRequest.Method.PUT, RestApiCompatibleVersion.currentVersion().previousMajor());
        assertThat(handler, sameInstance(previousVersionHandler));
    }

    public void testExceptionOnOverride() {
        RestHandler currentVersionHandler = new CurrentVersionHandler();

        MethodHandlers methodHandlers = new MethodHandlers("path", currentVersionHandler, RestRequest.Method.PUT);
        expectThrows(IllegalArgumentException.class, () -> methodHandlers.addMethods(currentVersionHandler, RestRequest.Method.PUT));
    }

    public void testMissingCurrentHandler(){
        RestHandler previousVersionHandler = new PreviousVersionHandler();
        MethodHandlers methodHandlers = new MethodHandlers("path", previousVersionHandler, RestRequest.Method.PUT, RestRequest.Method.POST);
        RestHandler handler = methodHandlers.getHandler(RestRequest.Method.PUT, RestApiCompatibleVersion.currentVersion());
        assertNull(handler);
    }

    public void testMissingPriorHandlerReturnsCurrentHandler(){
        RestHandler currentVersionHandler = new CurrentVersionHandler();
        MethodHandlers methodHandlers = new MethodHandlers("path", currentVersionHandler, RestRequest.Method.PUT, RestRequest.Method.POST);
        RestHandler handler = methodHandlers.getHandler(RestRequest.Method.PUT, RestApiCompatibleVersion.currentVersion().previousMajor());
        assertThat(handler, sameInstance(currentVersionHandler));
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
