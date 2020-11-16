/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.rest;

import org.elasticsearch.Version;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.sameInstance;

public class MethodHandlersTests extends ESTestCase {

    public void testLookupForDifferentMethodsSameVersion() {
        RestHandler putHandler = new CurrentVersionHandler();
        RestHandler postHandler = new CurrentVersionHandler();
        MethodHandlers methodHandlers = new MethodHandlers("path", putHandler, RestRequest.Method.PUT);
        methodHandlers.addMethods(postHandler, RestRequest.Method.POST);

        RestHandler handler = methodHandlers.getHandler(RestRequest.Method.PUT, Version.CURRENT);
        assertThat(handler, sameInstance(putHandler));
    }

    public void testLookupForHandlerUnderMultipleMethods() {
        RestHandler handler = new CurrentVersionHandler();
        MethodHandlers methodHandlers = new MethodHandlers("path", handler, RestRequest.Method.PUT, RestRequest.Method.POST);

        RestHandler handlerFound = methodHandlers.getHandler(RestRequest.Method.PUT, Version.CURRENT);
        assertThat(handlerFound, sameInstance(handler));

        handlerFound = methodHandlers.getHandler(RestRequest.Method.POST, Version.CURRENT);
        assertThat(handlerFound, sameInstance(handler));
    }

    public void testLookupForHandlersUnderDifferentVersions() {
        RestHandler currentVersionHandler = new CurrentVersionHandler();
        RestHandler previousVersionHandler = new PreviousVersionHandler();
        MethodHandlers methodHandlers = new MethodHandlers("path", currentVersionHandler, RestRequest.Method.PUT);
        methodHandlers.addMethods(previousVersionHandler, RestRequest.Method.PUT);

        RestHandler handler = methodHandlers.getHandler(RestRequest.Method.PUT, Version.CURRENT);
        assertThat(handler, sameInstance(currentVersionHandler));

        handler = methodHandlers.getHandler(RestRequest.Method.PUT, Version.CURRENT.previousMajor());
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
        RestHandler handler = methodHandlers.getHandler(RestRequest.Method.PUT, Version.CURRENT);
        assertNull(handler);
    }

    public void testMissingPriorHandlerReturnsCurrentHandler(){
        RestHandler currentVersionHandler = new CurrentVersionHandler();
        MethodHandlers methodHandlers = new MethodHandlers("path", currentVersionHandler, RestRequest.Method.PUT, RestRequest.Method.POST);
        RestHandler handler = methodHandlers.getHandler(RestRequest.Method.PUT, Version.CURRENT.previousMajor());
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
        public Version compatibleWithVersion() {
            return Version.CURRENT.previousMajor();
        }
    }
}
