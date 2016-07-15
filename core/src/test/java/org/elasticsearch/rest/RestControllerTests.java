/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.rest;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.rest.FakeRestRequest;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class RestControllerTests extends ESTestCase {

    public void testApplyRelevantHeaders() throws Exception {
        final ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        Set<String> headers = new HashSet<>(Arrays.asList("header.1", "header.2"));
        final RestController restController = new RestController(Settings.EMPTY, headers) {
            @Override
            boolean checkRequestParameters(RestRequest request, RestChannel channel) {
                return true;
            }

            @Override
            void executeHandler(RestRequest request, RestChannel channel, NodeClient client) throws Exception {
                assertEquals("true", threadContext.getHeader("header.1"));
                assertEquals("true", threadContext.getHeader("header.2"));
                assertNull(threadContext.getHeader("header.3"));
            }
        };
        threadContext.putHeader("header.3", "true");
        Map<String, String> restHeaders = new HashMap<>();
        restHeaders.put("header.1", "true");
        restHeaders.put("header.2", "true");
        restHeaders.put("header.3", "false");
        restController.dispatchRequest(new FakeRestRequest.Builder().withHeaders(restHeaders).build(), null, null, threadContext);
        assertNull(threadContext.getHeader("header.1"));
        assertNull(threadContext.getHeader("header.2"));
        assertEquals("true", threadContext.getHeader("header.3"));
    }

    public void testCanTripCircuitBreaker() throws Exception {
        RestController controller = new RestController(Settings.EMPTY, Collections.emptySet());
        // trip circuit breaker by default
        controller.registerHandler(RestRequest.Method.GET, "/trip", new FakeRestHandler(true));
        controller.registerHandler(RestRequest.Method.GET, "/do-not-trip", new FakeRestHandler(false));

        assertTrue(controller.canTripCircuitBreaker(new FakeRestRequest.Builder().withPath("/trip").build()));
        // assume trip even on unknown paths
        assertTrue(controller.canTripCircuitBreaker(new FakeRestRequest.Builder().withPath("/unknown-path").build()));
        assertFalse(controller.canTripCircuitBreaker(new FakeRestRequest.Builder().withPath("/do-not-trip").build()));
    }

    public void testRegisterAsDeprecatedHandler() {
        RestController controller = mock(RestController.class);

        RestRequest.Method method = randomFrom(RestRequest.Method.values());
        String path = "/_" + randomAsciiOfLengthBetween(1, 6);
        RestHandler handler = mock(RestHandler.class);
        String deprecationMessage = randomAsciiOfLengthBetween(1, 10);
        DeprecationLogger logger = mock(DeprecationLogger.class);

        // don't want to test everything -- just that it actually wraps the handler
        doCallRealMethod().when(controller).registerAsDeprecatedHandler(method, path, handler, deprecationMessage, logger);

        controller.registerAsDeprecatedHandler(method, path, handler, deprecationMessage, logger);

        verify(controller).registerHandler(eq(method), eq(path), any(DeprecationRestHandler.class));
    }

    public void testRegisterWithDeprecatedHandler() {
        final RestController controller = mock(RestController.class);

        final RestRequest.Method method = randomFrom(RestRequest.Method.values());
        final String path = "/_" + randomAsciiOfLengthBetween(1, 6);
        final RestHandler handler = mock(RestHandler.class);
        final RestRequest.Method deprecatedMethod = randomFrom(RestRequest.Method.values());
        final String deprecatedPath = "/_" + randomAsciiOfLengthBetween(1, 6);
        final DeprecationLogger logger = mock(DeprecationLogger.class);

        final String deprecationMessage = "[" + deprecatedMethod.name() + " " + deprecatedPath + "] is deprecated! Use [" +
            method.name() + " " + path + "] instead.";

        // don't want to test everything -- just that it actually wraps the handlers
        doCallRealMethod().when(controller).registerWithDeprecatedHandler(method, path, handler, deprecatedMethod, deprecatedPath, logger);

        controller.registerWithDeprecatedHandler(method, path, handler, deprecatedMethod, deprecatedPath, logger);

        verify(controller).registerHandler(method, path, handler);
        verify(controller).registerAsDeprecatedHandler(deprecatedMethod, deprecatedPath, handler, deprecationMessage, logger);
    }

    /**
     * Useful for testing with deprecation handler.
     */
    private static class FakeRestHandler implements RestHandler {
        private final boolean canTripCircuitBreaker;

        private FakeRestHandler(boolean canTripCircuitBreaker) {
            this.canTripCircuitBreaker = canTripCircuitBreaker;
        }

        @Override
        public void handleRequest(RestRequest request, RestChannel channel, NodeClient client) throws Exception {
            //no op
        }

        @Override
        public boolean canTripCircuitBreaker() {
            return canTripCircuitBreaker;
        }
    }
}
