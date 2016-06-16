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

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.rest.FakeRestRequest;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.CoreMatchers.equalTo;

public class RestControllerTests extends ESTestCase {

    public void testRegisterRelevantHeaders() throws InterruptedException {

        final RestController restController = new RestController(Settings.EMPTY);

        int iterations = randomIntBetween(1, 5);

        Set<String> headers = new HashSet<>();
        ExecutorService executorService = Executors.newFixedThreadPool(iterations);
        for (int i = 0; i < iterations; i++) {
            int headersCount = randomInt(10);
            final Set<String> newHeaders = new HashSet<>();
            for (int j = 0; j < headersCount; j++) {
                String usefulHeader = randomRealisticUnicodeOfLengthBetween(1, 30);
                newHeaders.add(usefulHeader);
            }
            headers.addAll(newHeaders);

            executorService.submit((Runnable) () -> restController.registerRelevantHeaders(newHeaders.toArray(new String[newHeaders.size()])));
        }

        executorService.shutdown();
        assertThat(executorService.awaitTermination(1, TimeUnit.SECONDS), equalTo(true));
        String[] relevantHeaders = restController.relevantHeaders().toArray(new String[restController.relevantHeaders().size()]);
        assertThat(relevantHeaders.length, equalTo(headers.size()));

        Arrays.sort(relevantHeaders);
        String[] headersArray = new String[headers.size()];
        headersArray = headers.toArray(headersArray);
        Arrays.sort(headersArray);
        assertThat(relevantHeaders, equalTo(headersArray));
    }

    public void testApplyRelevantHeaders() throws Exception {
        final ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        final RestController restController = new RestController(Settings.EMPTY) {
            @Override
            boolean checkRequestParameters(RestRequest request, RestChannel channel) {
                return true;
            }

            @Override
            void executeHandler(RestRequest request, RestChannel channel) throws Exception {
                assertEquals("true", threadContext.getHeader("header.1"));
                assertEquals("true", threadContext.getHeader("header.2"));
                assertNull(threadContext.getHeader("header.3"));

            }
        };
        threadContext.putHeader("header.3", "true");
        restController.registerRelevantHeaders("header.1", "header.2");
        Map<String, String> restHeaders = new HashMap<>();
        restHeaders.put("header.1", "true");
        restHeaders.put("header.2", "true");
        restHeaders.put("header.3", "false");
        restController.dispatchRequest(new FakeRestRequest.Builder().withHeaders(restHeaders).build(), null, threadContext);
        assertNull(threadContext.getHeader("header.1"));
        assertNull(threadContext.getHeader("header.2"));
        assertEquals("true", threadContext.getHeader("header.3"));
    }

    public void testCanTripCircuitBreaker() throws Exception {
        RestController controller = new RestController(Settings.EMPTY);
        // trip circuit breaker by default
        controller.registerHandler(RestRequest.Method.GET, "/trip", new FakeRestHandler(true));
        controller.registerHandler(RestRequest.Method.GET, "/do-not-trip", new FakeRestHandler(false));

        assertTrue(controller.canTripCircuitBreaker(new FakeRestRequest.Builder().withPath("/trip").build()));
        // assume trip even on unknown paths
        assertTrue(controller.canTripCircuitBreaker(new FakeRestRequest.Builder().withPath("/unknown-path").build()));
        assertFalse(controller.canTripCircuitBreaker(new FakeRestRequest.Builder().withPath("/do-not-trip").build()));
    }

    private static class FakeRestHandler implements RestHandler {
        private final boolean canTripCircuitBreaker;

        private FakeRestHandler(boolean canTripCircuitBreaker) {
            this.canTripCircuitBreaker = canTripCircuitBreaker;
        }

        @Override
        public void handleRequest(RestRequest request, RestChannel channel) throws Exception {
            //no op
        }

        @Override
        public boolean canTripCircuitBreaker() {
            return canTripCircuitBreaker;
        }
    }
}
