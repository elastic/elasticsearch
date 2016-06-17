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
package org.elasticsearch.http;

import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.bytes.ByteBufferBytesReference;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.BoundTransportAddress;
import org.elasticsearch.common.transport.LocalTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.indices.breaker.HierarchyCircuitBreakerService;
import org.elasticsearch.node.service.NodeService;
import org.elasticsearch.rest.AbstractRestChannel;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Map;

public class HttpServerTests extends ESTestCase {
    private static final ByteSizeValue BREAKER_LIMIT = new ByteSizeValue(20);
    private HttpServer httpServer;
    private CircuitBreaker inFlightRequestsBreaker;

    @Before
    public void setup() {
        Settings settings = Settings.EMPTY;
        CircuitBreakerService circuitBreakerService = new HierarchyCircuitBreakerService(
            Settings.builder()
                .put(HierarchyCircuitBreakerService.IN_FLIGHT_REQUESTS_CIRCUIT_BREAKER_LIMIT_SETTING.getKey(), BREAKER_LIMIT)
                .build(),
            new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS));
        // we can do this here only because we know that we don't adjust breaker settings dynamically in the test
        inFlightRequestsBreaker = circuitBreakerService.getBreaker(CircuitBreaker.IN_FLIGHT_REQUESTS);

        HttpServerTransport httpServerTransport = new TestHttpServerTransport();
        RestController restController = new RestController(settings);
        restController.registerHandler(RestRequest.Method.GET, "/",
            (request, channel) -> channel.sendResponse(
                new BytesRestResponse(RestStatus.OK, BytesRestResponse.TEXT_CONTENT_TYPE, BytesArray.EMPTY)));
        restController.registerHandler(RestRequest.Method.GET, "/error", (request, channel) -> {
                throw new IllegalArgumentException("test error");
            });

        ClusterService clusterService = new ClusterService(Settings.EMPTY,
            new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS), null);
        NodeService nodeService = new NodeService(Settings.EMPTY, null, null, null, null, null, null, null, null, null,
            clusterService, null);
        httpServer = new HttpServer(settings, httpServerTransport, restController, nodeService, circuitBreakerService);
        httpServer.start();
    }

    public void testDispatchRequestAddsAndFreesBytesOnSuccess() {
        int contentLength = BREAKER_LIMIT.bytesAsInt();
        String content = randomAsciiOfLength(contentLength);
        TestRestRequest request = new TestRestRequest("/", content);
        AssertingChannel channel = new AssertingChannel(request, true, RestStatus.OK);

        httpServer.dispatchRequest(request, channel, new ThreadContext(Settings.EMPTY));

        assertEquals(0, inFlightRequestsBreaker.getTrippedCount());
        assertEquals(0, inFlightRequestsBreaker.getUsed());
    }

    public void testDispatchRequestAddsAndFreesBytesOnError() {
        int contentLength = BREAKER_LIMIT.bytesAsInt();
        String content = randomAsciiOfLength(contentLength);
        TestRestRequest request = new TestRestRequest("/error", content);
        AssertingChannel channel = new AssertingChannel(request, true, RestStatus.BAD_REQUEST);

        httpServer.dispatchRequest(request, channel, new ThreadContext(Settings.EMPTY));

        assertEquals(0, inFlightRequestsBreaker.getTrippedCount());
        assertEquals(0, inFlightRequestsBreaker.getUsed());
    }

    public void testDispatchRequestAddsAndFreesBytesOnlyOnceOnError() {
        int contentLength = BREAKER_LIMIT.bytesAsInt();
        String content = randomAsciiOfLength(contentLength);
        // we will produce an error in the rest handler and one more when sending the error response
        TestRestRequest request = new TestRestRequest("/error", content);
        ExceptionThrowingChannel channel = new ExceptionThrowingChannel(request, true);

        httpServer.dispatchRequest(request, channel, new ThreadContext(Settings.EMPTY));

        assertEquals(0, inFlightRequestsBreaker.getTrippedCount());
        assertEquals(0, inFlightRequestsBreaker.getUsed());
    }

    public void testDispatchRequestLimitsBytes() {
        int contentLength = BREAKER_LIMIT.bytesAsInt() + 1;
        String content = randomAsciiOfLength(contentLength);
        TestRestRequest request = new TestRestRequest("/", content);
        AssertingChannel channel = new AssertingChannel(request, true, RestStatus.SERVICE_UNAVAILABLE);

        httpServer.dispatchRequest(request, channel, new ThreadContext(Settings.EMPTY));

        assertEquals(1, inFlightRequestsBreaker.getTrippedCount());
        assertEquals(0, inFlightRequestsBreaker.getUsed());
    }

    private static final class TestHttpServerTransport extends AbstractLifecycleComponent<HttpServerTransport> implements
        HttpServerTransport {

        public TestHttpServerTransport() {
            super(Settings.EMPTY);
        }

        @Override
        protected void doStart() {
        }

        @Override
        protected void doStop() {
        }

        @Override
        protected void doClose() {
        }

        @Override
        public BoundTransportAddress boundAddress() {
            LocalTransportAddress transportAddress = new LocalTransportAddress("1");
            return new BoundTransportAddress(new TransportAddress[] {transportAddress} ,transportAddress);
        }

        @Override
        public HttpInfo info() {
            return null;
        }

        @Override
        public HttpStats stats() {
            return null;
        }

        @Override
        public void httpServerAdapter(HttpServerAdapter httpServerAdapter) {

        }
    }

    private static final class AssertingChannel extends AbstractRestChannel {
        private final RestStatus expectedStatus;

        protected AssertingChannel(RestRequest request, boolean detailedErrorsEnabled, RestStatus expectedStatus) {
            super(request, detailedErrorsEnabled);
            this.expectedStatus = expectedStatus;
        }

        @Override
        public void sendResponse(RestResponse response) {
            assertEquals(expectedStatus, response.status());
        }
    }

    private static final class ExceptionThrowingChannel extends AbstractRestChannel {

        protected ExceptionThrowingChannel(RestRequest request, boolean detailedErrorsEnabled) {
            super(request, detailedErrorsEnabled);
        }

        @Override
        public void sendResponse(RestResponse response) {
            throw new IllegalStateException("always throwing an exception for testing");
        }
    }

    private static final class TestRestRequest extends RestRequest {
        private final String path;
        private final BytesReference content;

        private TestRestRequest(String path, String content) {
            this.path = path;
            this.content = new ByteBufferBytesReference(ByteBuffer.wrap(content.getBytes(StandardCharsets.UTF_8)));
        }

        @Override
        public Method method() {
            return Method.GET;
        }

        @Override
        public String uri() {
            return null;
        }

        @Override
        public String rawPath() {
            return path;
        }

        @Override
        public boolean hasContent() {
            return true;
        }

        @Override
        public BytesReference content() {
            return content;
        }

        @Override
        public String header(String name) {
            return null;
        }

        @Override
        public Iterable<Map.Entry<String, String>> headers() {
            return null;
        }

        @Override
        public boolean hasParam(String key) {
            return false;
        }

        @Override
        public String param(String key) {
            return null;
        }

        @Override
        public String param(String key, String defaultValue) {
            return null;
        }

        @Override
        public Map<String, String> params() {
            return null;
        }
    }
}
