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

import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.node.service.NodeService;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.elasticsearch.rest.RestStatus.FORBIDDEN;
import static org.elasticsearch.rest.RestStatus.INTERNAL_SERVER_ERROR;

/**
 * A component to serve http requests, backed by rest handlers.
 */
public class HttpServer extends AbstractLifecycleComponent<HttpServer> implements HttpServerAdapter {
    private final HttpServerTransport transport;

    private final RestController restController;

    private final NodeService nodeService;

    private final CircuitBreakerService circuitBreakerService;

    @Inject
    public HttpServer(Settings settings, HttpServerTransport transport, RestController restController, NodeService nodeService,
                      CircuitBreakerService circuitBreakerService) {
        super(settings);
        this.transport = transport;
        this.restController = restController;
        this.nodeService = nodeService;
        this.circuitBreakerService = circuitBreakerService;
        nodeService.setHttpServer(this);
        transport.httpServerAdapter(this);
    }


    @Override
    protected void doStart() {
        transport.start();
        if (logger.isInfoEnabled()) {
            logger.info("{}", transport.boundAddress());
        }
        nodeService.putAttribute("http_address", transport.boundAddress().publishAddress().toString());
    }

    @Override
    protected void doStop() {
        nodeService.removeAttribute("http_address");
        transport.stop();
    }

    @Override
    protected void doClose() {
        transport.close();
    }

    public HttpInfo info() {
        return transport.info();
    }

    public HttpStats stats() {
        return transport.stats();
    }

    public void dispatchRequest(RestRequest request, RestChannel channel, ThreadContext threadContext) {
        if (request.rawPath().equals("/favicon.ico")) {
            handleFavicon(request, channel);
            return;
        }
        RestChannel responseChannel = channel;
        try {
            int contentLength = request.content().length();
            if (restController.canTripCircuitBreaker(request)) {
                inFlightRequestsBreaker(circuitBreakerService).addEstimateBytesAndMaybeBreak(contentLength, "<http_request>");
            } else {
                inFlightRequestsBreaker(circuitBreakerService).addWithoutBreaking(contentLength);
            }
            // iff we could reserve bytes for the request we need to send the response also over this channel
            responseChannel = new ResourceHandlingHttpChannel(channel, circuitBreakerService, contentLength);
            restController.dispatchRequest(request, responseChannel, threadContext);
        } catch (Throwable t) {
            restController.sendErrorResponse(request, responseChannel, t);
        }
    }

    void handleFavicon(RestRequest request, RestChannel channel) {
        if (request.method() == RestRequest.Method.GET) {
            try {
                try (InputStream stream = getClass().getResourceAsStream("/config/favicon.ico")) {
                    ByteArrayOutputStream out = new ByteArrayOutputStream();
                    Streams.copy(stream, out);
                    BytesRestResponse restResponse = new BytesRestResponse(RestStatus.OK, "image/x-icon", out.toByteArray());
                    channel.sendResponse(restResponse);
                }
            } catch (IOException e) {
                channel.sendResponse(new BytesRestResponse(INTERNAL_SERVER_ERROR, BytesRestResponse.TEXT_CONTENT_TYPE, BytesArray.EMPTY));
            }
        } else {
            channel.sendResponse(new BytesRestResponse(FORBIDDEN, BytesRestResponse.TEXT_CONTENT_TYPE, BytesArray.EMPTY));
        }
    }

    private static final class ResourceHandlingHttpChannel implements RestChannel {
        private final RestChannel delegate;
        private final CircuitBreakerService circuitBreakerService;
        private final int contentLength;
        private final AtomicBoolean closed = new AtomicBoolean();

        public ResourceHandlingHttpChannel(RestChannel delegate, CircuitBreakerService circuitBreakerService, int contentLength) {
            this.delegate = delegate;
            this.circuitBreakerService = circuitBreakerService;
            this.contentLength = contentLength;
        }

        @Override
        public XContentBuilder newBuilder() throws IOException {
            return delegate.newBuilder();
        }

        @Override
        public XContentBuilder newErrorBuilder() throws IOException {
            return delegate.newErrorBuilder();
        }

        @Override
        public XContentBuilder newBuilder(@Nullable BytesReference autoDetectSource, boolean useFiltering) throws IOException {
            return delegate.newBuilder(autoDetectSource, useFiltering);
        }

        @Override
        public BytesStreamOutput bytesOutput() {
            return delegate.bytesOutput();
        }

        @Override
        public RestRequest request() {
            return delegate.request();
        }

        @Override
        public boolean detailedErrorsEnabled() {
            return delegate.detailedErrorsEnabled();
        }

        @Override
        public void sendResponse(RestResponse response) {
            close();
            delegate.sendResponse(response);
        }

        private void close() {
            // attempt to close once atomically
            if (closed.compareAndSet(false, true) == false) {
                throw new IllegalStateException("Channel is already closed");
            }
            inFlightRequestsBreaker(circuitBreakerService).addWithoutBreaking(-contentLength);
        }

    }

    private static CircuitBreaker inFlightRequestsBreaker(CircuitBreakerService circuitBreakerService) {
        // We always obtain a fresh breaker to reflect changes to the breaker configuration.
        return circuitBreakerService.getBreaker(CircuitBreaker.IN_FLIGHT_REQUESTS);
    }
}
