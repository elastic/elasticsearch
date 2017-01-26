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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;

import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.path.PathTrie;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.http.HttpTransportSettings;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.common.xcontent.XContentType;

import static org.elasticsearch.rest.RestStatus.BAD_REQUEST;
import static org.elasticsearch.rest.RestStatus.FORBIDDEN;
import static org.elasticsearch.rest.RestStatus.INTERNAL_SERVER_ERROR;
import static org.elasticsearch.rest.RestStatus.NOT_ACCEPTABLE;
import static org.elasticsearch.rest.RestStatus.OK;

public class RestController extends AbstractComponent {
    private final PathTrie<RestHandler> getHandlers = new PathTrie<>(RestUtils.REST_DECODER);
    private final PathTrie<RestHandler> postHandlers = new PathTrie<>(RestUtils.REST_DECODER);
    private final PathTrie<RestHandler> putHandlers = new PathTrie<>(RestUtils.REST_DECODER);
    private final PathTrie<RestHandler> deleteHandlers = new PathTrie<>(RestUtils.REST_DECODER);
    private final PathTrie<RestHandler> headHandlers = new PathTrie<>(RestUtils.REST_DECODER);
    private final PathTrie<RestHandler> optionsHandlers = new PathTrie<>(RestUtils.REST_DECODER);

    private final UnaryOperator<RestHandler> handlerWrapper;

    private final NodeClient client;

    private final CircuitBreakerService circuitBreakerService;

    /** Rest headers that are copied to internal requests made during a rest request. */
    private final Set<String> headersToCopy;

    private final boolean isContentTypeRequired;

    private final DeprecationLogger deprecationLogger;

    public RestController(Settings settings, Set<String> headersToCopy, UnaryOperator<RestHandler> handlerWrapper,
                          NodeClient client, CircuitBreakerService circuitBreakerService) {
        super(settings);
        this.headersToCopy = headersToCopy;
        if (handlerWrapper == null) {
            handlerWrapper = h -> h; // passthrough if no wrapper set
        }
        this.handlerWrapper = handlerWrapper;
        this.client = client;
        this.circuitBreakerService = circuitBreakerService;
        this.isContentTypeRequired = HttpTransportSettings.SETTING_HTTP_CONTENT_TYPE_REQUIRED.get(settings);
        this.deprecationLogger = new DeprecationLogger(logger);
    }

    /**
     * Registers a REST handler to be executed when the provided {@code method} and {@code path} match the request.
     *
     * @param method GET, POST, etc.
     * @param path Path to handle (e.g., "/{index}/{type}/_bulk")
     * @param handler The handler to actually execute
     * @param deprecationMessage The message to log and send as a header in the response
     * @param logger The existing deprecation logger to use
     */
    public void registerAsDeprecatedHandler(RestRequest.Method method, String path, RestHandler handler,
                                            String deprecationMessage, DeprecationLogger logger) {
        assert (handler instanceof DeprecationRestHandler) == false;

        registerHandler(method, path, new DeprecationRestHandler(handler, deprecationMessage, logger));
    }

    /**
     * Registers a REST handler to be executed when the provided {@code method} and {@code path} match the request, or when provided
     * with {@code deprecatedMethod} and {@code deprecatedPath}. Expected usage:
     * <pre><code>
     * // remove deprecation in next major release
     * controller.registerWithDeprecatedHandler(POST, "/_forcemerge", this,
     *                                          POST, "/_optimize", deprecationLogger);
     * controller.registerWithDeprecatedHandler(POST, "/{index}/_forcemerge", this,
     *                                          POST, "/{index}/_optimize", deprecationLogger);
     * </code></pre>
     * <p>
     * The registered REST handler ({@code method} with {@code path}) is a normal REST handler that is not deprecated and it is
     * replacing the deprecated REST handler ({@code deprecatedMethod} with {@code deprecatedPath}) that is using the <em>same</em>
     * {@code handler}.
     * <p>
     * Deprecated REST handlers without a direct replacement should be deprecated directly using {@link #registerAsDeprecatedHandler}
     * and a specific message.
     *
     * @param method GET, POST, etc.
     * @param path Path to handle (e.g., "/_forcemerge")
     * @param handler The handler to actually execute
     * @param deprecatedMethod GET, POST, etc.
     * @param deprecatedPath <em>Deprecated</em> path to handle (e.g., "/_optimize")
     * @param logger The existing deprecation logger to use
     */
    public void registerWithDeprecatedHandler(RestRequest.Method method, String path, RestHandler handler,
                                              RestRequest.Method deprecatedMethod, String deprecatedPath,
                                              DeprecationLogger logger) {
        // e.g., [POST /_optimize] is deprecated! Use [POST /_forcemerge] instead.
        final String deprecationMessage =
            "[" + deprecatedMethod.name() + " " + deprecatedPath + "] is deprecated! Use [" + method.name() + " " + path + "] instead.";

        registerHandler(method, path, handler);
        registerAsDeprecatedHandler(deprecatedMethod, deprecatedPath, handler, deprecationMessage, logger);
    }

    /**
     * Registers a REST handler to be executed when the provided method and path match the request.
     *
     * @param method GET, POST, etc.
     * @param path Path to handle (e.g., "/{index}/{type}/_bulk")
     * @param handler The handler to actually execute
     */
    public void registerHandler(RestRequest.Method method, String path, RestHandler handler) {
        PathTrie<RestHandler> handlers = getHandlersForMethod(method);
        if (handlers != null) {
            handlers.insert(path, handler);
        } else {
            throw new IllegalArgumentException("Can't handle [" + method + "] for path [" + path + "]");
        }
    }

    /**
     * @param request The current request. Must not be null.
     * @return true iff the circuit breaker limit must be enforced for processing this request.
     */
    public boolean canTripCircuitBreaker(RestRequest request) {
        RestHandler handler = getHandler(request);
        return (handler != null) ? handler.canTripCircuitBreaker() : true;
    }

    public void dispatchRequest(RestRequest request, RestChannel channel, ThreadContext threadContext) {
        if (request.rawPath().equals("/favicon.ico")) {
            handleFavicon(request, channel);
            return;
        }
        RestChannel responseChannel = channel;
        try {
            final int contentLength = request.hasContent() ? request.content().length() : 0;
            assert contentLength >= 0 : "content length was negative, how is that possible?";
            final RestHandler handler = getHandler(request);

            if (contentLength == 0 || checkForContentType(request, handler)) {
                if (canTripCircuitBreaker(request)) {
                    inFlightRequestsBreaker(circuitBreakerService).addEstimateBytesAndMaybeBreak(contentLength, "<http_request>");
                } else {
                    inFlightRequestsBreaker(circuitBreakerService).addWithoutBreaking(contentLength);
                }
                // iff we could reserve bytes for the request we need to send the response also over this channel
                responseChannel = new ResourceHandlingHttpChannel(channel, circuitBreakerService, contentLength);
                dispatchRequest(request, responseChannel, client, threadContext, handler);
            } else {
                sendContentTypeErrorMessage(request, responseChannel);
            }
        } catch (Exception e) {
            try {
                responseChannel.sendResponse(new BytesRestResponse(channel, e));
            } catch (Exception inner) {
                inner.addSuppressed(e);
                logger.error((Supplier<?>) () ->
                    new ParameterizedMessage("failed to send failure response for uri [{}]", request.uri()), inner);
            }
        }
    }

    void dispatchRequest(final RestRequest request, final RestChannel channel, final NodeClient client, ThreadContext threadContext,
                         final RestHandler handler) throws Exception {
        if (checkRequestParameters(request, channel) == false) {
            channel.sendResponse(BytesRestResponse.createSimpleErrorResponse(BAD_REQUEST, "error traces in responses are disabled."));
        }
        try (ThreadContext.StoredContext ignored = threadContext.stashContext()) {
            for (String key : headersToCopy) {
                String httpHeader = request.header(key);
                if (httpHeader != null) {
                    threadContext.putHeader(key, httpHeader);
                }
            }

            if (handler == null) {
                if (request.method() == RestRequest.Method.OPTIONS) {
                    // when we have OPTIONS request, simply send OK by default (with the Access Control Origin header which gets automatically added)
                    channel.sendResponse(new BytesRestResponse(OK, BytesRestResponse.TEXT_CONTENT_TYPE, BytesArray.EMPTY));
                } else {
                    final String msg = "No handler found for uri [" + request.uri() + "] and method [" + request.method() + "]";
                    channel.sendResponse(new BytesRestResponse(BAD_REQUEST, msg));
                }
            } else {
                final RestHandler wrappedHandler = Objects.requireNonNull(handlerWrapper.apply(handler));
                wrappedHandler.handleRequest(request, channel, client);
            }
        }
    }

    /**
     * If a request contains content, this method will return {@code true} if the {@code Content-Type} header is present, matches an
     * {@link XContentType} or the request is plain text, and content type is required. If content type is not required then this method
     * returns true unless a content type could not be inferred from the body and the rest handler does not support plain text
     */
    boolean checkForContentType(final RestRequest restRequest, final RestHandler restHandler) {
        if (restRequest.getXContentType() == null) {
            if (restHandler != null && restHandler.supportsPlainText()) {
                // content type of null with a handler that supports plain text gets through for now. Once we remove plain text this can
                // be removed!
                deprecationLogger.deprecated("Plain text request bodies are deprecated. Use request parameters or body " +
                    "in a supported format.");
            } else if (isContentTypeRequired) {
                return false;
            } else {
                deprecationLogger.deprecated("Content type detection for rest requests is deprecated. Specify the content type using" +
                    "the [Content-Type] header.");
                XContentType xContentType = XContentFactory.xContentType(restRequest.content());
                if (xContentType == null) {
                    if (restHandler != null) {
                        if (restHandler.supportsPlainText()) {
                            deprecationLogger.deprecated("Plain text request bodies are deprecated. Use request parameters or body " +
                                "in a supported format.");
                        } else {
                            return false;
                        }
                    }
                } else {
                    restRequest.setXContentType(xContentType);
                }
            }
        }
        return true;
    }

    private void sendContentTypeErrorMessage(RestRequest restRequest, RestChannel channel) throws IOException {
        final List<String> contentTypeHeader = restRequest.getAllHeaderValues("Content-Type");
        final String errorMessage;
        if (contentTypeHeader == null) {
            errorMessage = "Content-Type header is missing";
        } else {
            errorMessage = "Content-Type header [" +
                Strings.collectionToCommaDelimitedString(restRequest.getAllHeaderValues("Content-Type")) + "] is not supported";
        }

        channel.sendResponse(BytesRestResponse.createSimpleErrorResponse(NOT_ACCEPTABLE, errorMessage));
    }

    /**
     * Checks the request parameters against enabled settings for error trace support
     * @return true if the request does not have any parameters that conflict with system settings
     */
    boolean checkRequestParameters(final RestRequest request, final RestChannel channel) {
        // error_trace cannot be used when we disable detailed errors
        // we consume the error_trace parameter first to ensure that it is always consumed
        if (request.paramAsBoolean("error_trace", false) && channel.detailedErrorsEnabled() == false) {
            return false;
        }

        return true;
    }

    private RestHandler getHandler(RestRequest request) {
        String path = getPath(request);
        PathTrie<RestHandler> handlers = getHandlersForMethod(request.method());
        if (handlers != null) {
            return handlers.retrieve(path, request.params());
        } else {
            return null;
        }
    }

    private PathTrie<RestHandler> getHandlersForMethod(RestRequest.Method method) {
        if (method == RestRequest.Method.GET) {
            return getHandlers;
        } else if (method == RestRequest.Method.POST) {
            return postHandlers;
        } else if (method == RestRequest.Method.PUT) {
            return putHandlers;
        } else if (method == RestRequest.Method.DELETE) {
            return deleteHandlers;
        } else if (method == RestRequest.Method.HEAD) {
            return headHandlers;
        } else if (method == RestRequest.Method.OPTIONS) {
            return optionsHandlers;
        } else {
            return null;
        }
    }

    private String getPath(RestRequest request) {
        // we use rawPath since we don't want to decode it while processing the path resolution
        // so we can handle things like:
        // my_index/my_type/http%3A%2F%2Fwww.google.com
        return request.rawPath();
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
        public XContentBuilder newBuilder(@Nullable XContentType xContentType, boolean useFiltering) throws IOException {
            return delegate.newBuilder(xContentType, useFiltering);
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
