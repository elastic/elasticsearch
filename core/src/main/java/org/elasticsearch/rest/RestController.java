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

import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.ElasticsearchException;
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
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.http.HttpServerTransport;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.usage.UsageService;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;

import static org.elasticsearch.rest.RestStatus.BAD_REQUEST;
import static org.elasticsearch.rest.RestStatus.METHOD_NOT_ALLOWED;
import static org.elasticsearch.rest.RestStatus.FORBIDDEN;
import static org.elasticsearch.rest.RestStatus.INTERNAL_SERVER_ERROR;
import static org.elasticsearch.rest.RestStatus.NOT_ACCEPTABLE;
import static org.elasticsearch.rest.RestStatus.OK;
import static org.elasticsearch.rest.BytesRestResponse.TEXT_CONTENT_TYPE;

public class RestController extends AbstractComponent implements HttpServerTransport.Dispatcher {

    private final PathTrie<MethodHandlers> handlers = new PathTrie<>(RestUtils.REST_DECODER);

    private final UnaryOperator<RestHandler> handlerWrapper;

    private final NodeClient client;

    private final CircuitBreakerService circuitBreakerService;

    /** Rest headers that are copied to internal requests made during a rest request. */
    private final Set<String> headersToCopy;
    private UsageService usageService;

    public RestController(Settings settings, Set<String> headersToCopy, UnaryOperator<RestHandler> handlerWrapper,
            NodeClient client, CircuitBreakerService circuitBreakerService, UsageService usageService) {
        super(settings);
        this.headersToCopy = headersToCopy;
        this.usageService = usageService;
        if (handlerWrapper == null) {
            handlerWrapper = h -> h; // passthrough if no wrapper set
        }
        this.handlerWrapper = handlerWrapper;
        this.client = client;
        this.circuitBreakerService = circuitBreakerService;
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
     * Registers a REST handler to be executed when one of the provided methods and path match the request.
     *
     * @param path Path to handle (e.g., "/{index}/{type}/_bulk")
     * @param handler The handler to actually execute
     * @param method GET, POST, etc.
     */
    public void registerHandler(RestRequest.Method method, String path, RestHandler handler) {
        if (handler instanceof BaseRestHandler) {
            usageService.addRestHandler((BaseRestHandler) handler);
        }
        handlers.insertOrUpdate(path, new MethodHandlers(path, handler, method), (mHandlers, newMHandler) -> {
            return mHandlers.addMethods(handler, method);
        });
    }

    /**
     * @return true iff the circuit breaker limit must be enforced for processing this request.
     */
    public boolean canTripCircuitBreaker(final Optional<RestHandler> handler) {
        return handler.map(h -> h.canTripCircuitBreaker()).orElse(true);
    }

    @Override
    public void dispatchRequest(RestRequest request, RestChannel channel, ThreadContext threadContext) {
        if (request.rawPath().equals("/favicon.ico")) {
            handleFavicon(request, channel);
            return;
        }
        try {
            tryAllHandlers(request, channel, threadContext);
        } catch (Exception e) {
            try {
                channel.sendResponse(new BytesRestResponse(channel, e));
            } catch (Exception inner) {
                inner.addSuppressed(e);
                logger.error((Supplier<?>) () ->
                    new ParameterizedMessage("failed to send failure response for uri [{}]", request.uri()), inner);
            }
        }
    }

    @Override
    public void dispatchBadRequest(final RestRequest request, final RestChannel channel,
                                   final ThreadContext threadContext, final Throwable cause) {
        try {
            final Exception e;
            if (cause == null) {
                e = new ElasticsearchException("unknown cause");
            } else if (cause instanceof Exception) {
                e = (Exception) cause;
            } else {
                e = new ElasticsearchException(cause);
            }
            channel.sendResponse(new BytesRestResponse(channel, BAD_REQUEST, e));
        } catch (final IOException e) {
            if (cause != null) {
                e.addSuppressed(cause);
            }
            logger.warn("failed to send bad request response", e);
            channel.sendResponse(new BytesRestResponse(INTERNAL_SERVER_ERROR, BytesRestResponse.TEXT_CONTENT_TYPE, BytesArray.EMPTY));
        }
    }

    /**
     * Dispatch the request, if possible, returning true if a response was sent or false otherwise.
     */
    boolean dispatchRequest(final RestRequest request, final RestChannel channel, final NodeClient client,
                            final Optional<RestHandler> mHandler) throws Exception {
        final int contentLength = request.hasContent() ? request.content().length() : 0;

        RestChannel responseChannel = channel;
        // Indicator of whether a response was sent or not
        boolean requestHandled;

        if (contentLength > 0 && mHandler.map(h -> hasContentType(request, h) == false).orElse(false)) {
            sendContentTypeErrorMessage(request, channel);
            requestHandled = true;
        } else if (contentLength > 0 && mHandler.map(h -> h.supportsContentStream()).orElse(false) &&
            request.getXContentType() != XContentType.JSON && request.getXContentType() != XContentType.SMILE) {
            channel.sendResponse(BytesRestResponse.createSimpleErrorResponse(channel,
                RestStatus.NOT_ACCEPTABLE, "Content-Type [" + request.getXContentType() +
                    "] does not support stream parsing. Use JSON or SMILE instead"));
            requestHandled = true;
        } else if (mHandler.isPresent()) {

            try {
                if (canTripCircuitBreaker(mHandler)) {
                    inFlightRequestsBreaker(circuitBreakerService).addEstimateBytesAndMaybeBreak(contentLength, "<http_request>");
                } else {
                    inFlightRequestsBreaker(circuitBreakerService).addWithoutBreaking(contentLength);
                }
                // iff we could reserve bytes for the request we need to send the response also over this channel
                responseChannel = new ResourceHandlingHttpChannel(channel, circuitBreakerService, contentLength);

                final RestHandler wrappedHandler = mHandler.map(h -> handlerWrapper.apply(h)).get();
                wrappedHandler.handleRequest(request, responseChannel, client);
                requestHandled = true;
            } catch (Exception e) {
                responseChannel.sendResponse(new BytesRestResponse(responseChannel, e));
                // We "handled" the request by returning a response, even though it was an error
                requestHandled = true;
            }
        } else {
            // Get the map of matching handlers for a request, for the full set of HTTP methods.
            final Set<RestRequest.Method> validMethodSet = getValidHandlerMethodSet(request);
            if (validMethodSet.size() > 0
                && validMethodSet.contains(request.method()) == false
                && request.method() != RestRequest.Method.OPTIONS) {
                // If an alternative handler for an explicit path is registered to a
                // different HTTP method than the one supplied - return a 405 Method
                // Not Allowed error.
                handleUnsupportedHttpMethod(request, channel, validMethodSet);
                requestHandled = true;
            } else if (validMethodSet.contains(request.method()) == false
                && (request.method() == RestRequest.Method.OPTIONS)) {
                handleOptionsRequest(request, channel, validMethodSet);
                requestHandled = true;
            } else {
                requestHandled = false;
            }
        }
        // Return true if the request was handled, false otherwise.
        return requestHandled;
    }

    /**
     * If a request contains content, this method will return {@code true} if the {@code Content-Type} header is present, matches an
     * {@link XContentType} or the handler supports a content stream and the content type header is for newline delimited JSON,
     */
    private static boolean hasContentType(final RestRequest restRequest, final RestHandler restHandler) {
        if (restRequest.getXContentType() == null) {
            if (restHandler.supportsContentStream() && restRequest.header("Content-Type") != null) {
                final String lowercaseMediaType = restRequest.header("Content-Type").toLowerCase(Locale.ROOT);
                // we also support newline delimited JSON: http://specs.okfnlabs.org/ndjson/
                if (lowercaseMediaType.equals("application/x-ndjson")) {
                    restRequest.setXContentType(XContentType.JSON);
                    return true;
                }
            }
            return false;
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

        channel.sendResponse(BytesRestResponse.createSimpleErrorResponse(channel, NOT_ACCEPTABLE, errorMessage));
    }

    /**
     * Checks the request parameters against enabled settings for error trace support
     * @return true if the request does not have any parameters that conflict with system settings
     */
    boolean checkErrorTraceParameter(final RestRequest request, final RestChannel channel) {
        // error_trace cannot be used when we disable detailed errors
        // we consume the error_trace parameter first to ensure that it is always consumed
        if (request.paramAsBoolean("error_trace", false) && channel.detailedErrorsEnabled() == false) {
            return false;
        }

        return true;
    }

    void tryAllHandlers(final RestRequest request, final RestChannel channel, final ThreadContext threadContext) throws Exception {
        for (String key : headersToCopy) {
            String httpHeader = request.header(key);
            if (httpHeader != null) {
                threadContext.putHeader(key, httpHeader);
            }
        }
        // Request execution flag
        boolean requestHandled = false;

        if (checkErrorTraceParameter(request, channel) == false) {
            channel.sendResponse(
                    BytesRestResponse.createSimpleErrorResponse(channel, BAD_REQUEST, "error traces in responses are disabled."));
            return;
        }

        // Loop through all possible handlers, attempting to dispatch the request
        Iterator<MethodHandlers> allHandlers = getAllHandlers(request);
        for (Iterator<MethodHandlers> it = allHandlers; it.hasNext(); ) {
            final Optional<RestHandler> mHandler = Optional.ofNullable(it.next()).flatMap(mh -> mh.getHandler(request.method()));
            requestHandled = dispatchRequest(request, channel, client, mHandler);
            if (requestHandled) {
                break;
            }
        }

        // If request has not been handled, fallback to a bad request error.
        if (requestHandled == false) {
            handleBadRequest(request, channel);
        }
    }

    Iterator<MethodHandlers> getAllHandlers(final RestRequest request) {
        // Between retrieving the correct path, we need to reset the parameters,
        // otherwise parameters are parsed out of the URI that aren't actually handled.
        final Map<String, String> originalParams = new HashMap<>(request.params());
        return handlers.retrieveAll(getPath(request), () -> {
            // PathTrie modifies the request, so reset the params between each iteration
            request.params().clear();
            request.params().putAll(originalParams);
            return request.params();
        });
    }

    /**
     * Handle requests to a valid REST endpoint using an unsupported HTTP
     * method. A 405 HTTP response code is returned, and the response 'Allow'
     * header includes a list of valid HTTP methods for the endpoint (see
     * <a href="https://tools.ietf.org/html/rfc2616#section-10.4.6">HTTP/1.1 -
     * 10.4.6 - 405 Method Not Allowed</a>).
     */
    private void handleUnsupportedHttpMethod(RestRequest request, RestChannel channel, Set<RestRequest.Method> validMethodSet) {
        try {
            BytesRestResponse bytesRestResponse = BytesRestResponse.createSimpleErrorResponse(channel, METHOD_NOT_ALLOWED,
                "Incorrect HTTP method for uri [" + request.uri() + "] and method [" + request.method() + "], allowed: " + validMethodSet);
            bytesRestResponse.addHeader("Allow", Strings.collectionToDelimitedString(validMethodSet, ","));
            channel.sendResponse(bytesRestResponse);
        } catch (final IOException e) {
            logger.warn("failed to send bad request response", e);
            channel.sendResponse(new BytesRestResponse(INTERNAL_SERVER_ERROR, BytesRestResponse.TEXT_CONTENT_TYPE, BytesArray.EMPTY));
        }
    }

    /**
     * Handle HTTP OPTIONS requests to a valid REST endpoint. A 200 HTTP
     * response code is returned, and the response 'Allow' header includes a
     * list of valid HTTP methods for the endpoint (see
     * <a href="https://tools.ietf.org/html/rfc2616#section-9.2">HTTP/1.1 - 9.2
     * - Options</a>).
     */
    private void handleOptionsRequest(RestRequest request, RestChannel channel, Set<RestRequest.Method> validMethodSet) {
        if (request.method() == RestRequest.Method.OPTIONS && validMethodSet.size() > 0) {
            BytesRestResponse bytesRestResponse = new BytesRestResponse(OK, TEXT_CONTENT_TYPE, BytesArray.EMPTY);
            bytesRestResponse.addHeader("Allow", Strings.collectionToDelimitedString(validMethodSet, ","));
            channel.sendResponse(bytesRestResponse);
        } else if (request.method() == RestRequest.Method.OPTIONS && validMethodSet.size() == 0) {
            /*
             * When we have an OPTIONS HTTP request and no valid handlers,
             * simply send OK by default (with the Access Control Origin header
             * which gets automatically added).
             */
            channel.sendResponse(new BytesRestResponse(OK, TEXT_CONTENT_TYPE, BytesArray.EMPTY));
        }
    }

    /**
     * Handle a requests with no candidate handlers (return a 400 Bad Request
     * error).
     */
    private void handleBadRequest(RestRequest request, RestChannel channel) {
        channel.sendResponse(new BytesRestResponse(BAD_REQUEST,
            "No handler found for uri [" + request.uri() + "] and method [" + request.method() + "]"));
    }

    /**
     * Get the valid set of HTTP methods for a REST request.
     */
    private Set<RestRequest.Method> getValidHandlerMethodSet(RestRequest request) {
        Set<RestRequest.Method> validMethods = new HashSet<>();
        Iterator<MethodHandlers> allHandlers = getAllHandlers(request);
        for (Iterator<MethodHandlers> it = allHandlers; it.hasNext(); ) {
            Optional.ofNullable(it.next()).map(mh -> validMethods.addAll(mh.getValidMethods()));
        }
        return validMethods;
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

        ResourceHandlingHttpChannel(RestChannel delegate, CircuitBreakerService circuitBreakerService, int contentLength) {
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
