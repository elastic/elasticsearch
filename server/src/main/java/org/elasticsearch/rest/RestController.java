/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.rest;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.path.PathTrie;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.internal.io.Streams;
import org.elasticsearch.http.HttpServerTransport;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.rest.RestHandler.Route;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.usage.UsageService;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;

import static org.elasticsearch.indices.SystemIndices.EXTERNAL_SYSTEM_INDEX_ACCESS_CONTROL_HEADER_KEY;
import static org.elasticsearch.indices.SystemIndices.SYSTEM_INDEX_ACCESS_CONTROL_HEADER_KEY;
import static org.elasticsearch.rest.BytesRestResponse.TEXT_CONTENT_TYPE;
import static org.elasticsearch.rest.RestStatus.BAD_REQUEST;
import static org.elasticsearch.rest.RestStatus.INTERNAL_SERVER_ERROR;
import static org.elasticsearch.rest.RestStatus.METHOD_NOT_ALLOWED;
import static org.elasticsearch.rest.RestStatus.NOT_ACCEPTABLE;
import static org.elasticsearch.rest.RestStatus.OK;

public class RestController implements HttpServerTransport.Dispatcher {

    private static final Logger logger = LogManager.getLogger(RestController.class);
    private static final DeprecationLogger deprecationLogger = DeprecationLogger.getLogger(RestController.class);
    static final String ELASTIC_PRODUCT_HTTP_HEADER = "X-elastic-product";
    static final String ELASTIC_PRODUCT_HTTP_HEADER_VALUE = "Elasticsearch";
    static final Set<String> RESERVED_PATHS = org.elasticsearch.core.Set.of(
        "/__elb_health__",
        "/__elb_health__/zk",
        "/_health",
        "/_health/zk"
    );

    private static final BytesReference FAVICON_RESPONSE;

    static {
        try (InputStream stream = RestController.class.getResourceAsStream("/config/favicon.ico")) {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            Streams.copy(stream, out);
            FAVICON_RESPONSE = new BytesArray(out.toByteArray());
        } catch (IOException e) {
            throw new AssertionError(e);
        }
    }

    private final PathTrie<MethodHandlers> handlers = new PathTrie<>(RestUtils.REST_DECODER);

    private final UnaryOperator<RestHandler> handlerWrapper;

    private final NodeClient client;

    private final CircuitBreakerService circuitBreakerService;

    /** Rest headers that are copied to internal requests made during a rest request. */
    private final Set<RestHeaderDefinition> headersToCopy;
    private final UsageService usageService;

    public RestController(
        Set<RestHeaderDefinition> headersToCopy,
        UnaryOperator<RestHandler> handlerWrapper,
        NodeClient client,
        CircuitBreakerService circuitBreakerService,
        UsageService usageService
    ) {
        this.headersToCopy = headersToCopy;
        this.usageService = usageService;
        if (handlerWrapper == null) {
            handlerWrapper = h -> h; // passthrough if no wrapper set
        }
        this.handlerWrapper = handlerWrapper;
        this.client = client;
        this.circuitBreakerService = circuitBreakerService;
        registerHandlerNoWrap(
            RestRequest.Method.GET,
            "/favicon.ico",
            (request, channel, clnt) -> channel.sendResponse(new BytesRestResponse(RestStatus.OK, "image/x-icon", FAVICON_RESPONSE))
        );
    }

    /**
     * Registers a REST handler to be executed when the provided {@code method} and {@code path} match the request.
     *
     * @param method GET, POST, etc.
     * @param path Path to handle (e.g. "/{index}/{type}/_bulk")
     * @param handler The handler to actually execute
     * @param deprecationMessage The message to log and send as a header in the response
     */
    protected void registerAsDeprecatedHandler(RestRequest.Method method, String path, RestHandler handler, String deprecationMessage) {
        registerAsDeprecatedHandler(method, path, handler, deprecationMessage, null);
    }

    /**
     * Registers a REST handler to be executed when the provided {@code method} and {@code path} match the request.
     *
     * @param method GET, POST, etc.
     * @param path Path to handle (e.g. "/{index}/{type}/_bulk")
     * @param handler The handler to actually execute
     * @param deprecationMessage The message to log and send as a header in the response
     * @param deprecationLevel The deprecation level to use for the message
     */
    protected void registerAsDeprecatedHandler(
        RestRequest.Method method,
        String path,
        RestHandler handler,
        String deprecationMessage,
        @Nullable Level deprecationLevel
    ) {
        assert (handler instanceof DeprecationRestHandler) == false;
        registerHandler(
            method,
            path,
            new DeprecationRestHandler(handler, method, path, deprecationLevel, deprecationMessage, deprecationLogger)
        );
    }

    /**
     * Registers a REST handler to be executed when the provided {@code method} and {@code path} match the request, or when provided
     * with {@code replacedMethod} and {@code replacedPath}. Expected usage:
     * <pre><code>
     * // remove deprecation in next major release
     * controller.registerAsDeprecatedHandler(POST, "/_forcemerge", RestApiVersion.V_8, someHandler,
     *                                        POST, "/_optimize", RestApiVersion.V_7);
     * controller.registerAsDeprecatedHandler(POST, "/{index}/_forcemerge", RestApiVersion.V_8, someHandler,
     *                                        POST, "/{index}/_optimize", RestApiVersion.V_7);
     * </code></pre>
     * <p>
     * The registered REST handler ({@code method} with {@code path}) is a normal REST handler that is not deprecated and it is
     * replacing the deprecated REST handler ({@code replacedMethod} with {@code replacedPath}) that is using the <em>same</em>
     * {@code handler}.
     * <p>
     * Deprecated REST handlers without a direct replacement should be deprecated directly using {@link #registerAsDeprecatedHandler}
     * and a specific message.
     *
     * @param method GET, POST, etc.
     * @param path Path to handle (e.g. "/_forcemerge")
     * @param handler The handler to actually execute
     * @param replacedMethod GET, POST, etc.
     * @param replacedPath <em>Replaced</em> path to handle (e.g. "/_optimize")
     */
    protected void registerAsReplacedHandler(
        RestRequest.Method method,
        String path,
        RestHandler handler,
        RestRequest.Method replacedMethod,
        String replacedPath
    ) {
        // e.g. [POST /_optimize] is deprecated! Use [POST /_forcemerge] instead.
        final String replacedMessage = "["
            + replacedMethod.name()
            + " "
            + replacedPath
            + "] is deprecated! Use ["
            + method.name()
            + " "
            + path
            + "] instead.";

        registerHandler(method, path, handler);
        registerAsDeprecatedHandler(replacedMethod, replacedPath, handler, replacedMessage);
    }

    /**
     * Registers a REST handler to be executed when one of the provided methods and path match the request.
     *
     * @param method GET, POST, etc.
     * @param path Path to handle (e.g. "/{index}/{type}/_bulk")
     * @param handler The handler to actually execute
     */
    protected void registerHandler(RestRequest.Method method, String path, RestHandler handler) {
        if (handler instanceof BaseRestHandler) {
            usageService.addRestHandler((BaseRestHandler) handler);
        }
        registerHandlerNoWrap(method, path, handlerWrapper.apply(handler));
    }

    private void registerHandlerNoWrap(RestRequest.Method method, String path, RestHandler maybeWrappedHandler) {
        if (RESERVED_PATHS.contains(path)) {
            throw new IllegalArgumentException("path [" + path + "] is a reserved path and may not be registered");
        }
        handlers.insertOrUpdate(
            path,
            new MethodHandlers(path).addMethod(method, maybeWrappedHandler),
            (mHandlers, newMHandler) -> mHandlers.addMethod(method, maybeWrappedHandler)
        );
    }

    public void registerHandler(final Route route, final RestHandler handler) {
        if (route.isReplacement()) {
            Route replaced = route.getReplacedRoute();
            registerAsReplacedHandler(route.getMethod(), route.getPath(), handler, replaced.getMethod(), replaced.getPath());
        } else if (route.isDeprecated()) {
            registerAsDeprecatedHandler(
                route.getMethod(),
                route.getPath(),
                handler,
                route.getDeprecationMessage(),
                route.getDeprecationLevel()
            );
        } else {
            // it's just a normal route
            registerHandler(route.getMethod(), route.getPath(), handler);
        }
    }

    /**
     * Registers a REST handler with the controller. The REST handler declares the {@code method}
     * and {@code path} combinations.
     */
    public void registerHandler(final RestHandler handler) {
        handler.routes().forEach(route -> registerHandler(route, handler));
    }

    @Override
    public void dispatchRequest(RestRequest request, RestChannel channel, ThreadContext threadContext) {
        threadContext.addResponseHeader(ELASTIC_PRODUCT_HTTP_HEADER, ELASTIC_PRODUCT_HTTP_HEADER_VALUE);
        try {
            tryAllHandlers(request, channel, threadContext);
        } catch (Exception e) {
            try {
                channel.sendResponse(new BytesRestResponse(channel, e));
            } catch (Exception inner) {
                inner.addSuppressed(e);
                logger.error(() -> new ParameterizedMessage("failed to send failure response for uri [{}]", request.uri()), inner);
            }
        }
    }

    @Override
    public void dispatchBadRequest(final RestChannel channel, final ThreadContext threadContext, final Throwable cause) {
        threadContext.addResponseHeader(ELASTIC_PRODUCT_HTTP_HEADER, ELASTIC_PRODUCT_HTTP_HEADER_VALUE);
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

    private void dispatchRequest(RestRequest request, RestChannel channel, RestHandler handler, ThreadContext threadContext)
        throws Exception {
        final int contentLength = request.contentLength();
        if (contentLength > 0) {
            final XContentType xContentType = request.getXContentType();
            if (xContentType == null) {
                sendContentTypeErrorMessage(request.getAllHeaderValues("Content-Type"), channel);
                return;
            }
            if (handler.supportsContentStream() && xContentType != XContentType.JSON && xContentType != XContentType.SMILE) {
                channel.sendResponse(
                    BytesRestResponse.createSimpleErrorResponse(
                        channel,
                        RestStatus.NOT_ACCEPTABLE,
                        "Content-Type [" + xContentType + "] does not support stream parsing. Use JSON or SMILE instead"
                    )
                );
                return;
            }
        }
        RestChannel responseChannel = channel;
        try {
            if (handler.canTripCircuitBreaker()) {
                inFlightRequestsBreaker(circuitBreakerService).addEstimateBytesAndMaybeBreak(contentLength, "<http_request>");
            } else {
                inFlightRequestsBreaker(circuitBreakerService).addWithoutBreaking(contentLength);
            }
            // iff we could reserve bytes for the request we need to send the response also over this channel
            responseChannel = new ResourceHandlingHttpChannel(channel, circuitBreakerService, contentLength);
            // TODO: Count requests double in the circuit breaker if they need copying?
            if (handler.allowsUnsafeBuffers() == false) {
                request.ensureSafeBuffers();
            }

            if (handler.allowSystemIndexAccessByDefault() == false) {
                // The ELASTIC_PRODUCT_ORIGIN_HTTP_HEADER indicates that the request is coming from an Elastic product and
                // therefore we should allow a subset of external system index access.
                // This header is intended for internal use only.
                final String prodOriginValue = request.header(Task.X_ELASTIC_PRODUCT_ORIGIN_HTTP_HEADER);
                if (prodOriginValue != null) {
                    threadContext.putHeader(SYSTEM_INDEX_ACCESS_CONTROL_HEADER_KEY, Boolean.TRUE.toString());
                    threadContext.putHeader(EXTERNAL_SYSTEM_INDEX_ACCESS_CONTROL_HEADER_KEY, prodOriginValue);
                } else {
                    threadContext.putHeader(SYSTEM_INDEX_ACCESS_CONTROL_HEADER_KEY, Boolean.FALSE.toString());
                }
            } else {
                threadContext.putHeader(SYSTEM_INDEX_ACCESS_CONTROL_HEADER_KEY, Boolean.TRUE.toString());
            }

            handler.handleRequest(request, responseChannel, client);
        } catch (Exception e) {
            responseChannel.sendResponse(new BytesRestResponse(responseChannel, e));
        }
    }

    private boolean handleNoHandlerFound(String rawPath, RestRequest.Method method, String uri, RestChannel channel) {
        // Get the map of matching handlers for a request, for the full set of HTTP methods.
        final Set<RestRequest.Method> validMethodSet = getValidHandlerMethodSet(rawPath);
        if (validMethodSet.contains(method) == false) {
            if (method == RestRequest.Method.OPTIONS) {
                handleOptionsRequest(channel, validMethodSet);
                return true;
            }
            if (validMethodSet.isEmpty() == false) {
                // If an alternative handler for an explicit path is registered to a
                // different HTTP method than the one supplied - return a 405 Method
                // Not Allowed error.
                handleUnsupportedHttpMethod(uri, method, channel, validMethodSet, null);
                return true;
            }
        }
        return false;
    }

    private void sendContentTypeErrorMessage(@Nullable List<String> contentTypeHeader, RestChannel channel) throws IOException {
        final String errorMessage;
        if (contentTypeHeader == null) {
            errorMessage = "Content-Type header is missing";
        } else {
            errorMessage = "Content-Type header [" + Strings.collectionToCommaDelimitedString(contentTypeHeader) + "] is not supported";
        }

        channel.sendResponse(BytesRestResponse.createSimpleErrorResponse(channel, NOT_ACCEPTABLE, errorMessage));
    }

    private void tryAllHandlers(final RestRequest request, final RestChannel channel, final ThreadContext threadContext) throws Exception {
        try {
            copyRestHeaders(request, threadContext);
            validateErrorTrace(request, channel);
        } catch (IllegalArgumentException e) {
            channel.sendResponse(BytesRestResponse.createSimpleErrorResponse(channel, BAD_REQUEST, e.getMessage()));
            return;
        }

        final String rawPath = request.rawPath();
        final String uri = request.uri();
        final RestRequest.Method requestMethod;
        try {
            // Resolves the HTTP method and fails if the method is invalid
            requestMethod = request.method();
            // Loop through all possible handlers, attempting to dispatch the request
            Iterator<MethodHandlers> allHandlers = getAllHandlers(request.params(), rawPath);
            while (allHandlers.hasNext()) {
                final RestHandler handler;
                final MethodHandlers handlers = allHandlers.next();
                if (handlers == null) {
                    handler = null;
                } else {
                    handler = handlers.getHandler(requestMethod);
                }
                if (handler == null) {
                    if (handleNoHandlerFound(rawPath, requestMethod, uri, channel)) {
                        return;
                    }
                } else {
                    dispatchRequest(request, channel, handler, threadContext);
                    return;
                }
            }
        } catch (final IllegalArgumentException e) {
            handleUnsupportedHttpMethod(uri, null, channel, getValidHandlerMethodSet(rawPath), e);
            return;
        }
        // If request has not been handled, fallback to a bad request error.
        handleBadRequest(uri, requestMethod, channel);
    }

    private void validateErrorTrace(RestRequest request, RestChannel channel) {
        // error_trace cannot be used when we disable detailed errors
        // we consume the error_trace parameter first to ensure that it is always consumed
        if (request.paramAsBoolean("error_trace", false) && channel.detailedErrorsEnabled() == false) {
            throw new IllegalArgumentException("error traces in responses are disabled.");
        }
    }

    private void copyRestHeaders(RestRequest request, ThreadContext threadContext) throws IOException {
        for (final RestHeaderDefinition restHeader : headersToCopy) {
            final String name = restHeader.getName();
            final List<String> headerValues = request.getAllHeaderValues(name);
            if (headerValues != null && headerValues.isEmpty() == false) {
                final List<String> distinctHeaderValues = headerValues.stream().distinct().collect(Collectors.toList());
                if (restHeader.isMultiValueAllowed() == false && distinctHeaderValues.size() > 1) {
                    throw new IllegalArgumentException("multiple values for single-valued header [" + name + "].");
                } else if (name.equals(Task.TRACE_PARENT_HTTP_HEADER)) {
                    String traceparent = distinctHeaderValues.get(0);
                    Optional<String> traceId = RestUtils.extractTraceId(traceparent);
                    if (traceId.isPresent()) {
                        threadContext.putHeader(Task.TRACE_ID, traceId.get());
                    }
                } else {
                    threadContext.putHeader(name, String.join(",", distinctHeaderValues));
                }
            }
        }
    }

    Iterator<MethodHandlers> getAllHandlers(@Nullable Map<String, String> requestParamsRef, String rawPath) {
        final Supplier<Map<String, String>> paramsSupplier;
        if (requestParamsRef == null) {
            paramsSupplier = () -> null;
        } else {
            // Between retrieving the correct path, we need to reset the parameters,
            // otherwise parameters are parsed out of the URI that aren't actually handled.
            final Map<String, String> originalParams = new HashMap<>(requestParamsRef);
            paramsSupplier = () -> {
                // PathTrie modifies the request, so reset the params between each iteration
                requestParamsRef.clear();
                requestParamsRef.putAll(originalParams);
                return requestParamsRef;
            };
        }
        // we use rawPath since we don't want to decode it while processing the path resolution
        // so we can handle things like:
        // my_index/my_type/http%3A%2F%2Fwww.google.com
        return handlers.retrieveAll(rawPath, paramsSupplier);
    }

    /**
     * Handle requests to a valid REST endpoint using an unsupported HTTP
     * method. A 405 HTTP response code is returned, and the response 'Allow'
     * header includes a list of valid HTTP methods for the endpoint (see
     * <a href="https://tools.ietf.org/html/rfc2616#section-10.4.6">HTTP/1.1 -
     * 10.4.6 - 405 Method Not Allowed</a>).
     */
    private void handleUnsupportedHttpMethod(
        String uri,
        @Nullable RestRequest.Method method,
        final RestChannel channel,
        final Set<RestRequest.Method> validMethodSet,
        @Nullable final IllegalArgumentException exception
    ) {
        try {
            final StringBuilder msg = new StringBuilder();
            if (exception == null) {
                msg.append("Incorrect HTTP method for uri [").append(uri);
                msg.append("] and method [").append(method).append("]");
            } else {
                msg.append(exception.getMessage());
            }
            if (validMethodSet.isEmpty() == false) {
                msg.append(", allowed: ").append(validMethodSet);
            }
            BytesRestResponse bytesRestResponse = BytesRestResponse.createSimpleErrorResponse(channel, METHOD_NOT_ALLOWED, msg.toString());
            if (validMethodSet.isEmpty() == false) {
                bytesRestResponse.addHeader("Allow", Strings.collectionToDelimitedString(validMethodSet, ","));
            }
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
    private void handleOptionsRequest(RestChannel channel, Set<RestRequest.Method> validMethodSet) {
        BytesRestResponse bytesRestResponse = new BytesRestResponse(OK, TEXT_CONTENT_TYPE, BytesArray.EMPTY);
        // When we have an OPTIONS HTTP request and no valid handlers, simply send OK by default (with the Access Control Origin header
        // which gets automatically added).
        if (validMethodSet.isEmpty() == false) {
            bytesRestResponse.addHeader("Allow", Strings.collectionToDelimitedString(validMethodSet, ","));
        }
        channel.sendResponse(bytesRestResponse);
    }

    /**
     * Handle a requests with no candidate handlers (return a 400 Bad Request
     * error).
     */
    private void handleBadRequest(String uri, RestRequest.Method method, RestChannel channel) throws IOException {
        try (XContentBuilder builder = channel.newErrorBuilder()) {
            builder.startObject();
            {
                builder.field("error", "no handler found for uri [" + uri + "] and method [" + method + "]");
            }
            builder.endObject();
            channel.sendResponse(new BytesRestResponse(BAD_REQUEST, builder));
        }
    }

    /**
     * Get the valid set of HTTP methods for a REST request.
     */
    private Set<RestRequest.Method> getValidHandlerMethodSet(String rawPath) {
        Set<RestRequest.Method> validMethods = new HashSet<>();
        Iterator<MethodHandlers> allHandlers = getAllHandlers(null, rawPath);
        while (allHandlers.hasNext()) {
            final MethodHandlers methodHandlers = allHandlers.next();
            if (methodHandlers != null) {
                validMethods.addAll(methodHandlers.getValidMethods());
            }
        }
        return validMethods;
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
        public XContentBuilder newBuilder(XContentType xContentType, XContentType responseContentType, boolean useFiltering)
            throws IOException {
            return delegate.newBuilder(xContentType, responseContentType, useFiltering);
        }

        @Override
        public BytesStreamOutput bytesOutput() {
            return delegate.bytesOutput();
        }

        @Override
        public void releaseOutputBuffer() {
            delegate.releaseOutputBuffer();
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
            boolean success = false;
            try {
                close();
                delegate.sendResponse(response);
                success = true;
            } finally {
                if (success == false) {
                    releaseOutputBuffer();
                }
            }
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
