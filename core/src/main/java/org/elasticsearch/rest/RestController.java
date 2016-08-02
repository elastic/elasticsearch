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

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.path.PathTrie;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.plugins.ActionPlugin;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.Collections.unmodifiableSet;
import static org.elasticsearch.rest.RestStatus.BAD_REQUEST;
import static org.elasticsearch.rest.RestStatus.OK;

/**
 *
 */
public class RestController extends AbstractLifecycleComponent {
    private final PathTrie<RestHandler> getHandlers = new PathTrie<>(RestUtils.REST_DECODER);
    private final PathTrie<RestHandler> postHandlers = new PathTrie<>(RestUtils.REST_DECODER);
    private final PathTrie<RestHandler> putHandlers = new PathTrie<>(RestUtils.REST_DECODER);
    private final PathTrie<RestHandler> deleteHandlers = new PathTrie<>(RestUtils.REST_DECODER);
    private final PathTrie<RestHandler> headHandlers = new PathTrie<>(RestUtils.REST_DECODER);
    private final PathTrie<RestHandler> optionsHandlers = new PathTrie<>(RestUtils.REST_DECODER);

    private final RestHandlerFilter handlerFilter = new RestHandlerFilter();

    /** Rest headers that are copied to internal requests made during a rest request. */
    private final Set<String> headersToCopy;

    // non volatile since the assumption is that pre processors are registered on startup
    private RestFilter[] filters = new RestFilter[0];

    public RestController(Settings settings, Set<String> headersToCopy) {
        super(settings);
        this.headersToCopy = headersToCopy;
    }

    @Override
    protected void doStart() {
    }

    @Override
    protected void doStop() {
    }

    @Override
    protected void doClose() {
        for (RestFilter filter : filters) {
            filter.close();
        }
    }

    /**
     * Registers a pre processor to be executed before the rest request is actually handled.
     */
    public synchronized void registerFilter(RestFilter preProcessor) {
        RestFilter[] copy = new RestFilter[filters.length + 1];
        System.arraycopy(filters, 0, copy, 0, filters.length);
        copy[filters.length] = preProcessor;
        Arrays.sort(copy, (o1, o2) -> Integer.compare(o1.order(), o2.order()));
        filters = copy;
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
     * Returns a filter chain (if needed) to execute. If this method returns null, simply execute
     * as usual.
     */
    @Nullable
    public RestFilterChain filterChainOrNull(RestFilter executionFilter) {
        if (filters.length == 0) {
            return null;
        }
        return new ControllerFilterChain(executionFilter);
    }

    /**
     * Returns a filter chain with the final filter being the provided filter.
     */
    public RestFilterChain filterChain(RestFilter executionFilter) {
        return new ControllerFilterChain(executionFilter);
    }

    /**
     * @param request The current request. Must not be null.
     * @return true iff the circuit breaker limit must be enforced for processing this request.
     */
    public boolean canTripCircuitBreaker(RestRequest request) {
        RestHandler handler = getHandler(request);
        return (handler != null) ? handler.canTripCircuitBreaker() : true;
    }

    public void dispatchRequest(final RestRequest request, final RestChannel channel, final NodeClient client, ThreadContext threadContext) throws Exception {
        if (!checkRequestParameters(request, channel)) {
            return;
        }
        try (ThreadContext.StoredContext ignored = threadContext.stashContext()) {
            for (String key : headersToCopy) {
                String httpHeader = request.header(key);
                if (httpHeader != null) {
                    threadContext.putHeader(key, httpHeader);
                }
            }
            if (filters.length == 0) {
                executeHandler(request, channel, client);
            } else {
                ControllerFilterChain filterChain = new ControllerFilterChain(handlerFilter);
                filterChain.continueProcessing(request, channel, client);
            }
        }
    }

    public void sendErrorResponse(RestRequest request, RestChannel channel, Exception e) {
        try {
            channel.sendResponse(new BytesRestResponse(channel, e));
        } catch (Exception inner) {
            inner.addSuppressed(e);
            logger.error("failed to send failure response for uri [{}]", inner, request.uri());
        }
    }

    /**
     * Checks the request parameters against enabled settings for error trace support
     * @return true if the request does not have any parameters that conflict with system settings
     */
    boolean checkRequestParameters(final RestRequest request, final RestChannel channel) {
        // error_trace cannot be used when we disable detailed errors
        if (channel.detailedErrorsEnabled() == false && request.paramAsBoolean("error_trace", false)) {
            try {
                XContentBuilder builder = channel.newErrorBuilder();
                builder.startObject().field("error","error traces in responses are disabled.").endObject().string();
                RestResponse response = new BytesRestResponse(BAD_REQUEST, builder);
                response.addHeader("Content-Type", "application/json");
                channel.sendResponse(response);
            } catch (IOException e) {
                logger.warn("Failed to send response", e);
            }
            return false;
        }

        return true;
    }

    void executeHandler(RestRequest request, RestChannel channel, NodeClient client) throws Exception {
        final RestHandler handler = getHandler(request);
        if (handler != null) {
            handler.handleRequest(request, channel, client);
        } else {
            if (request.method() == RestRequest.Method.OPTIONS) {
                // when we have OPTIONS request, simply send OK by default (with the Access Control Origin header which gets automatically added)
                channel.sendResponse(new BytesRestResponse(OK, BytesRestResponse.TEXT_CONTENT_TYPE, BytesArray.EMPTY));
            } else {
                final String msg = "No handler found for uri [" + request.uri() + "] and method [" + request.method() + "]";
                channel.sendResponse(new BytesRestResponse(BAD_REQUEST, msg));
            }
        }
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

    class ControllerFilterChain implements RestFilterChain {

        private final RestFilter executionFilter;

        private final AtomicInteger index = new AtomicInteger();

        ControllerFilterChain(RestFilter executionFilter) {
            this.executionFilter = executionFilter;
        }

        @Override
        public void continueProcessing(RestRequest request, RestChannel channel, NodeClient client) {
            try {
                int loc = index.getAndIncrement();
                if (loc > filters.length) {
                    throw new IllegalStateException("filter continueProcessing was called more than expected");
                } else if (loc == filters.length) {
                    executionFilter.process(request, channel, client, this);
                } else {
                    RestFilter preProcessor = filters[loc];
                    preProcessor.process(request, channel, client, this);
                }
            } catch (Exception e) {
                try {
                    channel.sendResponse(new BytesRestResponse(channel, e));
                } catch (IOException e1) {
                    logger.error("Failed to send failure response for uri [{}]", e1, request.uri());
                }
            }
        }
    }

    class RestHandlerFilter extends RestFilter {

        @Override
        public void process(RestRequest request, RestChannel channel, NodeClient client, RestFilterChain filterChain) throws Exception {
            executeHandler(request, channel, client);
        }
    }
}
