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

import com.google.common.collect.ImmutableSet;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.path.PathTrie;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.rest.support.RestUtils;

import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.concurrent.atomic.AtomicInteger;

import static org.elasticsearch.rest.RestStatus.*;

/**
 *
 */
public class RestController extends AbstractLifecycleComponent<RestController> {

    private ImmutableSet<String> relevantHeaders = ImmutableSet.of();

    private final PathTrie<RestHandler> getHandlers = new PathTrie<>(RestUtils.REST_DECODER);
    private final PathTrie<RestHandler> postHandlers = new PathTrie<>(RestUtils.REST_DECODER);
    private final PathTrie<RestHandler> putHandlers = new PathTrie<>(RestUtils.REST_DECODER);
    private final PathTrie<RestHandler> deleteHandlers = new PathTrie<>(RestUtils.REST_DECODER);
    private final PathTrie<RestHandler> headHandlers = new PathTrie<>(RestUtils.REST_DECODER);
    private final PathTrie<RestHandler> optionsHandlers = new PathTrie<>(RestUtils.REST_DECODER);

    private final RestHandlerFilter handlerFilter = new RestHandlerFilter();

    // non volatile since the assumption is that pre processors are registered on startup
    private RestFilter[] filters = new RestFilter[0];

    @Inject
    public RestController(Settings settings) {
        super(settings);
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
     * Controls which REST headers get copied over from a {@link org.elasticsearch.rest.RestRequest} to
     * its corresponding {@link org.elasticsearch.transport.TransportRequest}(s).
     *
     * By default no headers get copied but it is possible to extend this behaviour via plugins by calling this method.
     */
    public synchronized void registerRelevantHeaders(String... headers) {
        relevantHeaders = new ImmutableSet.Builder<String>().addAll(relevantHeaders).add(headers).build();
    }

    /**
     * Returns the REST headers that get copied over from a {@link org.elasticsearch.rest.RestRequest} to
     * its corresponding {@link org.elasticsearch.transport.TransportRequest}(s).
     * By default no headers get copied but it is possible to extend this behaviour via plugins by calling {@link #registerRelevantHeaders(String...)}.
     */
    public ImmutableSet<String> relevantHeaders() {
        return relevantHeaders;
    }

    /**
     * Registers a pre processor to be executed before the rest request is actually handled.
     */
    public synchronized void registerFilter(RestFilter preProcessor) {
        RestFilter[] copy = new RestFilter[filters.length + 1];
        System.arraycopy(filters, 0, copy, 0, filters.length);
        copy[filters.length] = preProcessor;
        Arrays.sort(copy, new Comparator<RestFilter>() {
            @Override
            public int compare(RestFilter o1, RestFilter o2) {
                return Integer.compare(o1.order(), o2.order());
            }
        });
        filters = copy;
    }

    /**
     * Registers a rest handler to be execute when the provided method and path match the request.
     */
    public void registerHandler(RestRequest.Method method, String path, RestHandler handler) {
        switch (method) {
            case GET:
                getHandlers.insert(path, handler);
                break;
            case DELETE:
                deleteHandlers.insert(path, handler);
                break;
            case POST:
                postHandlers.insert(path, handler);
                break;
            case PUT:
                putHandlers.insert(path, handler);
                break;
            case OPTIONS:
                optionsHandlers.insert(path, handler);
                break;
            case HEAD:
                headHandlers.insert(path, handler);
                break;
            default:
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

    public void dispatchRequest(final RestRequest request, final RestChannel channel) {
        if (!checkRequestParameters(request, channel)) {
            return;
        }

        if (filters.length == 0) {
            try {
                executeHandler(request, channel);
            } catch (Throwable e) {
                try {
                    channel.sendResponse(new BytesRestResponse(channel, e));
                } catch (Throwable e1) {
                    logger.error("failed to send failure response for uri [" + request.uri() + "]", e1);
                }
            }
        } else {
            ControllerFilterChain filterChain = new ControllerFilterChain(handlerFilter);
            filterChain.continueProcessing(request, channel);
        }
    }

    /**
     * Checks the request parameters against enabled settings for error trace support
     * @param request
     * @param channel
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

    void executeHandler(RestRequest request, RestChannel channel) throws Exception {
        final RestHandler handler = getHandler(request);
        if (handler != null) {
            handler.handleRequest(request, channel);
        } else {
            if (request.method() == RestRequest.Method.OPTIONS) {
                // when we have OPTIONS request, simply send OK by default (with the Access Control Origin header which gets automatically added)
                channel.sendResponse(new BytesRestResponse(OK));
            } else {
                channel.sendResponse(new BytesRestResponse(BAD_REQUEST, "No handler found for uri [" + request.uri() + "] and method [" + request.method() + "]"));
            }
        }
    }

    private RestHandler getHandler(RestRequest request) {
        String path = getPath(request);
        RestRequest.Method method = request.method();
        if (method == RestRequest.Method.GET) {
            return getHandlers.retrieve(path, request.params());
        } else if (method == RestRequest.Method.POST) {
            return postHandlers.retrieve(path, request.params());
        } else if (method == RestRequest.Method.PUT) {
            return putHandlers.retrieve(path, request.params());
        } else if (method == RestRequest.Method.DELETE) {
            return deleteHandlers.retrieve(path, request.params());
        } else if (method == RestRequest.Method.HEAD) {
            return headHandlers.retrieve(path, request.params());
        } else if (method == RestRequest.Method.OPTIONS) {
            return optionsHandlers.retrieve(path, request.params());
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
        public void continueProcessing(RestRequest request, RestChannel channel) {
            try {
                int loc = index.getAndIncrement();
                if (loc > filters.length) {
                    throw new IllegalStateException("filter continueProcessing was called more than expected");
                } else if (loc == filters.length) {
                    executionFilter.process(request, channel, this);
                } else {
                    RestFilter preProcessor = filters[loc];
                    preProcessor.process(request, channel, this);
                }
            } catch (Exception e) {
                try {
                    channel.sendResponse(new BytesRestResponse(channel, e));
                } catch (IOException e1) {
                    logger.error("Failed to send failure response for uri [" + request.uri() + "]", e1);
                }
            }
        }
    }

    class RestHandlerFilter extends RestFilter {

        @Override
        public void process(RestRequest request, RestChannel channel, RestFilterChain filterChain) throws Exception {
            executeHandler(request, channel);
        }
    }
}
