/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.rest;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.compatibility.RestApiCompatibleVersion;
import org.elasticsearch.common.xcontent.MediaType;
import org.elasticsearch.common.xcontent.MediaTypeRegistry;
import org.elasticsearch.common.xcontent.XContent;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.rest.RestRequest.Method;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Handler for REST requests
 */
@FunctionalInterface
public interface RestHandler {

    /**
     * Handles a rest request.
     * @param request The request to handle
     * @param channel The channel to write the request response to
     * @param client A client to use to make internal requests on behalf of the original request
     */
    void handleRequest(RestRequest request, RestChannel channel, NodeClient client) throws Exception;

    default boolean canTripCircuitBreaker() {
        return true;
    }

    /**
     * Indicates if the RestHandler supports content as a stream. A stream would be multiple objects delineated by
     * {@link XContent#streamSeparator()}. If a handler returns true this will affect the types of content that can be sent to
     * this endpoint.
     */
    default boolean supportsContentStream() {
        return false;
    }

    /**
     * Indicates if the RestHandler supports working with pooled buffers. If the request handler will not escape the return
     * {@link RestRequest#content()} or any buffers extracted from it then there is no need to make a copies of any pooled buffers in the
     * {@link RestRequest} instance before passing a request to this handler. If this instance does not support pooled/unsafe buffers
     * {@link RestRequest#ensureSafeBuffers()} should be called on any request before passing it to {@link #handleRequest}.
     *
     * @return true iff the handler supports requests that make use of pooled buffers
     */
    default boolean allowsUnsafeBuffers() {
        return false;
    }

    /**
     * The list of {@link Route}s that this RestHandler is responsible for handling.
     */
    default List<Route> routes() {
        return Collections.emptyList();
    }

    /**
     * Controls whether requests handled by this class are allowed to to access system indices by default.
     * @return {@code true} if requests handled by this class should be allowed to access system indices.
     */
    default boolean allowSystemIndexAccessByDefault() {
        return false;
    }

    default MediaTypeRegistry<? extends MediaType> validAcceptMediaTypes() {
        return XContentType.MEDIA_TYPE_REGISTRY;
    }

    /**
     * Returns a version a handler is compatible with.
     * This version is then used to math a handler with a request that specified a version.
     * If no version is specified, handler is assumed to be compatible with <code>Version.CURRENT</code>
     * @return a version
     */
    default RestApiCompatibleVersion compatibleWithVersion() {
        return RestApiCompatibleVersion.currentVersion();
    }

    class Route {

        private final Method method;
        private final String path;

        private final RestApiCompatibleVersion restApiCompatibleVersion;

        private final String deprecationMessage;

        private final Method replacedMethod;
        private final String replacedPath;

        private Route(Method method, String path,
                      RestApiCompatibleVersion restApiCompatibleVersion,
                      String deprecationMessage,
                      Method replacedMethod, String replacedPath) {
            this.method = Objects.requireNonNull(method);
            this.path = Objects.requireNonNull(path);

            this.restApiCompatibleVersion = restApiCompatibleVersion;
            this.deprecationMessage = deprecationMessage;
            this.replacedMethod = replacedMethod;
            this.replacedPath = replacedPath;
        }

        // bog standard
        public static Route of(Method method, String path) {
            return new Route(method, path,
                null, null, null, null);
        }

        // deprecated without replacement
        public static Route deprecated(Method method, String path,
                                       RestApiCompatibleVersion restApiCompatibleVersion, String deprecationMessage) {
            return new Route(method, path,
                Objects.requireNonNull(restApiCompatibleVersion), Objects.requireNonNull(deprecationMessage), null, null);
        }

        // a route that is only available via compatibility
        public static Route of(Method method, String path,
                               RestApiCompatibleVersion restApiCompatibleVersion) {
            return new Route(method, path,
                Objects.requireNonNull(restApiCompatibleVersion), null, null, null);
        }

        // a replacement route where the replaced route is still available via compatibility
        public static Route replaces(Method method, String path,
                                     Method replacedMethod, String replacedPath, RestApiCompatibleVersion restApiCompatibleVersion) {
            return new Route(method, path,
                Objects.requireNonNull(restApiCompatibleVersion), null,
                Objects.requireNonNull(replacedMethod), Objects.requireNonNull(replacedPath));
        }

        public String getPath() {
            return path;
        }

        public Method getMethod() {
            return method;
        }

        public RestApiCompatibleVersion getRestApiCompatibleVersion() {
            return restApiCompatibleVersion;
        }

        public String getDeprecationMessage() {
            return deprecationMessage;
        }

        public boolean isDeprecated() {
            return deprecationMessage != null;
        }

        public String getReplacedPath() {
            return replacedPath;
        }

        public Method getReplacedMethod() {
            return replacedMethod;
        }

        public boolean isReplacement() {
            return replacedPath != null || replacedMethod != null;
        }
    }
}
