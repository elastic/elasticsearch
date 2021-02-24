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

        private final String deprecationMessage;

        private final Method deprecatedMethod;
        private final String deprecatedPath;

        // see .deprecated()
        private Route(Method method, String path, String deprecationMessage) {
            this.method = Objects.requireNonNull(method);
            this.path = Objects.requireNonNull(path);

            this.deprecationMessage = Objects.requireNonNull(deprecationMessage);
            this.deprecatedMethod = null;
            this.deprecatedPath = null;
        }

        // see .replaces()
        private Route(Method method, String path, Method deprecatedMethod, String deprecatedPath) {
            this.method = Objects.requireNonNull(method);
            this.path = Objects.requireNonNull(path);

            this.deprecationMessage = null;
            this.deprecatedMethod = Objects.requireNonNull(deprecatedMethod);
            this.deprecatedPath = Objects.requireNonNull(deprecatedPath);
        }

        // in the *trivial* case, you can just construct a Route
        public Route(Method method, String path) {
            this.method = Objects.requireNonNull(method);
            this.path = Objects.requireNonNull(path);

            this.deprecationMessage = null;
            this.deprecatedMethod = null;
            this.deprecatedPath = null;
        }

        public String getPath() {
            return path;
        }

        public Method getMethod() {
            return method;
        }

        public Route deprecated(String deprecationMessage) {
            assert isReplacement() == false; // either deprecated or a replacement, but not both
            return new Route(this.method, this.path, deprecationMessage);
        }

        public String getDeprecationMessage() {
            return deprecationMessage;
        }

        public boolean isDeprecated() {
            return deprecationMessage != null;
        }

        public Route replaces(Method deprecatedMethod, String deprecatedPath) {
            assert isDeprecated() == false; // either deprecated or a replacement, but not both
            return new Route(this.method, this.path, deprecatedMethod, deprecatedPath);
        }

        public String getDeprecatedPath() {
            return deprecatedPath;
        }

        public Method getDeprecatedMethod() {
            return deprecatedMethod;
        }

        public boolean isReplacement() {
            return deprecatedPath != null || deprecatedMethod != null;
        }
    }
}
