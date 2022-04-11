/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.rest;

import org.apache.logging.log4j.Level;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.RestApiVersion;
import org.elasticsearch.rest.RestRequest.Method;
import org.elasticsearch.xcontent.XContent;

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

    default boolean mediaTypesValid(RestRequest request) {
        return request.getXContentType() != null;
    }

    class Route {

        private final Method method;
        private final String path;
        private final RestApiVersion restApiVersion;

        private final String deprecationMessage;
        @Nullable
        private final Level deprecationLevel;

        private final Route replacedRoute;

        private Route(
            Method method,
            String path,
            RestApiVersion restApiVersion,
            String deprecationMessage,
            Level deprecationLevel,
            Route replacedRoute
        ) {
            this.method = Objects.requireNonNull(method);
            this.path = Objects.requireNonNull(path);
            this.restApiVersion = Objects.requireNonNull(restApiVersion);

            // a deprecated route will have a deprecation message, and the restApiVersion
            // will represent the version when the route was deprecated
            this.deprecationMessage = deprecationMessage;
            this.deprecationLevel = deprecationLevel;

            // a route that replaces another route will have a reference to the route that was replaced
            this.replacedRoute = replacedRoute;
        }

        /**
         * Constructs a Route that pairs an HTTP method with an associated path.
         * <p>
         * This is sufficient for most routes in Elasticsearch, like "GET /", "PUT /_cluster/settings", or "POST my_index/_close".
         *
         * @param method the method, e.g. GET
         * @param path   the path, e.g. "/"
         */
        public Route(Method method, String path) {
            this(method, path, RestApiVersion.current(), null, null, null);
        }

        public static class RouteBuilder {

            private final Method method;
            private final String path;
            private RestApiVersion restApiVersion;

            private String deprecationMessage;
            @Nullable
            private Level deprecationLevel;

            private Route replacedRoute;

            private RouteBuilder(Method method, String path) {
                this.method = Objects.requireNonNull(method);
                this.path = Objects.requireNonNull(path);
                this.restApiVersion = RestApiVersion.current();
            }

            /**
             * Marks that the route being built has been deprecated (for some reason -- the deprecationMessage), and notes the major
             * version in which that deprecation occurred.
             * <p>
             * For example:
             * <pre> {@code
             * Route.builder(GET, "_upgrade")
             *  .deprecated("The _upgrade API is no longer useful and will be removed.", RestApiVersion.V_7)
             *  .build()}</pre>
             *
             * @param deprecationMessage the user-visible explanation of this deprecation
             * @param deprecatedInVersion the major version in which the deprecation occurred
             * @return a reference to this object.
             */
            public RouteBuilder deprecated(String deprecationMessage, RestApiVersion deprecatedInVersion) {
                assert this.replacedRoute == null;
                this.restApiVersion = Objects.requireNonNull(deprecatedInVersion);
                this.deprecationMessage = Objects.requireNonNull(deprecationMessage);
                return this;
            }

            /**
             * Marks that the route being built has been deprecated (for some reason -- the deprecationMessage), and notes the major
             * version in which that deprecation occurred.
             * <p>
             * For example:
             * <pre> {@code
             * Route.builder(GET, "_upgrade")
             *  .deprecated("The _upgrade API is no longer useful and will be removed.", RestApiVersion.V_7)
             *  .build()}</pre>
             *
             * @param deprecationMessage the user-visible explanation of this deprecation
             * @param deprecationLevel the level at which to log the deprecation
             * @param deprecatedInVersion the major version in which the deprecation occurred
             * @return a reference to this object.
             */
            public RouteBuilder deprecated(String deprecationMessage, Level deprecationLevel, RestApiVersion deprecatedInVersion) {
                assert this.replacedRoute == null;
                this.restApiVersion = Objects.requireNonNull(deprecatedInVersion);
                this.deprecationMessage = Objects.requireNonNull(deprecationMessage);
                this.deprecationLevel = deprecationLevel;
                return this;
            }

            /**
             * Marks that the route being built replaces another route, and notes the major version in which that replacement occurred.
             * <p>
             * For example:
             * <pre> {@code
             * Route.builder(GET, "/_security/user/")
             *   .replaces(GET, "/_xpack/security/user/", RestApiVersion.V_7).build()}</pre>
             *
             * @param method the method being replaced
             * @param path the path being replaced
             * @param replacedInVersion the major version in which the replacement occurred
             * @return a reference to this object.
             */
            public RouteBuilder replaces(Method method, String path, RestApiVersion replacedInVersion) {
                assert this.deprecationMessage == null;
                this.replacedRoute = new Route(method, path, replacedInVersion, null, null, null);
                return this;
            }

            public Route build() {
                if (replacedRoute != null) {
                    return new Route(method, path, restApiVersion, null, null, replacedRoute);
                } else if (deprecationMessage != null) {
                    return new Route(method, path, restApiVersion, deprecationMessage, deprecationLevel, null);
                } else {
                    // this is a little silly, but perfectly legal
                    return new Route(method, path, restApiVersion, null, null, null);
                }
            }
        }

        public static RouteBuilder builder(Method method, String path) {
            return new RouteBuilder(method, path);
        }

        public String getPath() {
            return path;
        }

        public Method getMethod() {
            return method;
        }

        public RestApiVersion getRestApiVersion() {
            return restApiVersion;
        }

        public String getDeprecationMessage() {
            return deprecationMessage;
        }

        @Nullable
        public Level getDeprecationLevel() {
            return deprecationLevel;
        }

        public boolean isDeprecated() {
            return deprecationMessage != null;
        }

        public Route getReplacedRoute() {
            return replacedRoute;
        }

        public boolean isReplacement() {
            return replacedRoute != null;
        }
    }
}
