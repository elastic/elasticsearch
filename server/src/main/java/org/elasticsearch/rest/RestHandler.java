/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
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
import java.util.Set;

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
     * Indicates if the RestHandler supports bulk content. A bulk request contains multiple objects
     * delineated by {@link XContent#bulkSeparator()}. If a handler returns true this will affect
     * the types of content that can be sent to this endpoint.
     */
    default boolean supportsBulkContent() {
        return false;
    }

    /**
     * Returns the concrete RestHandler for this RestHandler. That is, if this is a delegating RestHandler it returns the delegate.
     * Otherwise it returns itself.
     * @return The underlying RestHandler
     */
    default RestHandler getConcreteRestHandler() {
        return this;
    }

    /**
     * Returns the serverless Scope of this RestHandler. This is only meaningful when running in a servlerless environment. If a
     * RestHandler has no ServerlessScope annotation, then this method returns null, meaning that this RestHandler is not visible at all in
     * Serverless mode.
     * @return The Scope for this handler, or null if there is no ServerlessScope annotation
     */
    default Scope getServerlessScope() {
        ServerlessScope serverlessScope = getConcreteRestHandler().getClass().getAnnotation(ServerlessScope.class);
        return serverlessScope == null ? null : serverlessScope.value();
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
     * The set of path and query parameters that could be present on this handler.
     * This method is only required due to <a href="https://github.com/elastic/elasticsearch/issues/36785">#36785</a>,
     * which conflates query and path parameters inside the rest handler.
     * This method should be overridden to add path parameters to {@link #supportedQueryParameters}
     * if the handler has path parameters.
     * This method will be removed when {@link #supportedQueryParameters()} and {@link BaseRestHandler#responseParams()} are combined.
     */
    default @Nullable Set<String> allSupportedParameters() {
        return supportedQueryParameters();
    }

    /**
     * The set of query parameters accepted by this rest handler,
     * {@code null} if query parameters should not be checked nor validated.
     * TODO - make this not nullable when all handlers have been updated
     */
    default @Nullable Set<String> supportedQueryParameters() {
        return null;
    }

    /**
     * The set of capabilities this rest handler supports.
     */
    default Set<String> supportedCapabilities() {
        return Set.of();
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

    default String getName() {
        return this.getClass().getSimpleName();
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
             * Marks that the route being built has been deprecated (for some reason -- the deprecationMessage) for removal. Notes the last
             * major version in which the path is fully supported without compatibility headers. If this path is being replaced by another
             * then use {@link #replaces(Method, String, RestApiVersion)} instead.
             * <p>
             * For example:
             * <pre> {@code
             * Route.builder(GET, "_upgrade")
             *  .deprecated("The _upgrade API is no longer useful and will be removed.", RestApiVersion.V_7)
             *  .build()}</pre>
             *
             * @param deprecationMessage the user-visible explanation of this deprecation
             * @param lastFullySupportedVersion the last {@link RestApiVersion} (i.e. 7) for which this route is fully supported.
             *                                  The next major version (i.e. 8) will require compatibility header(s). (;compatible-with=7)
             *                                  The next major version (i.e. 9) will have no support whatsoever for this route.
             * @return a reference to this object.
             */
            public RouteBuilder deprecated(String deprecationMessage, RestApiVersion lastFullySupportedVersion) {
                assert this.replacedRoute == null;
                this.restApiVersion = Objects.requireNonNull(lastFullySupportedVersion);
                this.deprecationMessage = Objects.requireNonNull(deprecationMessage);
                return this;
            }

            /**
             * Marks that the route being built replaces another route, and notes the last major version in which the path is fully
             * supported without compatibility headers.
             * <p>
             * For example:
             * <pre> {@code
             * Route.builder(GET, "/_security/user/")
             *   .replaces(GET, "/_xpack/security/user/", RestApiVersion.V_7).build()}</pre>
             *
             * @param method the method being replaced
             * @param path the path being replaced
             * @param lastFullySupportedVersion the last {@link RestApiVersion} (i.e. 7) for which this route is fully supported.
             *                                  The next major version (i.e. 8) will require compatibility header(s). (;compatible-with=7)
             *                                  The next major version (i.e. 9) will have no support whatsoever for this route.
             * @return a reference to this object.
             */
            public RouteBuilder replaces(Method method, String path, RestApiVersion lastFullySupportedVersion) {
                assert this.deprecationMessage == null;
                this.replacedRoute = new Route(method, path, lastFullySupportedVersion, null, null, null);
                return this;
            }

            /**
             * Marks that the route being built has been deprecated (for some reason -- the deprecationMessage), but will not be removed.
             * <p>
             * For example:
             * <pre> {@code
             * Route.builder(GET, "_upgrade")
             *  .deprecated("The _upgrade API is no longer useful but will not be removed.")
             *  .build()}</pre>
             *
             * @param deprecationMessage the user-visible explanation of this deprecation
             * @return a reference to this object.
             */
            public RouteBuilder deprecateAndKeep(String deprecationMessage) {
                assert this.replacedRoute == null;
                this.restApiVersion = RestApiVersion.current();
                this.deprecationMessage = Objects.requireNonNull(deprecationMessage);
                this.deprecationLevel = Level.WARN;
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
