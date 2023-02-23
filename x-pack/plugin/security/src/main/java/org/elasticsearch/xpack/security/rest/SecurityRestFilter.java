/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.rest;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.util.Supplier;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.http.HttpRequest;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestRequest.Method;
import org.elasticsearch.rest.RestRequestFilter;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.security.authc.AuthenticationService;
import org.elasticsearch.xpack.security.authc.support.SecondaryAuthenticator;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.core.Strings.format;

public class SecurityRestFilter implements RestHandler {

    private static final Logger logger = LogManager.getLogger(SecurityRestFilter.class);

    private final RestHandler restHandler;
    private final AuthenticationService authenticationService;
    private final SecondaryAuthenticator secondaryAuthenticator;
    private final boolean enabled;
    private final ThreadContext threadContext;

    public enum ActionType {
        Authentication("Authentication"),
        SecondaryAuthentication("Secondary authentication"),
        RequestHandling("Request handling");

        private final String name;

        ActionType(String name) {
            this.name = name;
        }

        @Override
        public String toString() {
            return name;
        }
    }

    public SecurityRestFilter(
        boolean enabled,
        ThreadContext threadContext,
        AuthenticationService authenticationService,
        SecondaryAuthenticator secondaryAuthenticator,
        RestHandler restHandler
    ) {
        this.enabled = enabled;
        this.threadContext = threadContext;
        this.authenticationService = authenticationService;
        this.secondaryAuthenticator = secondaryAuthenticator;
        this.restHandler = restHandler;
    }

    @Override
    public boolean allowSystemIndexAccessByDefault() {
        return restHandler.allowSystemIndexAccessByDefault();
    }

    public RestHandler getConcreteRestHandler() {
        return restHandler.getConcreteRestHandler();
    }

    @Override
    public void handleRequest(RestRequest request, RestChannel channel, NodeClient client) throws Exception {
        if (request.method() == Method.OPTIONS) {
            // CORS - allow for preflight unauthenticated OPTIONS request
            restHandler.handleRequest(request, channel, client);
            return;
        }

        if (enabled == false) {
            doHandleRequest(request, channel, client);
            return;
        }

        final HttpRequest httpRequest = wrapHttpContent(request.getHttpRequest(), request.getXContentType());
        authenticationService.authenticate(httpRequest, ActionListener.wrap(authentication -> {
            if (authentication == null) {
                logger.trace("No authentication available for REST request [{}]", httpRequest.uri());
            } else {
                logger.trace("Authenticated REST request [{}] as {}", httpRequest.uri(), authentication);
            }
            secondaryAuthenticator.authenticateAndAttachToContext(httpRequest, ActionListener.wrap(secondaryAuthentication -> {
                if (secondaryAuthentication != null) {
                    logger.trace("Found secondary authentication {} in REST request [{}]", secondaryAuthentication, httpRequest.uri());
                }
                try {
                    doHandleRequest(request, channel, client);
                } catch (Exception e) {
                    handleException(ActionType.RequestHandling, request, channel, e);
                }
            }, e -> handleException(ActionType.SecondaryAuthentication, request, channel, e)));
        }, e -> handleException(ActionType.Authentication, request, channel, e)));
    }

    private void doHandleRequest(RestRequest request, RestChannel channel, NodeClient client) throws Exception {
        threadContext.sanitizeHeaders();
        restHandler.handleRequest(request, channel, client);
    }

    protected void handleException(ActionType actionType, RestRequest request, RestChannel channel, Exception e) {
        threadContext.sanitizeHeaders();
        handleException(actionType, channel, e);
    }

    public static void handleAuthenticationFailed(RestChannel channel, Exception e) {
        handleException(ActionType.Authentication, channel, e);
    }

    private static void handleException(ActionType actionType, RestChannel channel, Exception e) {
        logger.debug(() -> format("%s failed for HTTP request [%s]", actionType, channel.request().uri()), e);
        try {
            channel.sendResponse(new RestResponse(channel, e) {

                @Override
                protected boolean skipStackTrace() {
                    return status() == RestStatus.UNAUTHORIZED;
                }

                @Override
                public Map<String, List<String>> filterHeaders(Map<String, List<String>> headers) {
                    if (actionType != ActionType.RequestHandling
                        || (status() == RestStatus.UNAUTHORIZED || status() == RestStatus.FORBIDDEN)) {
                        if (headers.containsKey("Warning")) {
                            headers = Maps.copyMapWithRemovedEntry(headers, "Warning");
                        }
                        if (headers.containsKey("X-elastic-product")) {
                            headers = Maps.copyMapWithRemovedEntry(headers, "X-elastic-product");
                        }
                    }
                    return headers;
                }

            });
        } catch (Exception inner) {
            inner.addSuppressed(e);
            logger.error((Supplier<?>) () -> "failed to send failure response for uri [" + channel.request().uri() + "]", inner);
        }
    }

    @Override
    public boolean canTripCircuitBreaker() {
        return restHandler.canTripCircuitBreaker();
    }

    @Override
    public boolean supportsContentStream() {
        return restHandler.supportsContentStream();
    }

    @Override
    public boolean allowsUnsafeBuffers() {
        return restHandler.allowsUnsafeBuffers();
    }

    @Override
    public List<Route> routes() {
        return restHandler.routes();
    }

    @Override
    public boolean mediaTypesValid(RestRequest request) {
        return restHandler.mediaTypesValid(request);
    }

    private HttpRequest wrapHttpContent(HttpRequest httpRequest, XContentType xContentType) {
        return RestRequestFilter.formatRequestContentForAuditing(
            httpRequest,
            xContentType,
            restHandler instanceof RestRequestFilter ? ((RestRequestFilter) restHandler).getFilteredFields() : Set.of()
        );
    }
}
