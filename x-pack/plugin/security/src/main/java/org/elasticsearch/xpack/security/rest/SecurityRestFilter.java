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
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestRequest.Method;
import org.elasticsearch.rest.RestRequestFilter;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.xpack.security.audit.AuditTrailService;
import org.elasticsearch.xpack.security.authc.AuthenticationService;
import org.elasticsearch.xpack.security.authc.support.SecondaryAuthenticator;

import java.util.List;
import java.util.Optional;

import static org.elasticsearch.core.Strings.format;

public class SecurityRestFilter implements RestHandler {

    private static final Logger logger = LogManager.getLogger(SecurityRestFilter.class);

    private final RestHandler restHandler;
    private final AuthenticationService authenticationService;
    private final SecondaryAuthenticator secondaryAuthenticator;
    private final AuditTrailService auditTrailService;
    private final boolean enabled;
    private final ThreadContext threadContext;

    public SecurityRestFilter(
        boolean enabled,
        ThreadContext threadContext,
        AuthenticationService authenticationService,
        SecondaryAuthenticator secondaryAuthenticator,
        AuditTrailService auditTrailService,
        RestHandler restHandler
    ) {
        this.enabled = enabled;
        this.threadContext = threadContext;
        this.authenticationService = authenticationService;
        this.secondaryAuthenticator = secondaryAuthenticator;
        this.auditTrailService = auditTrailService;
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

        final RestRequest wrappedRequest = maybeWrapRestRequest(request);
        authenticationService.authenticate(wrappedRequest.getHttpRequest(), ActionListener.wrap(authentication -> {
            if (authentication == null) {
                logger.trace("No authentication available for REST request [{}]", request.uri());
            } else {
                logger.trace("Authenticated REST request [{}] as {}", request.uri(), authentication);
            }
            auditTrailService.get().authenticationSuccess(wrappedRequest);
            secondaryAuthenticator.authenticateAndAttachToContext(wrappedRequest, ActionListener.wrap(secondaryAuthentication -> {
                if (secondaryAuthentication != null) {
                    logger.trace("Found secondary authentication {} in REST request [{}]", secondaryAuthentication, request.uri());
                }
                maybeStoreRestHandlerNameInThreadContext(request);
                doHandleRequest(request, channel, client);
            }, e -> handleException(request, channel, e)));
        }, e -> handleException(request, channel, e)));
    }

    private void doHandleRequest(RestRequest request, RestChannel channel, NodeClient client) throws Exception {
        threadContext.sanitizeHeaders();
        try {
            restHandler.handleRequest(request, channel, client);
        } catch (Exception e) {
            logger.debug(() -> format("Request handling failed for REST request [%s]", request.uri()), e);
            throw e;
        }
    }

    private Optional<String> resolveRestHandlerName() {
        // TODO: Not all rest handlers extend BaseRestHandler.
        // Would it make sense to enforce that every rest handler provides a unique name?
        // Or assume if name is missing then the roles which are restricted to workflows should be rejected?
        if (restHandler instanceof BaseRestHandler baseRestHandler) {
            return Optional.ofNullable(baseRestHandler.getName());
        } else {
            return Optional.empty();
        }
    }

    private void maybeStoreRestHandlerNameInThreadContext(RestRequest request) {
        final Optional<String> restHandlerName = resolveRestHandlerName();
        assert threadContext.getHeader("_xpack_rest_handler_name") == null
            : "thread context should not have rest handler name set: " + threadContext.getHeader("_xpack_rest_handler_name");
        if (restHandlerName.isPresent()) {
            logger.info(() -> format("resolved name [%s] of rest handler [%s] for uri [%s].",restHandlerName.get(), restHandler.getClass(), request.uri()));
            threadContext.putHeader("_xpack_rest_handler_name", restHandlerName.get());
        } else {
            logger.info(() -> format("could not resolve name of rest handler [%s] for uri [%s].", restHandler.getClass(), request.uri()));
        }
    }

    protected void handleException(RestRequest request, RestChannel channel, Exception e) {
        logger.debug(() -> format("failed for REST request [%s]", request.uri()), e);
        threadContext.sanitizeHeaders();
        try {
            channel.sendResponse(new RestResponse(channel, e));
        } catch (Exception inner) {
            inner.addSuppressed(e);
            logger.error((Supplier<?>) () -> "failed to send failure response for uri [" + request.uri() + "]", inner);
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

    private RestRequest maybeWrapRestRequest(RestRequest restRequest) {
        if (restHandler instanceof RestRequestFilter) {
            return ((RestRequestFilter) restHandler).getFilteredRequest(restRequest);
        }
        return restRequest;
    }

    @Override
    public boolean mediaTypesValid(RestRequest request) {
        return restHandler.mediaTypesValid(request);
    }
}
