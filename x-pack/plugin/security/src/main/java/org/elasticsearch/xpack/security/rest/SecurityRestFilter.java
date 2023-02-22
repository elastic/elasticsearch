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
import org.elasticsearch.ElasticsearchSecurityException;
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
import org.elasticsearch.xpack.core.security.SecurityContext;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.security.audit.AuditTrail;
import org.elasticsearch.xpack.security.audit.AuditTrailService;
import org.elasticsearch.xpack.security.audit.AuditUtil;
import org.elasticsearch.xpack.security.authc.support.SecondaryAuthenticator;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.core.Strings.format;

public class SecurityRestFilter implements RestHandler {

    private static final Logger logger = LogManager.getLogger(SecurityRestFilter.class);

    private final RestHandler restHandler;
    private final SecondaryAuthenticator secondaryAuthenticator;
    private final boolean enabled;
    private final SecurityContext securityContext;
    private final AuditTrailService auditTrailService;

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
        SecurityContext securityContext,
        AuditTrailService auditTrailService,
        SecondaryAuthenticator secondaryAuthenticator,
        RestHandler restHandler
    ) {
        this.enabled = enabled;
        this.securityContext = securityContext;
        this.auditTrailService = auditTrailService;
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
            restHandler.handleRequest(request, channel, client);
            return;
        }

        // this only lazily wraps the http request body to ensure sensitive info doesn't end up in the audit log
        final HttpRequest httpRequest = wrapHttpContent(request.getHttpRequest(), request.getXContentType());
        // ensures request is authenticated and audits the request body
        try {
            final String requestId = AuditUtil.extractRequestId(securityContext.getThreadContext());
            if (requestId == null) {
                throw new IllegalStateException("Audit request id not found");
            }
            final AuditTrail auditTrail = auditTrailService.get();
            final Authentication authentication;
            try {
                authentication = securityContext.getAuthentication();
            } catch (Exception e) {
                logger.error(() -> format("caught exception while trying to read authentication from request [%s]", httpRequest.uri()), e);
                auditTrail.tamperedRequest(requestId, httpRequest);
                throw new ElasticsearchSecurityException("http request could not be authenticated", e);
            }
            if (authentication == null) {
                logger.error(() -> format("No authentication available for REST request [%s]", httpRequest.uri()));
                auditTrail.tamperedRequest(requestId, httpRequest);
                throw new ElasticsearchSecurityException("http request could not be authenticated");
            }
            logger.trace(() -> format("Authenticated REST request [%s] as %s", httpRequest.uri(), authentication));
            auditTrail.authenticationSuccess(requestId, authentication, httpRequest);
        } catch (Exception e) {
            handleException(ActionType.Authentication, channel, e, securityContext.getThreadContext());
            return;
        }

        secondaryAuthenticator.authenticateAndAttachToContext(httpRequest, ActionListener.wrap(secondaryAuthentication -> {
            if (secondaryAuthentication != null) {
                logger.trace("Found secondary authentication {} in REST request [{}]", secondaryAuthentication, httpRequest.uri());
            }
            try {
                doHandleRequest(request, channel, client);
            } catch (Exception e) {
                handleException(ActionType.RequestHandling, channel, e, securityContext.getThreadContext());
            }
        }, e -> handleException(ActionType.SecondaryAuthentication, channel, e, securityContext.getThreadContext())));
    }

    private void doHandleRequest(RestRequest request, RestChannel channel, NodeClient client) throws Exception {
        securityContext.getThreadContext().sanitizeHeaders();
        restHandler.handleRequest(request, channel, client);
    }

    private static void handleException(ActionType actionType, RestChannel channel, Exception e, ThreadContext threadContext) {
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
