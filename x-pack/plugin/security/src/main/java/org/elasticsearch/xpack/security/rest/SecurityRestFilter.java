/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.rest;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.logging.log4j.util.Supplier;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.logging.HeaderWarning;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestRequest.Method;
import org.elasticsearch.rest.RestRequestFilter;
import org.elasticsearch.xpack.security.audit.AuditTrailService;
import org.elasticsearch.xpack.security.authc.support.SecondaryAuthenticator;

import java.io.IOException;
import java.util.List;

public class SecurityRestFilter implements RestHandler {

    private static final Logger logger = LogManager.getLogger(SecurityRestFilter.class);

    private final RestHandler restHandler;
    private final SecondaryAuthenticator secondaryAuthenticator;
    private final XPackLicenseState licenseState;
    private final AuditTrailService auditTrailService;
    private final ThreadContext threadContext;

    public SecurityRestFilter(
        XPackLicenseState licenseState,
        ThreadContext threadContext,
        SecondaryAuthenticator secondaryAuthenticator,
        AuditTrailService auditTrailService,
        RestHandler restHandler
    ) {
        this.licenseState = licenseState;
        this.threadContext = threadContext;
        this.secondaryAuthenticator = secondaryAuthenticator;
        this.auditTrailService = auditTrailService;
        this.restHandler = restHandler;
    }

    @Override
    public boolean allowSystemIndexAccessByDefault() {
        return restHandler.allowSystemIndexAccessByDefault();
    }

    @Override
    public void handleRequest(RestRequest request, RestChannel channel, NodeClient client) throws Exception {
        if (licenseState.isSecurityEnabled() && request.method() != Method.OPTIONS) {
            // CORS - allow for preflight unauthenticated OPTIONS request

            final String requestUri = request.uri();
            final RestRequest wrappedRequest = maybeWrapRestRequest(request);
            auditTrailService.get().authenticationSuccess(wrappedRequest);
            secondaryAuthenticator.authenticateAndAttachToContext(wrappedRequest, ActionListener.wrap(secondaryAuthentication -> {
                if (secondaryAuthentication != null) {
                    logger.trace("Found secondary authentication {} in REST request [{}]", secondaryAuthentication, requestUri);
                }
                restHandler.handleRequest(request, channel, client);
            }, e -> handleException("Secondary authentication", request, channel, e)));
        } else {
            if (request.method() != Method.OPTIONS) {
                HeaderWarning.addWarning(
                    "Elasticsearch built-in security features are not enabled. Without "
                        + "authentication, your cluster could be accessible to anyone. See "
                        + "https://www.elastic.co/guide/en/elasticsearch/reference/"
                        + Version.CURRENT.major
                        + "."
                        + Version.CURRENT.minor
                        + "/security-minimal-setup.html to enable security."
                );
            }
            restHandler.handleRequest(request, channel, client);
        }
    }

    private void handleException(String actionType, RestRequest request, RestChannel channel, Exception e) {
        logger.debug(new ParameterizedMessage("{} failed for REST request [{}]", actionType, request.uri()), e);
        try {
            channel.sendResponse(new BytesRestResponse(channel, e));
        } catch (Exception inner) {
            inner.addSuppressed(e);
            logger.error(
                (Supplier<?>) () -> new ParameterizedMessage("failed to send failure response for uri [{}]", request.uri()),
                inner
            );
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

    private RestRequest maybeWrapRestRequest(RestRequest restRequest) throws IOException {
        if (restHandler instanceof RestRequestFilter) {
            return ((RestRequestFilter) restHandler).getFilteredRequest(restRequest);
        }
        return restRequest;
    }
}
