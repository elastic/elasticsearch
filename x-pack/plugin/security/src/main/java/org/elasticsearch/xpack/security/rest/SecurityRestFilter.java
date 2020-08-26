/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.rest;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.logging.log4j.util.Supplier;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.http.HttpChannel;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestRequest.Method;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xpack.core.security.rest.RestRequestFilter;
import org.elasticsearch.xpack.security.authc.AuthenticationService;
import org.elasticsearch.xpack.security.authc.support.SecondaryAuthenticator;
import org.elasticsearch.xpack.security.transport.SSLEngineUtils;

import java.io.IOException;
import java.util.List;

public class SecurityRestFilter implements RestHandler {

    private static final Logger logger = LogManager.getLogger(SecurityRestFilter.class);

    private final RestHandler restHandler;
    private final AuthenticationService authenticationService;
    private final SecondaryAuthenticator secondaryAuthenticator;
    private final XPackLicenseState licenseState;
    private final ThreadContext threadContext;
    private final boolean extractClientCertificate;

    public SecurityRestFilter(XPackLicenseState licenseState, ThreadContext threadContext, AuthenticationService authenticationService,
                              SecondaryAuthenticator secondaryAuthenticator, RestHandler restHandler, boolean extractClientCertificate) {
        this.licenseState = licenseState;
        this.threadContext = threadContext;
        this.authenticationService = authenticationService;
        this.secondaryAuthenticator = secondaryAuthenticator;
        this.restHandler = restHandler;
        this.extractClientCertificate = extractClientCertificate;
    }

    @Override
    public void handleRequest(RestRequest request, RestChannel channel, NodeClient client) throws Exception {
        if (licenseState.isSecurityEnabled() && request.method() != Method.OPTIONS) {
            // CORS - allow for preflight unauthenticated OPTIONS request
            if (extractClientCertificate) {
                HttpChannel httpChannel = request.getHttpChannel();
                SSLEngineUtils.extractClientCertificates(logger, threadContext, httpChannel);
            }

            final String requestUri = request.uri();
            authenticationService.authenticate(maybeWrapRestRequest(request), ActionListener.wrap(
                authentication -> {
                    if (authentication == null) {
                        logger.trace("No authentication available for REST request [{}]", requestUri);
                    } else {
                        logger.trace("Authenticated REST request [{}] as {}", requestUri, authentication);
                    }
                    secondaryAuthenticator.authenticateAndAttachToContext(request, ActionListener.wrap(
                        secondaryAuthentication -> {
                            if (secondaryAuthentication != null) {
                                logger.trace("Found secondary authentication {} in REST request [{}]", secondaryAuthentication, requestUri);
                            }
                            RemoteHostHeader.process(request, threadContext);
                            restHandler.handleRequest(request, channel, client);
                        },
                        e -> handleException("Secondary authentication", request, channel, e)));
                }, e -> handleException("Authentication", request, channel, e)));
        } else {
            restHandler.handleRequest(request, channel, client);
        }
    }

    private void handleException(String actionType, RestRequest request, RestChannel channel, Exception e) {
        logger.debug(new ParameterizedMessage("{} failed for REST request [{}]", actionType, request.uri()), e);
        final RestStatus restStatus = ExceptionsHelper.status(e);
        try {
            channel.sendResponse(new BytesRestResponse(channel, restStatus, e) {

                @Override
                protected boolean skipStackTrace() { return restStatus == RestStatus.UNAUTHORIZED; }

            });
        } catch (Exception inner) {
            inner.addSuppressed(e);
            logger.error((Supplier<?>) () ->
                new ParameterizedMessage("failed to send failure response for uri [{}]", request.uri()), inner);
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
    public List<DeprecatedRoute> deprecatedRoutes() {
        return restHandler.deprecatedRoutes();
    }

    @Override
    public List<ReplacedRoute> replacedRoutes() {
        return restHandler.replacedRoutes();
    }

    private RestRequest maybeWrapRestRequest(RestRequest restRequest) throws IOException {
        if (restHandler instanceof RestRequestFilter) {
            return ((RestRequestFilter)restHandler).getFilteredRequest(restRequest);
        }
        return restRequest;
    }
}
