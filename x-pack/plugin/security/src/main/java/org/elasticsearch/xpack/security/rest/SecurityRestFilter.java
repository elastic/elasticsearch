/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.rest;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.exception.ElasticsearchSecurityException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.rest.RestInterceptor;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestRequest.Method;
import org.elasticsearch.rest.RestRequestFilter;
import org.elasticsearch.xpack.security.audit.AuditTrailService;
import org.elasticsearch.xpack.security.authc.support.SecondaryAuthenticator;
import org.elasticsearch.xpack.security.authz.restriction.WorkflowService;
import org.elasticsearch.xpack.security.operator.OperatorPrivileges;

import static org.elasticsearch.core.Strings.format;

public class SecurityRestFilter implements RestInterceptor {

    private static final Logger logger = LogManager.getLogger(SecurityRestFilter.class);

    private final SecondaryAuthenticator secondaryAuthenticator;
    private final AuditTrailService auditTrailService;
    private final boolean enabled;
    private final ThreadContext threadContext;
    private final OperatorPrivileges.OperatorPrivilegesService operatorPrivilegesService;

    public SecurityRestFilter(
        boolean enabled,
        ThreadContext threadContext,
        SecondaryAuthenticator secondaryAuthenticator,
        AuditTrailService auditTrailService,
        OperatorPrivileges.OperatorPrivilegesService operatorPrivilegesService
    ) {
        this.enabled = enabled;
        this.threadContext = threadContext;
        this.secondaryAuthenticator = secondaryAuthenticator;
        this.auditTrailService = auditTrailService;
        // can be null if security is not enabled
        this.operatorPrivilegesService = operatorPrivilegesService == null
            ? OperatorPrivileges.NOOP_OPERATOR_PRIVILEGES_SERVICE
            : operatorPrivilegesService;
    }

    @Override
    public void intercept(RestRequest request, RestChannel channel, RestHandler targetHandler, ActionListener<Boolean> listener)
        throws Exception {
        // requests with the OPTIONS method should be handled elsewhere, and not by calling {@code RestHandler#handleRequest}
        // authn is bypassed for HTTP requests with the OPTIONS method, so this sanity check prevents dispatching unauthenticated requests
        if (request.method() == Method.OPTIONS) {
            handleException(
                request,
                new ElasticsearchSecurityException("Cannot dispatch OPTIONS request, as they are not authenticated"),
                listener
            );
            return;
        }

        if (enabled == false) {
            listener.onResponse(Boolean.TRUE);
            return;
        }

        final RestRequest wrappedRequest = maybeWrapRestRequest(request, targetHandler);
        auditTrailService.get().authenticationSuccess(wrappedRequest);
        secondaryAuthenticator.authenticateAndAttachToContext(wrappedRequest, ActionListener.wrap(secondaryAuthentication -> {
            if (secondaryAuthentication != null) {
                logger.trace("Found secondary authentication {} in REST request [{}]", secondaryAuthentication, request.uri());
            }
            WorkflowService.resolveWorkflowAndStoreInThreadContext(targetHandler, threadContext);

            doHandleRequest(request, channel, targetHandler, listener);
        }, e -> handleException(request, e, listener)));
    }

    private void doHandleRequest(RestRequest request, RestChannel channel, RestHandler targetHandler, ActionListener<Boolean> listener) {
        threadContext.sanitizeHeaders();
        // operator privileges can short circuit to return a non-successful response
        if (operatorPrivilegesService.checkRest(targetHandler, request, channel, threadContext)) {
            listener.onResponse(Boolean.TRUE);
        } else {
            // The service sends its own response if it returns `false`.
            // That's kind of ugly, and it would be better if we throw an exception and let the rest controller serialize it as normal
            listener.onResponse(Boolean.FALSE);
        }
    }

    protected void handleException(RestRequest request, Exception e, ActionListener<?> listener) {
        logger.debug(() -> format("failed for REST request [%s]", request.uri()), e);
        threadContext.sanitizeHeaders();
        listener.onFailure(e);
    }

    // for testing
    OperatorPrivileges.OperatorPrivilegesService getOperatorPrivilegesService() {
        return operatorPrivilegesService;
    }

    private RestRequest maybeWrapRestRequest(RestRequest restRequest, RestHandler targetHandler) {
        if (targetHandler instanceof RestRequestFilter rrf) {
            return rrf.getFilteredRequest(restRequest);
        }
        return restRequest;
    }

}
