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
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.rest.FilterRestHandler;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestRequest.Method;
import org.elasticsearch.rest.RestRequestFilter;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.xpack.security.audit.AuditTrailService;
import org.elasticsearch.xpack.security.authc.support.SecondaryAuthenticator;
import org.elasticsearch.xpack.security.authz.restriction.WorkflowService;
import org.elasticsearch.xpack.security.operator.OperatorPrivileges;

import static org.elasticsearch.core.Strings.format;

public class SecurityRestFilter extends FilterRestHandler implements RestHandler {

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
        RestHandler restHandler,
        OperatorPrivileges.OperatorPrivilegesService operatorPrivilegesService
    ) {
        super(restHandler);
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
    public void handleRequest(RestRequest request, RestChannel channel, NodeClient client) throws Exception {
        // requests with the OPTIONS method should be handled elsewhere, and not by calling {@code RestHandler#handleRequest}
        // authn is bypassed for HTTP requests with the OPTIONS method, so this sanity check prevents dispatching unauthenticated requests
        if (request.method() == Method.OPTIONS) {
            handleException(
                request,
                channel,
                new ElasticsearchSecurityException("Cannot dispatch OPTIONS request, as they are not authenticated")
            );
            return;
        }

        if (enabled == false) {
            doHandleRequest(request, channel, client);
            return;
        }

        final RestRequest wrappedRequest = maybeWrapRestRequest(request);
        auditTrailService.get().authenticationSuccess(wrappedRequest);
        secondaryAuthenticator.authenticateAndAttachToContext(wrappedRequest, ActionListener.wrap(secondaryAuthentication -> {
            if (secondaryAuthentication != null) {
                logger.trace("Found secondary authentication {} in REST request [{}]", secondaryAuthentication, request.uri());
            }
            WorkflowService.resolveWorkflowAndStoreInThreadContext(getConcreteRestHandler(), threadContext);

            doHandleRequest(request, channel, client);
        }, e -> handleException(request, channel, e)));
    }

    private void doHandleRequest(RestRequest request, RestChannel channel, NodeClient client) throws Exception {
        threadContext.sanitizeHeaders();
        // operator privileges can short circuit to return a non-successful response
        if (operatorPrivilegesService.checkRest(getConcreteRestHandler(), request, channel, threadContext)) {
            try {
                getDelegate().handleRequest(request, channel, client);
            } catch (Exception e) {
                logger.debug(() -> format("Request handling failed for REST request [%s]", request.uri()), e);
                throw e;
            }
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

    // for testing
    OperatorPrivileges.OperatorPrivilegesService getOperatorPrivilegesService() {
        return operatorPrivilegesService;
    }

    private RestRequest maybeWrapRestRequest(RestRequest restRequest) {
        if (getConcreteRestHandler() instanceof RestRequestFilter rrf) {
            return rrf.getFilteredRequest(restRequest);
        }
        return restRequest;
    }

}
