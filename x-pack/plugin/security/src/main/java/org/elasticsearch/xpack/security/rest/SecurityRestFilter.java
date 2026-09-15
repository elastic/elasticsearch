/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.rest;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.rest.RestInterceptor;
import org.elasticsearch.rest.RestInterceptorChain;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestRequest.Method;
import org.elasticsearch.rest.RestRequestFilter;
import org.elasticsearch.xpack.security.audit.AuditTrailService;
import org.elasticsearch.xpack.security.authc.support.SecondaryAuthenticator;
import org.elasticsearch.xpack.security.authz.restriction.WorkflowService;
import org.elasticsearch.xpack.security.operator.OperatorPrivileges;

import java.util.function.Consumer;

import static org.elasticsearch.core.Strings.format;
import static org.elasticsearch.rest.RestContentAggregator.aggregate;

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
    public void intercept(RestInterceptorChain chain, ActionListener<Void> listener) {
        // requests with the OPTIONS method should be handled elsewhere, and not by calling {@code RestHandler#handleRequest}
        // authn is bypassed for HTTP requests with the OPTIONS method, so this sanity check prevents dispatching unauthenticated requests
        if (chain.request().method() == Method.OPTIONS) {
            handleException(
                chain.request(),
                new ElasticsearchSecurityException("Cannot dispatch OPTIONS request, as they are not authenticated"),
                listener
            );
            return;
        }

        if (enabled == false) {
            chain.proceed(listener);
            return;
        }

        // RestRequest might have stream content, in some cases we need to aggregate request content, for example audit logging.
        final Consumer<RestRequest> aggregationCallback = (aggregatedRestRequest) -> {
            final RestRequest wrappedRequest = maybeWrapRestRequest(aggregatedRestRequest, chain.handler());
            try {
                auditTrailService.get().authenticationSuccess(wrappedRequest);
            } catch (ElasticsearchStatusException e) {
                handleException(aggregatedRestRequest, e, listener);
                return;
            }
            secondaryAuthenticator.authenticateAndAttachToContext(wrappedRequest, ActionListener.wrap(secondaryAuthentication -> {
                if (secondaryAuthentication != null) {
                    logger.trace(
                        "Found secondary authentication {} in REST request [{}]",
                        secondaryAuthentication,
                        aggregatedRestRequest.uri()
                    );
                }
                WorkflowService.resolveWorkflowAndStoreInThreadContext(chain.handler(), threadContext);

                doHandleRequest(chain, aggregatedRestRequest, listener);
            }, e -> handleException(aggregatedRestRequest, e, listener)));
        };
        if (chain.request().isStreamedContent() && auditTrailService.includeRequestBody()) {
            aggregate(chain.request(), aggregationCallback);
        } else {
            aggregationCallback.accept(chain.request());
        }
    }

    @Override
    public int order() {
        // Security interceptor should execute before the default ones
        return -1000;
    }

    private void doHandleRequest(RestInterceptorChain chain, RestRequest request, ActionListener<Void> listener) {
        threadContext.sanitizeHeaders();
        // operator privileges can short circuit to return a non-successful response
        if (operatorPrivilegesService.checkRest(chain.handler(), request, chain.channel(), threadContext)) {
            chain.proceed(listener);
        } else {
            // The service sends its own response if it returns `null`.
            // That's kind of ugly, and it would be better if we throw an exception and let the rest controller serialize it as normal
            listener.onResponse(null);
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
