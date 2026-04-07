/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.action.filter;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.admin.indices.close.TransportCloseIndexAction;
import org.elasticsearch.action.admin.indices.delete.TransportDeleteIndexAction;
import org.elasticsearch.action.admin.indices.open.OpenIndexAction;
import org.elasticsearch.action.support.ActionFilter;
import org.elasticsearch.action.support.ActionFilterChain;
import org.elasticsearch.action.support.ContextPreservingActionListener;
import org.elasticsearch.action.support.DestructiveOperations;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.license.LicenseUtils;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.XPackField;
import org.elasticsearch.xpack.core.security.SecurityContext;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.support.SecondaryAuthentication;
import org.elasticsearch.xpack.core.security.authz.privilege.HealthAndStatsPrivilege;
import org.elasticsearch.xpack.core.security.user.InternalUsers;
import org.elasticsearch.xpack.security.action.SecurityActionMapper;
import org.elasticsearch.xpack.security.audit.AuditTrailService;
import org.elasticsearch.xpack.security.audit.AuditUtil;
import org.elasticsearch.xpack.security.authc.AuthenticationService;
import org.elasticsearch.xpack.security.authc.support.SecondaryAuthActions;
import org.elasticsearch.xpack.security.authz.AuthorizationService;
import org.elasticsearch.xpack.security.authz.AuthorizationUtils;

import java.util.function.Predicate;

public class SecurityActionFilter implements ActionFilter {

    private static final Predicate<String> LICENSE_EXPIRATION_ACTION_MATCHER = HealthAndStatsPrivilege.INSTANCE.predicate();
    private static final Logger logger = LogManager.getLogger(SecurityActionFilter.class);

    private final AuthenticationService authcService;
    private final AuthorizationService authzService;
    private final AuditTrailService auditTrailService;
    private final XPackLicenseState licenseState;
    private final ThreadContext threadContext;
    private final SecurityContext securityContext;
    private final DestructiveOperations destructiveOperations;
    private final SecondaryAuthActions secondaryAuthActions;

    public SecurityActionFilter(
        AuthenticationService authcService,
        AuthorizationService authzService,
        AuditTrailService auditTrailService,
        XPackLicenseState licenseState,
        ThreadPool threadPool,
        SecurityContext securityContext,
        DestructiveOperations destructiveOperations,
        SecondaryAuthActions secondaryAuthActions
    ) {
        this.authcService = authcService;
        this.authzService = authzService;
        this.auditTrailService = auditTrailService;
        this.licenseState = licenseState;
        this.threadContext = threadPool.getThreadContext();
        this.securityContext = securityContext;
        this.destructiveOperations = destructiveOperations;
        this.secondaryAuthActions = secondaryAuthActions;
    }

    @Override
    public <Request extends ActionRequest, Response extends ActionResponse> void apply(
        Task task,
        String action,
        Request request,
        ActionListener<Response> listener,
        ActionFilterChain<Request, Response> chain
    ) {
        /*
          A functional requirement - when the license of security is disabled (invalid/expires), security will continue
          to operate normally, except the following read operations will be blocked:
            - cluster:monitor/health*
            - cluster:monitor/stats*
            - indices:monitor/stats*
            - cluster:monitor/nodes/stats*
          */
        if (licenseState.isActive() == false && LICENSE_EXPIRATION_ACTION_MATCHER.test(action)) {
            logger.error("""
                blocking [{}] operation due to expired license. Cluster health, cluster stats and indices stats\s
                operations are blocked on license expiration. All data operations (read and write) continue to work.\s
                If you have a new license, please update it. Otherwise, please reach out to your support contact.""", action);
            throw LicenseUtils.newComplianceException(XPackField.SECURITY);
        }

        final ActionListener<Response> contextPreservingListener = ContextPreservingActionListener.wrapPreservingContext(
            listener,
            threadContext
        );
        final boolean useSystemUser = AuthorizationUtils.shouldReplaceUserWithSystem(threadContext, action);
        try {
            if (useSystemUser) {
                securityContext.executeAsSystemUser(original -> applyInternal(task, chain, action, request, contextPreservingListener));
            } else if (AuthorizationUtils.shouldSetUserBasedOnActionOrigin(threadContext)) {
                AuthorizationUtils.switchUserBasedOnActionOriginAndExecute(
                    threadContext,
                    securityContext,
                    TransportVersion.current(), // current version since this is on the same node
                    (original) -> { applyInternal(task, chain, action, request, contextPreservingListener); }
                );
            } else if (secondaryAuthActions.get().contains(action) && threadContext.getHeader("secondary_auth_action_applied") == null) {
                SecondaryAuthentication secondaryAuth = securityContext.getSecondaryAuthentication();
                if (secondaryAuth == null) {
                    throw new IllegalArgumentException("es-secondary-authorization header must be used to call action [" + action + "]");
                } else {
                    secondaryAuth.execute(ignore -> {
                        // this header exists to ensure that if this action goes across nodes we don't attempt to swap out the user again
                        threadContext.putHeader("secondary_auth_action_applied", "true");
                        applyInternal(task, chain, action, request, contextPreservingListener);
                        return null;
                    });
                }
            } else {
                try (ThreadContext.StoredContext ignore = threadContext.newStoredContextPreservingResponseHeaders()) {
                    applyInternal(task, chain, action, request, contextPreservingListener);
                }
            }
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    @Override
    public int order() {
        return Integer.MIN_VALUE;
    }

    private <Request extends ActionRequest, Response extends ActionResponse> void applyInternal(
        Task task,
        ActionFilterChain<Request, Response> chain,
        String action,
        Request request,
        ActionListener<Response> listener
    ) {
        if (TransportCloseIndexAction.NAME.equals(action)
            || OpenIndexAction.NAME.equals(action)
            || TransportDeleteIndexAction.TYPE.name().equals(action)) {
            IndicesRequest indicesRequest = (IndicesRequest) request;
            try {
                destructiveOperations.failDestructive(indicesRequest.indices());
            } catch (IllegalArgumentException e) {
                listener.onFailure(e);
                return;
            }
        }

        final String securityAction = SecurityActionMapper.action(action, request);
        final SubscribableListener<AuthResult> authListener = new SubscribableListener<>();
        authcService.authenticate(
            securityAction,
            request,
            /*
                Here we fall back on the SYSTEM_USER. Internal system requests are requests that are triggered by the system itself (e.g.
                pings, shard relocations) instead of by a REST request sent by a user. Since these requests are triggered internally, they
                are security-agnostic and therefore not associated with any user. When these requests execute locally, they are executed
                directly on their relevant action. Since there is no other way a request can make it to the action without an associated
                user (not via REST or transport - this is taken care of by the SecurityRestFilter and the ServerTransportFilter
                respectively), it's safe to assume a system user here if a request is not associated with any other user.
            */
            InternalUsers.SYSTEM_USER,
            authListener.delegateFailureAndWrap((delegate, authc) -> {
                if (authc != null) {
                    final String requestId = AuditUtil.extractRequestId(threadContext);
                    assert Strings.hasText(requestId);
                    authzService.authorize(
                        authc,
                        securityAction,
                        request,
                        delegate.map(ignored -> new AuthResult(threadContext, requestId, authc))
                    );
                } else {
                    delegate.onFailure(new IllegalStateException("no authentication present but auth is allowed"));
                }
            })
        );

        // Break the sequence of potentially-async actions with a SubscribableListener after authentication & authorization so that in the
        // (common) case that these things completed on the current thread we unwind the massive stack of function calls before proceeding
        // down the rest of the ActionFilterChain and into the action itself.

        authListener.addListener(listener.delegateFailure((ll, authResult) -> {
            try (var ignored = authResult.inAuthenticatedContext()) {
                chain.proceed(task, action, request, ll.map(response -> {
                    auditTrailService.get().coordinatingActionResponse(authResult.requestId, authResult.authc, action, request, response);
                    return response;
                }));
            }
        }));
    }

    /**
     * Carries the request ID, the {@link Authentication}, and the thread context in which auth completed.
     */
    private static class AuthResult {
        private final ThreadContext threadContext;
        private final ThreadContext.StoredContext authenticatedContext;
        final String requestId;
        final Authentication authc;

        AuthResult(ThreadContext threadContext, String requestId, Authentication authc) {
            this.threadContext = threadContext;
            this.authenticatedContext = threadContext.newStoredContext();
            this.requestId = requestId;
            this.authc = authc;
        }

        Releasable inAuthenticatedContext() {
            return threadContext.wrapRestorable(authenticatedContext).get();
        }
    }
}
