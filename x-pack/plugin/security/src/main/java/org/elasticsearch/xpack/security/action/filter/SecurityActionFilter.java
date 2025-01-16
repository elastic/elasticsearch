/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.action.filter;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.admin.indices.close.CloseIndexAction;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexAction;
import org.elasticsearch.action.admin.indices.open.OpenIndexAction;
import org.elasticsearch.action.support.ActionFilter;
import org.elasticsearch.action.support.ActionFilterChain;
import org.elasticsearch.action.support.ContextPreservingActionListener;
import org.elasticsearch.action.support.DestructiveOperations;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.license.LicenseUtils;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.XPackField;
import org.elasticsearch.xpack.core.security.SecurityContext;
import org.elasticsearch.xpack.core.security.authz.privilege.HealthAndStatsPrivilege;
import org.elasticsearch.xpack.core.security.support.Automatons;
import org.elasticsearch.xpack.core.security.user.SystemUser;
import org.elasticsearch.xpack.security.action.SecurityActionMapper;
import org.elasticsearch.xpack.security.audit.AuditTrailService;
import org.elasticsearch.xpack.security.audit.AuditUtil;
import org.elasticsearch.xpack.security.authc.AuthenticationService;
import org.elasticsearch.xpack.security.authz.AuthorizationService;
import org.elasticsearch.xpack.security.authz.AuthorizationUtils;

import java.util.function.Predicate;

public class SecurityActionFilter implements ActionFilter {

    private static final Predicate<String> LICENSE_EXPIRATION_ACTION_MATCHER = HealthAndStatsPrivilege.INSTANCE.predicate();
    private static final Predicate<String> SECURITY_ACTION_MATCHER = Automatons.predicate("cluster:admin/xpack/security*");
    private static final Logger logger = LogManager.getLogger(SecurityActionFilter.class);

    private final AuthenticationService authcService;
    private final AuthorizationService authzService;
    private final AuditTrailService auditTrailService;
    private final SecurityActionMapper actionMapper = new SecurityActionMapper();
    private final XPackLicenseState licenseState;
    private final ThreadContext threadContext;
    private final SecurityContext securityContext;
    private final DestructiveOperations destructiveOperations;

    public SecurityActionFilter(
        AuthenticationService authcService,
        AuthorizationService authzService,
        AuditTrailService auditTrailService,
        XPackLicenseState licenseState,
        ThreadPool threadPool,
        SecurityContext securityContext,
        DestructiveOperations destructiveOperations
    ) {
        this.authcService = authcService;
        this.authzService = authzService;
        this.auditTrailService = auditTrailService;
        this.licenseState = licenseState;
        this.threadContext = threadPool.getThreadContext();
        this.securityContext = securityContext;
        this.destructiveOperations = destructiveOperations;
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
            logger.error(
                "blocking [{}] operation due to expired license. Cluster health, cluster stats and indices stats \n"
                    + "operations are blocked on license expiration. All data operations (read and write) continue to work. \n"
                    + "If you have a new license, please update it. Otherwise, please reach out to your support contact.",
                action
            );
            throw LicenseUtils.newComplianceException(XPackField.SECURITY);
        }

        if (licenseState.isSecurityEnabled()) {
            final ActionListener<Response> contextPreservingListener = ContextPreservingActionListener.wrapPreservingContext(
                listener,
                threadContext
            );
            final boolean useSystemUser = AuthorizationUtils.shouldReplaceUserWithSystem(threadContext, action);
            try {
                if (useSystemUser) {
                    securityContext.executeAsUser(
                        SystemUser.INSTANCE,
                        (original) -> { applyInternal(task, chain, action, request, contextPreservingListener); },
                        Version.CURRENT
                    );
                } else if (AuthorizationUtils.shouldSetUserBasedOnActionOrigin(threadContext)) {
                    AuthorizationUtils.switchUserBasedOnActionOriginAndExecute(threadContext, securityContext, (original) -> {
                        applyInternal(task, chain, action, request, contextPreservingListener);
                    });
                } else {
                    try (ThreadContext.StoredContext ignore = threadContext.newStoredContext(true)) {
                        applyInternal(task, chain, action, request, contextPreservingListener);
                    }
                }
            } catch (Exception e) {
                listener.onFailure(e);
            }
        } else if (SECURITY_ACTION_MATCHER.test(action)) {
            if (licenseState.isSecurityEnabled() == false) {
                listener.onFailure(
                    new ElasticsearchException(
                        "Security must be explicitly enabled when using a ["
                            + licenseState.getOperationMode().description()
                            + "] license. "
                            + "Enable security by setting [xpack.security.enabled] to [true] in the elasticsearch.yml file "
                            + "and restart the node."
                    )
                );
            } else {
                listener.onFailure(LicenseUtils.newComplianceException(XPackField.SECURITY));
            }
        } else {
            chain.proceed(task, action, request, listener);
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
        if (CloseIndexAction.NAME.equals(action) || OpenIndexAction.NAME.equals(action) || DeleteIndexAction.NAME.equals(action)) {
            IndicesRequest indicesRequest = (IndicesRequest) request;
            try {
                destructiveOperations.failDestructive(indicesRequest.indices());
            } catch (IllegalArgumentException e) {
                listener.onFailure(e);
                return;
            }
        }

        /*
         here we fallback on the system user. Internal system requests are requests that are triggered by
         the system itself (e.g. pings, update mappings, share relocation, etc...) and were not originated
         by user interaction. Since these requests are triggered by es core modules, they are security
         agnostic and therefore not associated with any user. When these requests execute locally, they
         are executed directly on their relevant action. Since there is no other way a request can make
         it to the action without an associated user (not via REST or transport - this is taken care of by
         the {@link Rest} filter and the {@link ServerTransport} filter respectively), it's safe to assume a system user
         here if a request is not associated with any other user.
         */
        final String securityAction = actionMapper.action(action, request);
        authcService.authenticate(securityAction, request, SystemUser.INSTANCE, ActionListener.wrap((authc) -> {
            if (authc != null) {
                final String requestId = AuditUtil.extractRequestId(threadContext);
                assert Strings.hasText(requestId);
                authzService.authorize(
                    authc,
                    securityAction,
                    request,
                    listener.delegateFailure((ll, aVoid) -> chain.proceed(task, action, request, ll.delegateFailure((l, response) -> {
                        auditTrailService.get().coordinatingActionResponse(requestId, authc, action, request, response);
                        l.onResponse(response);
                    })))
                );
            } else if (licenseState.isSecurityEnabled() == false) {
                listener.onResponse(null);
            } else {
                listener.onFailure(new IllegalStateException("no authentication present but auth is allowed"));
            }
        }, listener::onFailure));
    }
}
