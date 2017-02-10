/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.action.filter;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.admin.indices.close.CloseIndexAction;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexAction;
import org.elasticsearch.action.admin.indices.open.OpenIndexAction;
import org.elasticsearch.action.search.ClearScrollRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.action.support.ActionFilter;
import org.elasticsearch.action.support.ActionFilterChain;
import org.elasticsearch.action.support.ContextPreservingActionListener;
import org.elasticsearch.action.support.DestructiveOperations;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.license.LicenseUtils;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.XPackPlugin;
import org.elasticsearch.xpack.security.SecurityContext;
import org.elasticsearch.xpack.security.action.SecurityActionMapper;
import org.elasticsearch.xpack.security.action.interceptor.RequestInterceptor;
import org.elasticsearch.xpack.security.audit.AuditTrail;
import org.elasticsearch.xpack.security.audit.AuditTrailService;
import org.elasticsearch.xpack.security.authc.Authentication;
import org.elasticsearch.xpack.security.authc.AuthenticationService;
import org.elasticsearch.xpack.security.authz.AuthorizationService;
import org.elasticsearch.xpack.security.authz.AuthorizationUtils;
import org.elasticsearch.xpack.security.authz.privilege.HealthAndStatsPrivilege;
import org.elasticsearch.xpack.security.crypto.CryptoService;
import org.elasticsearch.xpack.security.support.Automatons;
import org.elasticsearch.xpack.security.user.SystemUser;
import org.elasticsearch.xpack.security.user.User;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;
import java.util.function.Supplier;

import static org.elasticsearch.xpack.security.support.Exceptions.authorizationError;

public class SecurityActionFilter extends AbstractComponent implements ActionFilter {

    private static final Predicate<String> LICENSE_EXPIRATION_ACTION_MATCHER = HealthAndStatsPrivilege.INSTANCE.predicate();
    private static final Predicate<String> SECURITY_ACTION_MATCHER = Automatons.predicate("cluster:admin/xpack/security*");

    private final AuthenticationService authcService;
    private final AuthorizationService authzService;
    private final CryptoService cryptoService;
    private final AuditTrail auditTrail;
    private final SecurityActionMapper actionMapper = new SecurityActionMapper();
    private final Set<RequestInterceptor> requestInterceptors;
    private final XPackLicenseState licenseState;
    private final ThreadContext threadContext;
    private final SecurityContext securityContext;
    private final DestructiveOperations destructiveOperations;

    @Inject
    public SecurityActionFilter(Settings settings, AuthenticationService authcService, AuthorizationService authzService,
                                CryptoService cryptoService, AuditTrailService auditTrail, XPackLicenseState licenseState,
                                Set<RequestInterceptor> requestInterceptors, ThreadPool threadPool,
                                SecurityContext securityContext, DestructiveOperations destructiveOperations) {
        super(settings);
        this.authcService = authcService;
        this.authzService = authzService;
        this.cryptoService = cryptoService;
        this.auditTrail = auditTrail;
        this.licenseState = licenseState;
        this.requestInterceptors = requestInterceptors;
        this.threadContext = threadPool.getThreadContext();
        this.securityContext = securityContext;
        this.destructiveOperations = destructiveOperations;
    }

    @Override
    public void apply(Task task, String action, ActionRequest request, ActionListener listener, ActionFilterChain chain) {

        /*
         A functional requirement - when the license of security is disabled (invalid/expires), security will continue
         to operate normally, except all read operations will be blocked.
         */
        if (licenseState.isStatsAndHealthAllowed() == false && LICENSE_EXPIRATION_ACTION_MATCHER.test(action)) {
            logger.error("blocking [{}] operation due to expired license. Cluster health, cluster stats and indices stats \n" +
                    "operations are blocked on license expiration. All data operations (read and write) continue to work. \n" +
                    "If you have a new license, please update it. Otherwise, please reach out to your support contact.", action);
            throw LicenseUtils.newComplianceException(XPackPlugin.SECURITY);
        }

        if (licenseState.isAuthAllowed()) {
            final boolean useSystemUser = AuthorizationUtils.shouldReplaceUserWithSystem(threadContext, action);
            final Supplier<ThreadContext.StoredContext> toRestore = threadContext.newRestorableContext(true);
            final ActionListener<ActionResponse> signingListener = new ContextPreservingActionListener<>(toRestore,
                    ActionListener.wrap(r -> {
                        try {
                            listener.onResponse(sign(r));
                        } catch (IOException e) {
                            throw new UncheckedIOException(e);
                        }
                    }, listener::onFailure));
            ActionListener<Void> authenticatedListener =
                    ActionListener.wrap((aVoid) -> chain.proceed(task, action, request, signingListener), signingListener::onFailure);
            try {
                if (useSystemUser) {
                    securityContext.executeAsUser(SystemUser.INSTANCE, (original) -> {
                        try {
                            applyInternal(action, request, authenticatedListener);
                        } catch (IOException e) {
                            listener.onFailure(e);
                        }
                    });
                } else {
                    try (ThreadContext.StoredContext ignore = threadContext.newStoredContext(true)) {
                        applyInternal(action, request, authenticatedListener);
                    }
                }
            } catch (Exception e) {
                listener.onFailure(e);
            }
        } else if (SECURITY_ACTION_MATCHER.test(action)) {
            listener.onFailure(LicenseUtils.newComplianceException(XPackPlugin.SECURITY));
        } else {
            chain.proceed(task, action, request, listener);
        }
    }

    @Override
    public int order() {
        return Integer.MIN_VALUE;
    }

    private void applyInternal(String action, final ActionRequest request, ActionListener<Void> listener) throws IOException {
        if (CloseIndexAction.NAME.equals(action) || OpenIndexAction.NAME.equals(action) || DeleteIndexAction.NAME.equals(action)) {
            IndicesRequest indicesRequest = (IndicesRequest) request;
            try {
                destructiveOperations.failDestructive(indicesRequest.indices());
            } catch(IllegalArgumentException e) {
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
        authcService.authenticate(securityAction, request, SystemUser.INSTANCE,
                ActionListener.wrap((authc) -> authorizeRequest(authc, securityAction, request, listener), listener::onFailure));
    }

    void authorizeRequest(Authentication authentication, String securityAction, ActionRequest request, ActionListener listener) {
        if (authentication == null) {
            listener.onFailure(new IllegalArgumentException("authentication must be non null for authorization"));
        } else {
            final AuthorizationUtils.AsyncAuthorizer asyncAuthorizer = new AuthorizationUtils.AsyncAuthorizer(authentication, listener,
                    (userRoles, runAsRoles) -> {
                        authzService.authorize(authentication, securityAction, request, userRoles, runAsRoles);
                        final User user = authentication.getUser();
                        ActionRequest unsignedRequest = unsign(user, securityAction, request);

                            /*
                             * We use a separate concept for code that needs to be run after authentication and authorization that could
                             * affect the running of the action. This is done to make it more clear of the state of the request.
                             */
                        for (RequestInterceptor interceptor : requestInterceptors) {
                            if (interceptor.supports(unsignedRequest)) {
                                interceptor.intercept(unsignedRequest, user);
                            }
                        }
                        listener.onResponse(null);
                    });
            asyncAuthorizer.authorize(authzService);
        }
    }

    ActionRequest unsign(User user, String action, final ActionRequest request) {
        try {
            if (request instanceof SearchScrollRequest) {
                SearchScrollRequest scrollRequest = (SearchScrollRequest) request;
                String scrollId = scrollRequest.scrollId();
                scrollRequest.scrollId(cryptoService.unsignAndVerify(scrollId));
            } else if (request instanceof ClearScrollRequest) {
                ClearScrollRequest clearScrollRequest = (ClearScrollRequest) request;
                boolean isClearAllScrollRequest = clearScrollRequest.scrollIds().contains("_all");
                if (!isClearAllScrollRequest) {
                    List<String> signedIds = clearScrollRequest.scrollIds();
                    List<String> unsignedIds = new ArrayList<>(signedIds.size());
                    for (String signedId : signedIds) {
                        unsignedIds.add(cryptoService.unsignAndVerify(signedId));
                    }
                    clearScrollRequest.scrollIds(unsignedIds);
                }
            }
        } catch (IllegalArgumentException | IllegalStateException e) {
            auditTrail.tamperedRequest(user, action, request);
            throw authorizationError("invalid request. {}", e.getMessage());
        }
        return request;
    }

    private <Response extends ActionResponse> Response sign(Response response) throws IOException {
        if (response instanceof SearchResponse) {
            SearchResponse searchResponse = (SearchResponse) response;
            String scrollId = searchResponse.getScrollId();
            if (scrollId != null && !cryptoService.isSigned(scrollId)) {
                searchResponse.scrollId(cryptoService.sign(scrollId));
            }
        }
        return response;
    }
}
