/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.authc;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.common.cache.Cache;
import org.elasticsearch.common.cache.CacheBuilder;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.FixForMultiProject;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.exception.ElasticsearchSecurityException;
import org.elasticsearch.http.HttpPreRequest;
import org.elasticsearch.node.Node;
import org.elasticsearch.telemetry.metric.MeterRegistry;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.AuthenticationFailureHandler;
import org.elasticsearch.xpack.core.security.authc.AuthenticationServiceField;
import org.elasticsearch.xpack.core.security.authc.AuthenticationToken;
import org.elasticsearch.xpack.core.security.authc.Realm;
import org.elasticsearch.xpack.core.security.authc.support.AuthenticationContextSerializer;
import org.elasticsearch.xpack.core.security.authz.AuthorizationEngine.EmptyAuthorizationInfo;
import org.elasticsearch.xpack.core.security.user.AnonymousUser;
import org.elasticsearch.xpack.core.security.user.User;
import org.elasticsearch.xpack.security.audit.AuditTrail;
import org.elasticsearch.xpack.security.audit.AuditTrailService;
import org.elasticsearch.xpack.security.audit.AuditUtil;
import org.elasticsearch.xpack.security.authc.service.ServiceAccountService;
import org.elasticsearch.xpack.security.operator.OperatorPrivileges.OperatorPrivilegesService;
import org.elasticsearch.xpack.security.support.SecurityIndexManager;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

import static org.elasticsearch.xpack.security.support.SecurityIndexManager.isIndexDeleted;
import static org.elasticsearch.xpack.security.support.SecurityIndexManager.isMoveFromRedToNonRed;

/**
 * An authentication service that delegates the authentication process to its configured {@link Realm realms}.
 * This service also supports request level caching of authenticated users (i.e. once a user authenticated
 * successfully, it is set on the request context to avoid subsequent redundant authentication process)
 */
public class AuthenticationService {

    static final Setting<Boolean> SUCCESS_AUTH_CACHE_ENABLED = Setting.boolSetting(
        "xpack.security.authc.success_cache.enabled",
        true,
        Property.NodeScope
    );
    private static final Setting<Integer> SUCCESS_AUTH_CACHE_MAX_SIZE = Setting.intSetting(
        "xpack.security.authc.success_cache.size",
        10000,
        Property.NodeScope
    );
    private static final Setting<TimeValue> SUCCESS_AUTH_CACHE_EXPIRE_AFTER_ACCESS = Setting.timeSetting(
        "xpack.security.authc.success_cache.expire_after_access",
        TimeValue.timeValueHours(1L),
        Property.NodeScope
    );

    private static final Logger logger = LogManager.getLogger(AuthenticationService.class);

    private final Realms realms;
    private final AuditTrailService auditTrailService;
    private final AuthenticationFailureHandler failureHandler;
    private final ThreadContext threadContext;
    private final Cache<String, Realm> lastSuccessfulAuthCache;
    private final AtomicLong numInvalidation = new AtomicLong();
    private final AuthenticatorChain authenticatorChain;

    public AuthenticationService(
        Settings settings,
        Realms realms,
        AuditTrailService auditTrailService,
        AuthenticationFailureHandler failureHandler,
        ThreadPool threadPool,
        AnonymousUser anonymousUser,
        TokenService tokenService,
        ApiKeyService apiKeyService,
        ServiceAccountService serviceAccountService,
        OperatorPrivilegesService operatorPrivilegesService,
        MeterRegistry meterRegistry
    ) {
        this.realms = realms;
        this.auditTrailService = auditTrailService;
        this.failureHandler = failureHandler;
        this.threadContext = threadPool.getThreadContext();
        if (SUCCESS_AUTH_CACHE_ENABLED.get(settings)) {
            this.lastSuccessfulAuthCache = CacheBuilder.<String, Realm>builder()
                .setMaximumWeight(Integer.toUnsignedLong(SUCCESS_AUTH_CACHE_MAX_SIZE.get(settings)))
                .setExpireAfterAccess(SUCCESS_AUTH_CACHE_EXPIRE_AFTER_ACCESS.get(settings))
                .build();
        } else {
            this.lastSuccessfulAuthCache = null;
        }

        final String nodeName = Node.NODE_NAME_SETTING.get(settings);
        this.authenticatorChain = new AuthenticatorChain(
            settings,
            operatorPrivilegesService,
            anonymousUser,
            new AuthenticationContextSerializer(),
            new ServiceAccountAuthenticator(serviceAccountService, nodeName, meterRegistry),
            new OAuth2TokenAuthenticator(tokenService, meterRegistry),
            new ApiKeyAuthenticator(apiKeyService, nodeName, meterRegistry),
            new RealmsAuthenticator(numInvalidation, lastSuccessfulAuthCache, meterRegistry)
        );
    }

    /**
     * Authenticates the user that is associated with the given request. If the user was authenticated successfully (i.e.
     * a user was indeed associated with the request and the credentials were verified to be valid), the method returns
     * the user and that user is then "attached" to the request's context.
     * This method will authenticate as the anonymous user if the service is configured to allow anonymous access.
     *
     * @param request The request to be authenticated
     */
    public void authenticate(HttpPreRequest request, ActionListener<Authentication> authenticationListener) {
        authenticate(request, true, authenticationListener);
    }

    /**
     * Authenticates the user that is associated with the given request. If the user was authenticated successfully (i.e.
     * a user was indeed associated with the request and the credentials were verified to be valid), the method returns
     * the user and that user is then "attached" to the request's context.
     * This method will optionally, authenticate as the anonymous user if the service is configured to allow anonymous access.
     *
     * @param request The request to be authenticated
     * @param allowAnonymous If {@code false}, then authentication will <em>not</em> fallback to anonymous.
     *                               If {@code true}, then authentication <em>will</em> fallback to anonymous, if this service is
     *                               configured to allow anonymous access.
     */
    public void authenticate(HttpPreRequest request, boolean allowAnonymous, ActionListener<Authentication> authenticationListener) {
        final Authenticator.Context context = new Authenticator.Context(
            threadContext,
            new AuditableHttpRequest(auditTrailService.get(), failureHandler, threadContext, request),
            null,
            allowAnonymous,
            realms
        );
        authenticate(context, authenticationListener);
    }

    /**
     * Authenticates the user that is associated with the given message. If the user was authenticated successfully (i.e.
     * a user was indeed associated with the request and the credentials were verified to be valid), the method returns
     * the user and that user is then "attached" to the message's context. If no user was found to be attached to the given
     * message, then the given fallback user will be returned instead.
     * @param action       The action of the message
     * @param transportRequest      The request to be authenticated
     * @param fallbackUser The default user that will be assumed if no other user is attached to the message. May not be {@code null}.
     */
    public void authenticate(String action, TransportRequest transportRequest, User fallbackUser, ActionListener<Authentication> listener) {
        Objects.requireNonNull(fallbackUser, "fallback user may not be null");
        final Authenticator.Context context = new Authenticator.Context(
            threadContext,
            new AuditableTransportRequest(auditTrailService.get(), failureHandler, threadContext, action, transportRequest),
            fallbackUser,
            false,
            realms
        );
        authenticate(context, listener);
    }

    /**
     * Authenticates the user that is associated with the given message. If the user was authenticated successfully (i.e.
     * a user was indeed associated with the request and the credentials were verified to be valid), the method returns
     * the user and that user is then "attached" to the message's context.
     * If no user or credentials are found to be attached to the given message, and the caller allows anonymous access
     * ({@code allowAnonymous} parameter), and this service is configured for anonymous access,
     * then the anonymous user will be returned instead.
     * @param action       The action of the message
     * @param transportRequest      The request to be authenticated
     * @param allowAnonymous Whether to permit anonymous access for this request (this only relevant if the service is
     *                       configured for anonymous access).
     */
    public void authenticate(
        String action,
        TransportRequest transportRequest,
        boolean allowAnonymous,
        ActionListener<Authentication> listener
    ) {
        final Authenticator.Context context = new Authenticator.Context(
            threadContext,
            new AuditableTransportRequest(auditTrailService.get(), failureHandler, threadContext, action, transportRequest),
            null,
            allowAnonymous,
            realms
        );
        authenticate(context, listener);
    }

    /**
     * Authenticates the user based on the contents of the token that is provided as parameter. This will not look at the values in the
     * ThreadContext for Authentication.
     *  @param action  The action of the message
     * @param transportRequest The message that resulted in this authenticate call
     * @param token   The token (credentials) to be authenticated
     */
    public void authenticate(
        String action,
        TransportRequest transportRequest,
        AuthenticationToken token,
        ActionListener<Authentication> listener
    ) {
        final Authenticator.Context context = new Authenticator.Context(
            threadContext,
            new AuditableTransportRequest(auditTrailService.get(), failureHandler, threadContext, action, transportRequest),
            realms,
            token
        );
        authenticatorChain.authenticate(context, listener);
    }

    public void expire(String principal) {
        if (lastSuccessfulAuthCache != null) {
            numInvalidation.incrementAndGet();
            lastSuccessfulAuthCache.invalidate(principal);
        }
    }

    public void expireAll() {
        if (lastSuccessfulAuthCache != null) {
            numInvalidation.incrementAndGet();
            lastSuccessfulAuthCache.invalidateAll();
        }
    }

    @FixForMultiProject
    public void onSecurityIndexStateChange(
        ProjectId projectId,
        SecurityIndexManager.IndexState previousState,
        SecurityIndexManager.IndexState currentState
    ) {
        if (lastSuccessfulAuthCache != null) {
            if (isMoveFromRedToNonRed(previousState, currentState)
                || isIndexDeleted(previousState, currentState)
                || Objects.equals(previousState.indexUUID, currentState.indexUUID) == false) {
                expireAll();
            }
        }
    }

    /**
     * Returns an authenticator context for verifying only the provided {@param authenticationToken} without trying
     * to extract any other tokens from the thread context.
     */
    Authenticator.Context newContext(final String action, final TransportRequest request, AuthenticationToken authenticationToken) {
        return new Authenticator.Context(
            threadContext,
            new AuditableTransportRequest(auditTrailService.get(), failureHandler, threadContext, action, request),
            realms,
            authenticationToken
        );
    }

    void authenticate(final Authenticator.Context context, final ActionListener<Authentication> listener) {
        authenticatorChain.authenticate(context, listener);
    }

    // pkg private method for testing
    long getNumInvalidation() {
        return numInvalidation.get();
    }

    public abstract static class AuditableRequest {

        final AuditTrail auditTrail;
        final AuthenticationFailureHandler failureHandler;
        final ThreadContext threadContext;

        AuditableRequest(AuditTrail auditTrail, AuthenticationFailureHandler failureHandler, ThreadContext threadContext) {
            this.auditTrail = auditTrail;
            this.failureHandler = failureHandler;
            this.threadContext = threadContext;
        }

        abstract void realmAuthenticationFailed(AuthenticationToken token, String realm);

        abstract ElasticsearchSecurityException tamperedRequest();

        abstract ElasticsearchSecurityException exceptionProcessingRequest(Exception e, @Nullable AuthenticationToken token);

        abstract ElasticsearchSecurityException authenticationFailed(AuthenticationToken token);

        abstract ElasticsearchSecurityException anonymousAccessDenied();

        abstract ElasticsearchSecurityException runAsDenied(Authentication authentication, AuthenticationToken token);

        abstract void authenticationSuccess(Authentication authentication);

    }

    static class AuditableTransportRequest extends AuditableRequest {

        private final String action;
        private final TransportRequest transportRequest;
        private final String requestId;

        AuditableTransportRequest(
            AuditTrail auditTrail,
            AuthenticationFailureHandler failureHandler,
            ThreadContext threadContext,
            String action,
            TransportRequest transportRequest
        ) {
            super(auditTrail, failureHandler, threadContext);
            this.action = action;
            this.transportRequest = transportRequest;
            // There might be an existing audit-id (e.g. generated by the rest request) but there might not be (e.g. an internal action)
            this.requestId = AuditUtil.getOrGenerateRequestId(threadContext);
        }

        @Override
        void authenticationSuccess(Authentication authentication) {
            auditTrail.authenticationSuccess(requestId, authentication, action, transportRequest);
        }

        @Override
        void realmAuthenticationFailed(AuthenticationToken token, String realm) {
            auditTrail.authenticationFailed(requestId, realm, token, action, transportRequest);
        }

        @Override
        ElasticsearchSecurityException tamperedRequest() {
            auditTrail.tamperedRequest(requestId, action, transportRequest);
            return new ElasticsearchSecurityException("failed to verify signed authentication information");
        }

        @Override
        ElasticsearchSecurityException exceptionProcessingRequest(Exception e, @Nullable AuthenticationToken token) {
            if (token != null) {
                auditTrail.authenticationFailed(requestId, token, action, transportRequest);
            } else {
                auditTrail.authenticationFailed(requestId, action, transportRequest);
            }
            return failureHandler.exceptionProcessingRequest(transportRequest, action, e, threadContext);
        }

        @Override
        ElasticsearchSecurityException authenticationFailed(AuthenticationToken token) {
            auditTrail.authenticationFailed(requestId, token, action, transportRequest);
            return failureHandler.failedAuthentication(transportRequest, token, action, threadContext);
        }

        @Override
        ElasticsearchSecurityException anonymousAccessDenied() {
            auditTrail.anonymousAccessDenied(requestId, action, transportRequest);
            return failureHandler.missingToken(transportRequest, action, threadContext);
        }

        @Override
        ElasticsearchSecurityException runAsDenied(Authentication authentication, AuthenticationToken token) {
            auditTrail.runAsDenied(requestId, authentication, action, transportRequest, EmptyAuthorizationInfo.INSTANCE);
            return failureHandler.failedAuthentication(transportRequest, token, action, threadContext);
        }

        @Override
        public String toString() {
            return "transport request action [" + action + "]";
        }

    }

    static class AuditableHttpRequest extends AuditableRequest {

        private final HttpPreRequest request;
        private final String requestId;

        AuditableHttpRequest(
            AuditTrail auditTrail,
            AuthenticationFailureHandler failureHandler,
            ThreadContext threadContext,
            HttpPreRequest request
        ) {
            super(auditTrail, failureHandler, threadContext);
            this.request = request;
            // There should never be an existing audit-id when processing a rest request.
            this.requestId = AuditUtil.generateRequestId(threadContext);
        }

        @Override
        void authenticationSuccess(Authentication authentication) {
            // REST requests are audited in the {@code SecurityRestFilter} because they need access to the request body
            // see {@code AuditTrail#authenticationSuccess(HttpRequestLineAndHeaders)}
            // It's still valuable to keep the parent interface {@code AuditableRequest#AuthenticationSuccess(Authentication)} around
            // in order to audit authN success for transport requests for CCS. We may be able to find another way to audit that, which
            // doesn't rely on an `AuditableRequest` instance, but it's not trivial because we'd have to make sure to not audit
            // existing authentications. Separately, it's not easy to reconstruct another `AuditableRequest` outside the
            // `AuthenticationService` because that's tied to the audit `request.id` generation.
            // For more context see: https://github.com/elastic/elasticsearch/pull/94120#discussion_r1152804133
        }

        @Override
        void realmAuthenticationFailed(AuthenticationToken token, String realm) {
            auditTrail.authenticationFailed(requestId, realm, token, request);
        }

        @Override
        ElasticsearchSecurityException tamperedRequest() {
            auditTrail.tamperedRequest(requestId, request);
            return new ElasticsearchSecurityException("rest request attempted to inject a user");
        }

        @Override
        ElasticsearchSecurityException exceptionProcessingRequest(Exception e, @Nullable AuthenticationToken token) {
            if (token != null) {
                auditTrail.authenticationFailed(requestId, token, request);
            } else {
                auditTrail.authenticationFailed(requestId, request);
            }
            return failureHandler.exceptionProcessingRequest(request, e, threadContext);
        }

        @Override
        ElasticsearchSecurityException authenticationFailed(AuthenticationToken token) {
            auditTrail.authenticationFailed(requestId, token, request);
            return failureHandler.failedAuthentication(request, token, threadContext);
        }

        @Override
        ElasticsearchSecurityException anonymousAccessDenied() {
            auditTrail.anonymousAccessDenied(requestId, request);
            return failureHandler.missingToken(request, threadContext);
        }

        @Override
        ElasticsearchSecurityException runAsDenied(Authentication authentication, AuthenticationToken token) {
            auditTrail.runAsDenied(requestId, authentication, request, EmptyAuthorizationInfo.INSTANCE);
            return failureHandler.failedAuthentication(request, token, threadContext);
        }

        @Override
        public String toString() {
            return "rest request uri [" + request.uri() + "]";
        }
    }

    public static void addSettings(List<Setting<?>> settings) {
        settings.add(AuthenticationServiceField.RUN_AS_ENABLED);
        settings.add(SUCCESS_AUTH_CACHE_ENABLED);
        settings.add(SUCCESS_AUTH_CACHE_MAX_SIZE);
        settings.add(SUCCESS_AUTH_CACHE_EXPIRE_AFTER_ACCESS);
    }
}
