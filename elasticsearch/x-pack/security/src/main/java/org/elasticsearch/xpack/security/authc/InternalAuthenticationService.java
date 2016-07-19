/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authc;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.node.Node;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.xpack.security.audit.AuditTrailService;
import org.elasticsearch.xpack.security.authc.Authentication.RealmRef;
import org.elasticsearch.xpack.security.user.AnonymousUser;
import org.elasticsearch.xpack.security.user.User;
import org.elasticsearch.xpack.security.audit.AuditTrail;
import org.elasticsearch.xpack.security.crypto.CryptoService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportMessage;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.xpack.security.Security.setting;

/**
 * An authentication service that delegates the authentication process to its configured {@link Realm realms}.
 * This service also supports request level caching of authenticated users (i.e. once a user authenticated
 * successfully, it is set on the request context to avoid subsequent redundant authentication process)
 */
public class InternalAuthenticationService extends AbstractComponent implements AuthenticationService {

    public static final Setting<Boolean> SIGN_USER_HEADER =
            Setting.boolSetting(setting("authc.sign_user_header"), true, Property.NodeScope);
    public static final Setting<Boolean> RUN_AS_ENABLED =
            Setting.boolSetting(setting("authc.run_as.enabled"), true, Property.NodeScope);
    public static final String RUN_AS_USER_HEADER = "es-security-runas-user";

    private final Realms realms;
    private final AuditTrail auditTrail;
    private final CryptoService cryptoService;
    private final AuthenticationFailureHandler failureHandler;
    private final ThreadContext threadContext;
    private final String nodeName;
    private final boolean signUserHeader;
    private final boolean runAsEnabled;

    @Inject
    public InternalAuthenticationService(Settings settings, Realms realms, AuditTrailService auditTrail, CryptoService cryptoService,
                                         AuthenticationFailureHandler failureHandler, ThreadPool threadPool) {
        super(settings);
        this.nodeName = Node.NODE_NAME_SETTING.get(settings);
        this.realms = realms;
        this.auditTrail = auditTrail;
        this.cryptoService = cryptoService;
        this.failureHandler = failureHandler;
        this.threadContext = threadPool.getThreadContext();
        this.signUserHeader = SIGN_USER_HEADER.get(settings);
        this.runAsEnabled = RUN_AS_ENABLED.get(settings);
    }

    @Override
    public Authentication authenticate(RestRequest request) throws IOException, ElasticsearchSecurityException {
        return createAuthenticator(request).authenticate();
    }

    @Override
    public Authentication authenticate(String action, TransportMessage message, User fallbackUser) throws IOException {
        return createAuthenticator(action, message, fallbackUser).authenticate();
    }

    @Override
    public void attachUserIfMissing(User user) throws IOException {
        Authentication authentication = new Authentication(user, new RealmRef("__attach", "__attach", nodeName), null);
        authentication.writeToContextIfMissing(threadContext, cryptoService, signUserHeader);
    }

    Authenticator createAuthenticator(RestRequest request) {
        return new Authenticator(request);
    }

    Authenticator createAuthenticator(String action, TransportMessage message, User fallbackUser) {
        return new Authenticator(action, message, fallbackUser);
    }

    class Authenticator {

        private final AuditableRequest request;
        private final User fallbackUser;

        private RealmRef authenticatedBy = null;
        private RealmRef lookedupBy = null;

        Authenticator(RestRequest request) {
            this.request = new Rest(request);
            this.fallbackUser = null;
        }

        Authenticator(String action, TransportMessage message, User fallbackUser) {
            this.request = new Transport(action, message);
            this.fallbackUser = fallbackUser;
        }

        Authentication authenticate() throws IOException, IllegalArgumentException {
            Authentication existing = getCurrentAuthentication();
            if (existing != null) {
                return existing;
            }

            AuthenticationToken token = extractToken();
            if (token == null) {
                return handleNullToken();
            }

            User user = authenticateToken(token);
            if (user == null) {
                throw handleNullUser(token);
            }
            user = lookupRunAsUserIfNecessary(user, token);

            final Authentication authentication = new Authentication(user, authenticatedBy, lookedupBy);
            authentication.writeToContext(threadContext, cryptoService, signUserHeader);
            return authentication;
        }

        Authentication getCurrentAuthentication() {
            Authentication authentication;
            try {
                authentication = Authentication.readFromContext(threadContext, cryptoService, signUserHeader);
            } catch (Exception e) {
                throw request.tamperedRequest();
            }

            // make sure this isn't a rest request since we don't allow authentication to be read via a HTTP request...
            if (authentication != null && request instanceof Rest) {
                throw request.tamperedRequest();
            }
            return authentication;
        }

        AuthenticationToken extractToken() {
            AuthenticationToken token = null;
            try {
                for (Realm realm : realms) {
                    token = realm.token(threadContext);
                    if (token != null) {
                        logger.trace("realm [{}] resolved authentication token [{}] from [{}]", realm, token.principal(), request);
                        break;
                    }
                }
            } catch (Exception e) {
                if (logger.isDebugEnabled()) {
                    logger.debug("failed to extract token from request: [{}]", e, request);
                } else {
                    logger.warn("failed to extract token from request: [{}]: {}", request, e.getMessage());
                }
                throw request.exceptionProcessingRequest(e, null);
            }
            return token;
        }

        Authentication handleNullToken() throws IOException {
            Authentication authentication = null;
            if (fallbackUser != null) {
                RealmRef authenticatedBy = new RealmRef("__fallback", "__fallback", nodeName);
                authentication = new Authentication(fallbackUser, authenticatedBy, null);
            } else if (AnonymousUser.enabled()) {
                RealmRef authenticatedBy = new RealmRef("__anonymous", "__anonymous", nodeName);
                authentication = new Authentication(AnonymousUser.INSTANCE, authenticatedBy, null);
            }

            if (authentication != null) {
                authentication.writeToContext(threadContext, cryptoService, signUserHeader);
                return authentication;
            }
            throw request.anonymousAccessDenied();
        }

        User authenticateToken(AuthenticationToken token) {
            User user = null;
            try {
                for (Realm realm : realms) {
                    if (realm.supports(token)) {
                        user = realm.authenticate(token);
                        if (user != null) {
                            authenticatedBy = new RealmRef(realm.name(), realm.type(), nodeName);
                            break;
                        }
                        request.realmAuthenticationFailed(token, realm.name());
                    }
                }
            } catch (Exception e) {
                logger.debug("authentication failed for principal [{}], [{}] ", e, token.principal(), request);
                throw request.exceptionProcessingRequest(e, token);
            } finally {
                token.clearCredentials();
            }
            return user;
        }

        ElasticsearchSecurityException handleNullUser(AuthenticationToken token) {
            throw request.authenticationFailed(token);
        }

        boolean shouldTryToRunAs(User authenticatedUser, AuthenticationToken token) {
            if (runAsEnabled == false) {
                return false;
            }

            String runAsUsername = threadContext.getHeader(RUN_AS_USER_HEADER);
            if (runAsUsername == null) {
                return false;
            }

            if (runAsUsername.isEmpty()) {
                logger.debug("user [{}] attempted to runAs with an empty username", authenticatedUser.principal());
                throw request.runAsDenied(new User(authenticatedUser.principal(), authenticatedUser.roles(),
                        new User(runAsUsername, Strings.EMPTY_ARRAY)), token);
            }
            return true;
        }

        User lookupRunAsUserIfNecessary(User authenticatedUser, AuthenticationToken token) {
            User user = authenticatedUser;
            if (shouldTryToRunAs(user, token) == false) {
                return user;
            }

            final String runAsUsername = threadContext.getHeader(RUN_AS_USER_HEADER);
            try {
                for (Realm realm : realms) {
                    if (realm.userLookupSupported()) {
                        User runAsUser = realm.lookupUser(runAsUsername);
                        if (runAsUser != null) {
                            lookedupBy = new RealmRef(realm.name(), realm.type(), nodeName);
                            user = new User(user.principal(), user.roles(), runAsUser);
                            return user;
                        }
                    }
                }

                // the requested run as user does not exist, but we don't throw an error here otherwise this could let
                // information leak about users in the system... instead we'll just let the authz service fail throw an
                // authorization error
                user = new User(user.principal(), user.roles(), new User(runAsUsername, Strings.EMPTY_ARRAY));
            } catch (Exception e) {
                logger.debug("run as failed for principal [{}], [{}], run as username [{}]", e, token.principal(), request, runAsUsername);
                throw request.exceptionProcessingRequest(e, token);
            }
            return user;
        }

        abstract class AuditableRequest {

            abstract void realmAuthenticationFailed(AuthenticationToken token, String realm);

            abstract ElasticsearchSecurityException tamperedRequest();

            abstract ElasticsearchSecurityException exceptionProcessingRequest(Exception e, @Nullable AuthenticationToken token);

            abstract ElasticsearchSecurityException authenticationFailed(AuthenticationToken token);

            abstract ElasticsearchSecurityException anonymousAccessDenied();

            abstract ElasticsearchSecurityException runAsDenied(User user, AuthenticationToken token);
        }

        class Transport extends AuditableRequest {

            private final String action;
            private final TransportMessage message;

            Transport(String action, TransportMessage message) {
                this.action = action;
                this.message = message;
            }

            @Override
            void realmAuthenticationFailed(AuthenticationToken token, String realm) {
                auditTrail.authenticationFailed(realm, token, action, message);
            }

            @Override
            ElasticsearchSecurityException tamperedRequest() {
                auditTrail.tamperedRequest(action, message);
                return new ElasticsearchSecurityException("failed to verify signed authentication information");
            }

            @Override
            ElasticsearchSecurityException exceptionProcessingRequest(Exception e, @Nullable AuthenticationToken token) {
                if (token != null) {
                    auditTrail.authenticationFailed(token, action, message);
                } else {
                    auditTrail.authenticationFailed(action, message);
                }
                return failureHandler.exceptionProcessingRequest(message, action, e, threadContext);
            }

            @Override
            ElasticsearchSecurityException authenticationFailed(AuthenticationToken token) {
                auditTrail.authenticationFailed(token, action, message);
                return failureHandler.failedAuthentication(message, token, action, threadContext);
            }

            @Override
            ElasticsearchSecurityException anonymousAccessDenied() {
                auditTrail.anonymousAccessDenied(action, message);
                return failureHandler.missingToken(message, action, threadContext);
            }

            @Override
            ElasticsearchSecurityException runAsDenied(User user, AuthenticationToken token) {
                auditTrail.runAsDenied(user, action, message);
                return failureHandler.failedAuthentication(message, token, action, threadContext);
            }

            public String toString() {
                return "transport request action [" + action + "]";
            }
        }

        class Rest extends AuditableRequest {

            private final RestRequest request;

            Rest(RestRequest request) {
                this.request = request;
            }

            @Override
            void realmAuthenticationFailed(AuthenticationToken token, String realm) {
                auditTrail.authenticationFailed(realm, token, request);
            }

            @Override
            ElasticsearchSecurityException tamperedRequest() {
                auditTrail.tamperedRequest(request);
                return new ElasticsearchSecurityException("rest request attempted to inject a user");
            }

            @Override
            ElasticsearchSecurityException exceptionProcessingRequest(Exception e, @Nullable AuthenticationToken token) {
                if (token != null) {
                    auditTrail.authenticationFailed(token, request);
                } else {
                    auditTrail.authenticationFailed(request);
                }
                return failureHandler.exceptionProcessingRequest(request, e, threadContext);
            }

            @Override
            ElasticsearchSecurityException authenticationFailed(AuthenticationToken token) {
                auditTrail.authenticationFailed(token, request);
                return failureHandler.failedAuthentication(request, token, threadContext);
            }

            @Override
            ElasticsearchSecurityException anonymousAccessDenied() {
                auditTrail.anonymousAccessDenied(request);
                return failureHandler.missingToken(request, threadContext);
            }

            @Override
            ElasticsearchSecurityException runAsDenied(User user, AuthenticationToken token) {
                auditTrail.runAsDenied(user, request);
                return failureHandler.failedAuthentication(request, token, threadContext);
            }

            public String toString() {
                return "rest request uri [" + request.uri() + "]";
            }
        }
    }

    public static void addSettings(List<Setting<?>> settings) {
        settings.add(SIGN_USER_HEADER);
        settings.add(RUN_AS_ENABLED);
    }
}
