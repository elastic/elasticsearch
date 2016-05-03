/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.authc;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.Version;
import org.elasticsearch.common.Base64;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsModule;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.shield.user.AnonymousUser;
import org.elasticsearch.shield.user.User;
import org.elasticsearch.shield.audit.AuditTrail;
import org.elasticsearch.shield.crypto.CryptoService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportMessage;

import java.io.IOException;

import static org.elasticsearch.shield.Security.setting;
import static org.elasticsearch.shield.support.Exceptions.authenticationError;

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
    public static final String RUN_AS_USER_HEADER = "es-shield-runas-user";

    static final String TOKEN_KEY = "_shield_token";
    public static final String USER_KEY = "_shield_user";

    private final Realms realms;
    private final AuditTrail auditTrail;
    private final CryptoService cryptoService;
    private final AuthenticationFailureHandler failureHandler;
    private final ThreadContext threadContext;
    private final boolean signUserHeader;
    private final boolean runAsEnabled;

    @Inject
    public InternalAuthenticationService(Settings settings, Realms realms, AuditTrail auditTrail, CryptoService cryptoService,
                                         AuthenticationFailureHandler failureHandler, ThreadPool threadPool, RestController controller) {
        super(settings);
        this.realms = realms;
        this.auditTrail = auditTrail;
        this.cryptoService = cryptoService;
        this.failureHandler = failureHandler;
        this.threadContext = threadPool.getThreadContext();
        this.signUserHeader = SIGN_USER_HEADER.get(settings);
        this.runAsEnabled = RUN_AS_ENABLED.get(settings);
        if (runAsEnabled) {
            controller.registerRelevantHeaders(RUN_AS_USER_HEADER);
        }
    }

    @Override
    public User authenticate(RestRequest request) throws IOException, ElasticsearchSecurityException {
        return authenticate(newRequest(request), (User) null);
    }

    @Override
    public User authenticate(String action, TransportMessage message, User fallbackUser) throws IOException {
        return authenticate(newRequest(action, message), fallbackUser);
    }

    User authenticate(AuditableRequest request, User fallbackUser) throws IOException {
        User user = getUserFromContext();
        if (user != null) {
            return user;
        }

        String header = threadContext.getHeader(USER_KEY);
        if (header != null) {
            if (request instanceof Rest) {
                request.tamperedRequest();
                throw new ElasticsearchSecurityException("rest request attempted to inject a user");
            }
            if (signUserHeader) {
                try {
                    header = cryptoService.unsignAndVerify(header);
                } catch (Exception e) {
                    request.tamperedRequest();
                    throw e;
                }
            }
            user = decodeUser(header);
            assert user != null;
            putUserInContext(user);
        } else {
            user = authenticateWithRealms(request, fallbackUser);
            setUser(user);
        }
        return user;
    }

    @Override
    public void attachUserHeaderIfMissing(User user) throws IOException {
        if (threadContext.getHeader(USER_KEY) != null) {
            return;
        }
        User transientUser = threadContext.getTransient(USER_KEY);
        if (transientUser != null) {
            setUserHeader(transientUser);
            return;
        }

        setUser(user);
    }

    @Override
    public User getCurrentUser() {
        return getUserFromContext();
    }

    void setUserHeader(User user) throws IOException {
        String userHeader = signUserHeader ? cryptoService.sign(encodeUser(user, logger)) : encodeUser(user, logger);
        threadContext.putHeader(USER_KEY, userHeader);
    }

    void setUser(User user) throws IOException {
        putUserInContext(user);
        setUserHeader(user);
    }

    void putUserInContext(User user) {
        if (threadContext.getTransient(USER_KEY) != null) {
            User ctxUser = threadContext.getTransient(USER_KEY);
            throw new IllegalArgumentException("context already has user [" + ctxUser.principal() + "]. trying to set user [" + user
                    .principal() + "]");
        }
        threadContext.putTransient(USER_KEY, user);
    }

    User getUserFromContext() {
        return threadContext.getTransient(USER_KEY);
    }

    static User decodeUser(String text) {
        try {
            byte[] bytes = Base64.decode(text);
            StreamInput input = StreamInput.wrap(bytes);
            Version version = Version.readVersion(input);
            input.setVersion(version);
            return User.readFrom(input);
        } catch (IOException ioe) {
            throw authenticationError("could not read authenticated user", ioe);
        }
    }

    static String encodeUser(User user, ESLogger logger) {
        try {
            BytesStreamOutput output = new BytesStreamOutput();
            Version.writeVersion(Version.CURRENT, output);
            User.writeTo(user, output);
            byte[] bytes = output.bytes().toBytes();
            return Base64.encodeBytes(bytes);
        } catch (IOException ioe) {
            if (logger != null) {
                logger.error("could not encode authenticated user in message header... falling back to token headers", ioe);
            }
            return null;
        }
    }

    /**
     * Authenticates the user associated with the given request by delegating the authentication to
     * the configured realms. Each realm that supports the given token will be asked to perform authentication,
     * the first realm that successfully authenticates will "win" and its authenticated user will be returned.
     * If none of the configured realms successfully authenticates the request, an {@link ElasticsearchSecurityException}
     * will be thrown.
     * <p>
     * The order by which the realms are checked is defined in {@link Realms}.
     *
     * @param request the request to authenticate
     * @param fallbackUser The user to assume if there is not other user attached to the message
     * @return The authenticated user
     * @throws ElasticsearchSecurityException If none of the configured realms successfully authenticated the
     *                                        request
     */
    User authenticateWithRealms(AuditableRequest request, User fallbackUser) throws ElasticsearchSecurityException {
        AuthenticationToken token;
        try {
            token = token(request);
        } catch (Exception e) {
            if (logger.isDebugEnabled()) {
                logger.debug("failed to extract token from request: [{}]", e, request);
            } else {
                logger.warn("failed to extract token from request: [{}]", request, e.getMessage());
            }
            throw request.exceptionProcessingRequest(e);
        }

        if (token == null) {
            if (fallbackUser != null) {
                return fallbackUser;
            }
            if (AnonymousUser.enabled()) {
                return AnonymousUser.INSTANCE;
            }
            throw request.anonymousAccessDenied();
        }

        User user;
        try {
            user = authenticate(request, token);
        } catch (Exception e) {
            if (logger.isDebugEnabled()) {
                logger.debug("authentication failed for principal [{}], [{}]", e, request);
            }
            throw request.exceptionProcessingRequest(e, token);
        }

        if (user == null) {
            throw request.failedAuthentication(token);
        }

        if (runAsEnabled) {
            String runAsUsername = threadContext.getHeader(RUN_AS_USER_HEADER);
            if (runAsUsername != null) {
                if (runAsUsername.isEmpty()) {
                    logger.warn("user [{}] attempted to runAs with an empty username", user.principal());
                    throw request.failedAuthentication(token);
                }
                User runAsUser;
                try {
                    runAsUser = lookupUser(runAsUsername);
                } catch (Exception e) {
                    if (logger.isDebugEnabled()) {
                        logger.debug("lookup of run as user failed for principal [{}], [{}], run as username [{}]", e,
                                token.principal(), request, runAsUsername);
                    }
                    throw request.exceptionProcessingRequest(e, token);
                }

                // wrap in a try catch because the user constructor could throw an exception if we are trying to runAs the system user
                try {
                    if (runAsUser != null) {
                        user = new User(user.principal(), user.roles(), runAsUser);
                    } else {
                        // the requested run as user does not exist, but we don't throw an error here otherwise this could let
                        // information leak about users in the system... instead we'll just let the authz service fail throw an
                        // authorization error
                        user = new User(user.principal(), user.roles(), new User(runAsUsername, Strings.EMPTY_ARRAY));
                    }
                } catch (Exception e) {
                    if (logger.isDebugEnabled()) {
                        logger.debug("user creation failed for principal [{}], [{}], run as username [{}]", e, token.principal(),
                                request, runAsUsername);
                    }
                    throw request.exceptionProcessingRequest(e, token);
                }
            }
        }
        return user;
    }

    User authenticate(AuditableRequest request, AuthenticationToken token) throws ElasticsearchSecurityException {
        assert token != null : "cannot authenticate null tokens";
        try {
            for (Realm realm : realms) {
                if (realm.supports(token)) {
                    User user = realm.authenticate(token);
                    if (user != null) {
                        return user;
                    }
                    request.authenticationFailed(token, realm.name());
                }
            }
            request.authenticationFailed(token);
            return null;
        } finally {
            token.clearCredentials();
        }
    }

    AuthenticationToken token(AuditableRequest request) throws ElasticsearchSecurityException {
        for (Realm realm : realms) {
            AuthenticationToken token = realm.token(threadContext);
            if (token != null) {
                request.tokenResolved(realm.name(), token);
                return token;
            }
        }
        return null;
    }

    User lookupUser(String username) {
        for (Realm realm : realms) {
            if (realm.userLookupSupported()) {
                User user = realm.lookupUser(username);
                if (user != null) {
                    return user;
                }
            }
        }
        return null;
    }

    public static void registerSettings(SettingsModule settingsModule) {
        settingsModule.registerSetting(SIGN_USER_HEADER);
        settingsModule.registerSetting(RUN_AS_ENABLED);
    }

    // these methods are package private for testing. They are also needed so that a AuditableRequest can be created in tests
    AuditableRequest newRequest(String action, TransportMessage message) {
        return new Transport(action, message);
    }

    AuditableRequest newRequest(RestRequest request) {
        return new Rest(request);
    }

    abstract class AuditableRequest {

        abstract void authenticationFailed(AuthenticationToken token);

        abstract void authenticationFailed(AuthenticationToken token, String realm);

        abstract void tamperedRequest();

        abstract void tokenResolved(String realm, AuthenticationToken token);

        abstract ElasticsearchSecurityException exceptionProcessingRequest(Exception e);

        abstract ElasticsearchSecurityException exceptionProcessingRequest(Exception e, AuthenticationToken token);

        abstract ElasticsearchSecurityException failedAuthentication(AuthenticationToken token);

        abstract ElasticsearchSecurityException anonymousAccessDenied();
    }

    class Transport extends AuditableRequest {

        private final String action;
        private final TransportMessage message;

        Transport(String action, TransportMessage message) {
            this.action = action;
            this.message = message;
        }

        @Override
        void authenticationFailed(AuthenticationToken token) {
            auditTrail.authenticationFailed(token, action, message);
        }

        @Override
        void authenticationFailed(AuthenticationToken token, String realm) {
            auditTrail.authenticationFailed(realm, token, action, message);
        }

        @Override
        void tamperedRequest() {
            auditTrail.tamperedRequest(action, message);
        }

        @Override
        void tokenResolved(String realm, AuthenticationToken token) {
            logger.trace("realm [{}] resolved authentication token [{}] from transport request with action [{}]",
                    realm, token.principal(), action);
        }

        @Override
        ElasticsearchSecurityException exceptionProcessingRequest(Exception e) {
            auditTrail.authenticationFailed(action, message);
            return failureHandler.exceptionProcessingRequest(message, action, e, threadContext);
        }

        @Override
        ElasticsearchSecurityException exceptionProcessingRequest(Exception e, AuthenticationToken token) {
            authenticationFailed(token);
            return failureHandler.exceptionProcessingRequest(message, action, e, threadContext);
        }

        ElasticsearchSecurityException failedAuthentication(AuthenticationToken token) {
            auditTrail.authenticationFailed(token, action, message);
            return failureHandler.failedAuthentication(message, token, action, threadContext);
        }

        @Override
        ElasticsearchSecurityException anonymousAccessDenied() {
            auditTrail.anonymousAccessDenied(action, message);
            return failureHandler.missingToken(message, action, threadContext);
        }

        public String toString() {
            return "transport action [" + action + "]";
        }
    }

    class Rest extends AuditableRequest {

        private final RestRequest request;

        Rest(RestRequest request) {
            this.request = request;
        }

        @Override
        void authenticationFailed(AuthenticationToken token) {
            auditTrail.authenticationFailed(token, request);
        }

        @Override
        void authenticationFailed(AuthenticationToken token, String realm) {
            auditTrail.authenticationFailed(realm, token, request);
        }

        @Override
        void tamperedRequest() {
            auditTrail.tamperedRequest(request);
        }

        @Override
        void tokenResolved(String realm, AuthenticationToken token) {
            logger.trace("realm [{}] resolved authentication token [{}] from rest request with uri [{}]",
                    realm, token.principal(), request.uri());
        }

        @Override
        ElasticsearchSecurityException exceptionProcessingRequest(Exception e) {
            auditTrail.authenticationFailed(request);
            return failureHandler.exceptionProcessingRequest(request, e, threadContext);
        }

        @Override
        ElasticsearchSecurityException exceptionProcessingRequest(Exception e, AuthenticationToken token) {
            authenticationFailed(token);
            return failureHandler.exceptionProcessingRequest(request, e, threadContext);
        }

        ElasticsearchSecurityException failedAuthentication(AuthenticationToken token) {
            auditTrail.authenticationFailed(token, request);
            return failureHandler.failedAuthentication(request, token, threadContext);
        }

        @Override
        ElasticsearchSecurityException anonymousAccessDenied() {
            auditTrail.anonymousAccessDenied(request);
            return failureHandler.missingToken(request, threadContext);
        }

        public String toString() {
            return "rest uri [" + request.uri() + "]";
        }
    }
}
