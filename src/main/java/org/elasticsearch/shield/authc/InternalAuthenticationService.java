/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.authc;

import org.elasticsearch.common.Base64;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.shield.User;
import org.elasticsearch.shield.audit.AuditTrail;
import org.elasticsearch.shield.crypto.CryptoService;
import org.elasticsearch.transport.TransportMessage;

import java.io.IOException;

/**
 * An authentication service that delegates the authentication process to its configured {@link Realm realms}.
 * This service also supports request level caching of authenticated users (i.e. once a user authenticated
 * successfully, it is set on the request context to avoid subsequent redundant authentication process)
 */
public class InternalAuthenticationService extends AbstractComponent implements AuthenticationService {

    public static final String SETTING_SIGN_USER_HEADER = "shield.authc.sign_user_header";

    static final String TOKEN_KEY = "_shield_token";
    static final String USER_KEY = "_shield_user";

    private final Realms realms;
    private final AuditTrail auditTrail;
    private final CryptoService cryptoService;
    private final AnonymousService anonymousService;
    private final boolean signUserHeader;

    @Inject
    public InternalAuthenticationService(Settings settings, Realms realms, AuditTrail auditTrail, CryptoService cryptoService, AnonymousService anonymousService) {
        super(settings);
        this.realms = realms;
        this.auditTrail = auditTrail;
        this.cryptoService = cryptoService;
        this.anonymousService = anonymousService;
        this.signUserHeader = settings.getAsBoolean(SETTING_SIGN_USER_HEADER, true);
    }

    @Override
    public User authenticate(RestRequest request) throws AuthenticationException {
        AuthenticationToken token;
        try {
            token = token(request);
        } catch (Exception e) {
            if (logger.isDebugEnabled()) {
                logger.debug("failed to extract token from request", e);
            } else {
                logger.warn("failed to extract token from request: ", e.getMessage());
            }
            auditTrail.authenticationFailed(request);

            if (e instanceof AuthenticationException) {
                throw e;
            }
            throw new AuthenticationException("error attempting to authenticate request", e);
        }

        if (token == null) {
            if (anonymousService.enabled()) {
                // we must put the user in the request context, so it'll be copied to the
                // transport request - without it, the transport will assume system user
                request.putInContext(USER_KEY, anonymousService.anonymousUser());
                return anonymousService.anonymousUser();
            }
            auditTrail.anonymousAccessDenied(request);
            throw new AuthenticationException("missing authentication token for REST request [" + request.uri() + "]");
        }

        User user;
        try {
            user = authenticate(request, token);
        } catch (Exception e) {
            if (logger.isDebugEnabled()) {
                logger.debug("authentication of request failed for principal [{}], uri [{}]", e, token.principal(), request.uri());
            }
            auditTrail.authenticationFailed(token, request);
            if (e instanceof AuthenticationException) {
                throw e;
            }
            throw new AuthenticationException("error attempting to authenticate request", e);
        }

        if (user == null) {
            throw new AuthenticationException("unable to authenticate user [" + token.principal() + "] for REST request [" + request.uri() + "]");
        }
        // we must put the user in the request context, so it'll be copied to the
        // transport request - without it, the transport will assume system user
        request.putInContext(USER_KEY, user);
        return user;
    }

    @Override
    public User authenticate(String action, TransportMessage message, User fallbackUser) {
        User user = message.getFromContext(USER_KEY);
        if (user != null) {
            return user;
        }
        String header = message.getHeader(USER_KEY);
        if (header != null) {
            if (signUserHeader) {
                header = cryptoService.unsignAndVerify(header);
            }
            user = decodeUser(header);
        }
        if (user == null) {
            user = authenticateWithRealms(action, message, fallbackUser);
            header = signUserHeader ? cryptoService.sign(encodeUser(user, logger)) : encodeUser(user, logger);
            message.putHeader(USER_KEY, header);
        }
        message.putInContext(USER_KEY, user);
        return user;
    }

    @Override
    public void attachUserHeaderIfMissing(TransportMessage message, User user) {
        if (message.hasHeader(USER_KEY)) {
            return;
        }
        User userFromContext = message.getFromContext(USER_KEY);
        if (userFromContext != null) {
            String userHeader = signUserHeader ? cryptoService.sign(encodeUser(userFromContext, logger)) : encodeUser(userFromContext, logger);
            message.putHeader(USER_KEY, userHeader);
            return;
        }

        message.putInContext(USER_KEY, user);
        String userHeader = signUserHeader ? cryptoService.sign(encodeUser(user, logger)) : encodeUser(user, logger);
        message.putHeader(USER_KEY, userHeader);
    }

    static User decodeUser(String text) {
        try {
            byte[] bytes = Base64.decode(text);
            StreamInput input = StreamInput.wrap(bytes);
            return User.readFrom(input);
        } catch (IOException ioe) {
            throw new AuthenticationException("could not read authenticated user", ioe);
        }
    }

    static String encodeUser(User user, ESLogger logger) {
        try {
            BytesStreamOutput output = new BytesStreamOutput();
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
     * If none of the configured realms successfully authenticates the request, an {@link AuthenticationException} will
     * be thrown.
     * <p/>
     * The order by which the realms are checked is defined in {@link Realms}.
     *
     * @param action       The executed action
     * @param message      The executed request
     * @param fallbackUser The user to assume if there is not other user attached to the message
     *
     * @return The authenticated user
     *
     * @throws AuthenticationException If none of the configured realms successfully authenticated the
     *                                 request
     */
    @SuppressWarnings("unchecked")
    User authenticateWithRealms(String action, TransportMessage<?> message, User fallbackUser) throws AuthenticationException {
        AuthenticationToken token;
        try {
            token = token(action, message);
        } catch (Exception e) {
            if (logger.isDebugEnabled()) {
                logger.debug("failed to extract token from transport message", e);
            } else {
                logger.warn("failed to extract token from transport message: ", e.getMessage());
            }
            auditTrail.authenticationFailed(action, message);
            if (e instanceof AuthenticationException) {
                throw e;
            }
            throw new AuthenticationException("error attempting to authenticate request", e);
        }

        if (token == null) {
            if (fallbackUser != null) {
                return fallbackUser;
            }
            if (anonymousService.enabled()) {
                return anonymousService.anonymousUser();
            }
            auditTrail.anonymousAccessDenied(action, message);
            throw new AuthenticationException("missing authentication token for action [" + action + "]");
        }

        User user;
        try {
            user = authenticate(message, token, action);
        } catch (Exception e) {
            if (logger.isDebugEnabled()) {
                logger.debug("authentication of transport message failed for principal [{}], action [{}]", e, token.principal(), action);
            }
            auditTrail.authenticationFailed(token, action, message);
            if (e instanceof AuthenticationException) {
                throw e;
            }
            throw new AuthenticationException("error attempting to authenticate request", e);
        }

        if (user == null) {
            throw new AuthenticationException("unable to authenticate user [" + token.principal() + "] for action [" + action + "]");
        }
        return user;
    }

    User authenticate(TransportMessage<?> message, AuthenticationToken token, String action) throws AuthenticationException {
        assert token != null : "cannot authenticate null tokens";
        try {
            for (Realm realm : realms) {
                if (realm.supports(token)) {
                    User user = realm.authenticate(token);
                    if (user != null) {
                        return user;
                    }
                    auditTrail.authenticationFailed(realm.type(), token, action, message);
                }
            }
            auditTrail.authenticationFailed(token, action, message);
            return null;
        } finally {
            token.clearCredentials();
        }
    }

    User authenticate(RestRequest request, AuthenticationToken token) throws AuthenticationException {
        assert token != null : "cannot authenticate null tokens";
        try {
            for (Realm realm : realms) {
                if (realm.supports(token)) {
                    User user = realm.authenticate(token);
                    if (user != null) {
                        return user;
                    }
                    auditTrail.authenticationFailed(realm.type(), token, request);
                }
            }
            auditTrail.authenticationFailed(token, request);
            return null;
        } finally {
            token.clearCredentials();
        }
    }

    AuthenticationToken token(RestRequest request) throws AuthenticationException {
        for (Realm realm : realms) {
            AuthenticationToken token = realm.token(request);
            if (token != null) {
                request.putInContext(TOKEN_KEY, token);
                return token;
            }
        }
        return null;
    }

    @SuppressWarnings("unchecked")
    AuthenticationToken token(String action, TransportMessage<?> message) {
        AuthenticationToken token = message.getFromContext(TOKEN_KEY);
        if (token != null) {
            return token;
        }
        for (Realm realm : realms) {
            token = realm.token(message);
            if (token != null) {

                if (logger.isTraceEnabled()) {
                    logger.trace("realm [{}] resolved authentication token [{}] from transport request with action [{}]", realm, token.principal(), action);
                }

                message.putInContext(TOKEN_KEY, token);
                return token;
            }
        }
        return null;
    }
}
