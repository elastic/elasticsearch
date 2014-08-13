/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.authc;

import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.internal.Nullable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.shield.User;
import org.elasticsearch.shield.audit.AuditTrail;
import org.elasticsearch.transport.TransportMessage;

/**
 * An authentication service that delegates the authentication process to its configured {@link Realm realms}.
 * This service also supports request level caching of authenticated users (i.e. once a user authenticated
 * successfully, it is set on the request context to avoid subsequent redundant authentication process)
 */
public class InternalAuthenticationService extends AbstractComponent implements AuthenticationService {

    static final String TOKEN_CTX_KEY = "_shield_token";
    static final String USER_CTX_KEY = "_shield_user";

    private final Realm[] realms;
    private final AuditTrail auditTrail;

    @Inject
    public InternalAuthenticationService(Settings settings, Realms realms, @Nullable AuditTrail auditTrail) {
        super(settings);
        this.realms = realms.realms();
        this.auditTrail = auditTrail;
    }

    @Override
    public AuthenticationToken token(String action, TransportMessage<?> message) {
        return token(action, message, null);
    }

    @Override
    @SuppressWarnings("unchecked")
    public AuthenticationToken token(String action, TransportMessage<?> message, AuthenticationToken defaultToken) {
        AuthenticationToken token = (AuthenticationToken) message.context().get(TOKEN_CTX_KEY);
        if (token != null) {
            return token;
        }
        for (Realm realm : realms) {
            token = realm.token(message);
            if (token != null) {
                message.context().put(TOKEN_CTX_KEY, token);
                return token;
            }
        }

        if (defaultToken == null) {
            if (auditTrail != null) {
                auditTrail.anonymousAccess(action, message);
            }
            throw new AuthenticationException("Missing authentication token for request [" + action + "]");
        }

        message.context().put(TOKEN_CTX_KEY, defaultToken);
        return defaultToken;
    }

    /**
     * Authenticates the user associated with the given request by delegating the authentication to
     * the configured realms. Each realm that supports the given token will be asked to perform authentication,
     * the first realm that successfully authenticates will "win" and its authenticated user will be returned.
     * If none of the configured realms successfully authenticates the request, an {@link AuthenticationException} will
     * be thrown.
     *
     * The order by which the realms are checked is defined in {@link Realms}.
     *
     * @param action    The executed action
     * @param message   The executed request
     * @param token     The authentication token
     * @return          The authenticated user
     * @throws AuthenticationException If none of the configured realms successfully authenticated the
     *                                 request
     */
    @Override
    @SuppressWarnings("unchecked")
    public User authenticate(String action, TransportMessage<?> message, AuthenticationToken token) throws AuthenticationException {
        assert token != null : "cannot authenticate null tokens";
        User user = (User) message.context().get(USER_CTX_KEY);
        if (user != null) {
            return user;
        }
        for (Realm realm : realms) {
            if (realm.supports(token)) {
                user = realm.authenticate(token);
                if (user != null) {
                    message.context().put(USER_CTX_KEY, user);
                    return user;
                } else if (auditTrail != null) {
                    auditTrail.authenticationFailed(realm.type(), token, action, message);
                }
            }
        }
        if (auditTrail != null) {
            auditTrail.authenticationFailed(token, action, message);
        }
        throw new AuthenticationException("Unable to authenticate user for request");
    }
}
