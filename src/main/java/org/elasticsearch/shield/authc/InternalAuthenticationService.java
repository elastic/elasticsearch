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
 */
public class InternalAuthenticationService extends AbstractComponent implements AuthenticationService {

    private final Realm[] realms;
    private final AuditTrail auditTrail;

    @Inject
    public InternalAuthenticationService(Settings settings, Realms realms, @Nullable AuditTrail auditTrail) {
        super(settings);
        this.realms = realms.realms();
        this.auditTrail = auditTrail;
    }

    @Override
    public AuthenticationToken token(TransportMessage<?> message) {
        for (Realm realm : realms) {
            AuthenticationToken token = realm.token(message);
            if (token != null) {
                return token;
            }
        }
        return null;
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
        for (Realm realm : realms) {
            if (realm.supports(token)) {
                User user = realm.authenticate(token);
                if (user != null) {
                    return user;
                } else if (auditTrail != null) {
                    auditTrail.authenticationFailed(realm.type(), token, action, message);
                }
            }
        }
        if (auditTrail != null) {
            auditTrail.anonymousAccess(action, message);
        }
        throw new AuthenticationException("Unable to authenticate user for request");
    }
}
