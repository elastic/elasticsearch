/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.authc;

import org.elasticsearch.shield.User;
import org.elasticsearch.transport.TransportMessage;

/**
 * Responsible for authenticating the Users behind requests
 */
public interface AuthenticationService {

    /**
     * Extracts the authenticate token from the given message. If no recognized auth token is associated
     * with the message, {@code null} is returned.
     */
    AuthenticationToken token(TransportMessage<?> message);

    /**
     * Authenticates the user associated with the given request based on the given authentication token.
     *
     * On successful authentication, the {@link org.elasticsearch.shield.User user} that is associated
     * with the request (i.e. that is associated with the token's {@link AuthenticationToken#principal() principal})
     * will be returned. If authentication fails, an {@link AuthenticationException} will be thrown.
     *
     * @param action    The executed action
     * @param message   The executed message
     * @param token     The authentication token associated with the given request (must not be {@code null})
     * @return          The authenticated User
     * @throws AuthenticationException  If no user could be authenticated (can either be due to missing
     *                                  supported authentication token, or simply due to bad credentials.
     */
    User authenticate(String action, TransportMessage<?> message, AuthenticationToken token) throws AuthenticationException;

}
