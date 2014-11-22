/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.authc;

import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.shield.User;
import org.elasticsearch.transport.TransportMessage;

/**
 * An authentication mechanism to which the default authentication {@link org.elasticsearch.shield.authc.AuthenticationService service}
 * delegates the authentication process. Different realms may be defined, each may be based on different
 * authentication mechanism supporting its own specific authentication token type.
 */
public interface Realm<T extends AuthenticationToken> {

    /**
     * @return The type of this realm
     */
    String type();

    /**
     * @return  {@code true} if this realm supports the given authentication token, {@code false} otherwise.
     */
    boolean supports(AuthenticationToken token);

    /**
     * Attempts to extract an authentication token from the given rest request. If an appropriate token
     * is found it's returned, otherwise {@code null} is returned.
     *
     * @param request   The rest request
     * @return          The authentication token or {@code null} if not found
     */
    T token(RestRequest request);

    /**
     * Attempts to extract an authentication token from the given transport message. If an appropriate token
     * is found it's returned, otherwise {@code null} is returned.
     *
     * @param message   The transport message
     * @return          The authentication token or {@code null} if not found
     */
    T token(TransportMessage<?> message);

    /**
     * Authenticates the given token. A successful authentication will return the User associated
     * with the given token. An unsuccessful authentication returns {@code null}.
     *
     * @param token The authentication token
     * @return      The authenticated user or {@code null} if authentication failed.
     */
    User authenticate(T token);

}
