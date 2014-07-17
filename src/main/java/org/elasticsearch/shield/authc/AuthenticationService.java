/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.authc;

import org.elasticsearch.shield.User;
import org.elasticsearch.transport.TransportRequest;

/**
 * Responsible for authenticating the Users behind requests
 */
public interface AuthenticationService {

    /**
     * Authenticates the user associated with the given request.
     *
     * An {@link AuthenticationToken authentication token} will be extracted from the request, and
     * will be authenticated. On successful authentication, the {@link org.elasticsearch.shield.User user} that is associated
     * with the request (i.e. that is associated with the token's {@link AuthenticationToken#principal() principal})
     * will be returned.
     *
     * @param request   The executed request
     * @return          The authenticated User
     * @throws AuthenticationException  If no user could be authenticated (can either be due to missing
     *                                  supported authentication token, or simply due to bad credentials.
     */
    User authenticate(String action, TransportRequest request) throws AuthenticationException;

}
