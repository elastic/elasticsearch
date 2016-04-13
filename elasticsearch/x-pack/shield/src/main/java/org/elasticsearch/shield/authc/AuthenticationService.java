/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.authc;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.shield.user.User;
import org.elasticsearch.transport.TransportMessage;

import java.io.IOException;

/**
 * Responsible for authenticating the Users behind requests
 */
public interface AuthenticationService {

    /**
     * Authenticates the user that is associated with the given request. If the user was authenticated successfully (i.e.
     * a user was indeed associated with the request and the credentials were verified to be valid), the method returns
     * the user and that user is then "attached" to the request's context.
     *
     * @param request   The request to be authenticated
     * @return          The authenticated user
     * @throws ElasticsearchSecurityException   If no user was associated with the request or if the associated
     *                                          user credentials were found to be invalid
     * @throws IOException If an error occurs when reading or writing
     */
    User authenticate(RestRequest request) throws IOException, ElasticsearchSecurityException;

    /**
     * Authenticates the user that is associated with the given message. If the user was authenticated successfully (i.e.
     * a user was indeed associated with the request and the credentials were verified to be valid), the method returns
     * the user and that user is then "attached" to the message's context. If no user was found to be attached to the given
     * message, the the given fallback user will be returned instead.
     *
     * @param action        The action of the message
     * @param message       The message to be authenticated
     * @param fallbackUser  The default user that will be assumed if no other user is attached to the message. Can be
     *                      {@code null}, in which case there will be no fallback user and the success/failure of the
     *                      authentication will be based on the whether there's an attached user to in the message and
     *                      if there is, whether its credentials are valid.
     *
     * @return              The authenticated user (either the attached one or if there isn't the fallback one if provided)
     *
     * @throws ElasticsearchSecurityException   If the associated user credentials were found to be invalid or in the
     *                                          case where there was no user associated with the request, if the defautl
 *                                              token could not be authenticated.
     */
    User authenticate(String action, TransportMessage message, User fallbackUser) throws IOException;

    /**
     * Checks if there's already a user header attached to the given message. If missing, a new header is
     * set on the message with the given user (encoded).
     *
     * @param user      The user to be attached if the header is missing
     */
    void attachUserHeaderIfMissing(User user) throws IOException;

    User getCurrentUser();
}
