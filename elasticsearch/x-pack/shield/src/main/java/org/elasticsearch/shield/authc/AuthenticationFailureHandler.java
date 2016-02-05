/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.authc;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.transport.TransportMessage;

/**
 * A AuthenticationFailureHandler is responsible for the handling of a request that has failed authentication. This must
 * consist of returning an exception and this exception can have headers to indicate authentication is required or another
 * HTTP operation such as a redirect.
 * <p>
 * For example, when using Basic authentication, most clients wait to send credentials until they have been challenged
 * for them. In this workflow a client makes a request, the server responds with a 401 status with the header
 * <code>WWW-Authenticate: Basic realm=auth-realm</code>, and then the client will send credentials. The same scheme also
 * applies for other methods of authentication, with changes to the value provided in the WWW-Authenticate header.
 * <p>
 * Additionally, some methods of authentication may require a different status code. When using an single sign on system,
 * clients will often retrieve a token from a single sign on system that is presented to the server and verified. When a
 * client does not provide such a token, then the server can choose to redirect the client to the single sign on system to
 * retrieve a token. This can be accomplished in the AuthenticationFailureHandler by setting the
 * {@link org.elasticsearch.rest.RestStatus#FOUND}
 * with a <code>Location</code> header that contains the location to redirect the user to.
 */
public interface AuthenticationFailureHandler {

    /**
     * This method is called when there has been an authentication failure for the given REST request and authentication
     * token.
     *
     * @param request The request that could not be authenticated
     * @param token   The token that was extracted from the request
     * @return ElasticsearchSecurityException with the appropriate headers and message
     */
    ElasticsearchSecurityException unsuccessfulAuthentication(RestRequest request, AuthenticationToken token);

    /**
     * This method is called when there has been an authentication failure for the given message and token
     *
     * @param message The transport message that could not be authenticated
     * @param token   The token that was extracted from the message
     * @param action  The name of the action that the message is trying to perform
     * @return ElasticsearchSecurityException with the appropriate headers and message
     */
    ElasticsearchSecurityException unsuccessfulAuthentication(TransportMessage message, AuthenticationToken token, String action);

    /**
     * The method is called when an exception has occurred while processing the REST request. This could be an error that
     * occurred while attempting to extract a token or while attempting to authenticate the request
     *
     * @param request The request that was being authenticated when the exception occurred
     * @param e       The exception that was thrown
     * @return ElasticsearchSecurityException with the appropriate headers and message
     */
    ElasticsearchSecurityException exceptionProcessingRequest(RestRequest request, Exception e);

    /**
     * The method is called when an exception has occurred while processing the transport message. This could be an error that
     * occurred while attempting to extract a token or while attempting to authenticate the request
     *
     * @param message The message that was being authenticated when the exception occurred
     * @param e       The exception that was thrown
     * @return ElasticsearchSecurityException with the appropriate headers and message
     */
    ElasticsearchSecurityException exceptionProcessingRequest(TransportMessage message, Exception e);

    /**
     * This method is called when a REST request is received and no authentication token could be extracted AND anonymous
     * access is disabled. If anonymous access is enabled, this method will not be called
     *
     * @param request The request that did not have a token
     * @return ElasticsearchSecurityException with the appropriate headers and message
     */
    ElasticsearchSecurityException missingToken(RestRequest request);

    /**
     * This method is called when a transport message is received and no authentication token could be extracted AND
     * anonymous access is disabled. If anonymous access is enabled this method will not be called
     *
     * @param message The message that did not have a token
     * @param action  The name of the action that the message is trying to perform
     * @return ElasticsearchSecurityException with the appropriate headers and message
     */
    ElasticsearchSecurityException missingToken(TransportMessage message, String action);

    /**
     * This method is called when anonymous access is enabled, a request does not pass authorization with the anonymous
     * user, AND the anonymous service is configured to thrown a authentication exception instead of a authorization
     * exception
     *
     * @param action the action that failed authorization for anonymous access
     * @return ElasticsearchSecurityException with the appropriate headers and message
     */
    ElasticsearchSecurityException authenticationRequired(String action);
}
