/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authc;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.AuthenticationResult;
import org.elasticsearch.xpack.core.security.authc.AuthenticationToken;
import org.elasticsearch.xpack.core.security.support.Exceptions;

import static org.elasticsearch.core.Strings.format;

class CrossClusterAuthenticator implements Authenticator {

    private static final Logger logger = LogManager.getLogger(CrossClusterAuthenticator.class);

    private final CrossClusterService crossClusterService;
    private final String nodeName;

    CrossClusterAuthenticator(CrossClusterService crossClusterService, String nodeName) {
        this.crossClusterService = crossClusterService;
        this.nodeName = nodeName;
    }

    @Override
    public String name() {
        return "Cross Cluster";
    }

    @Override
    public AuthenticationToken extractCredentials(Context context) {
        return crossClusterService.getCredentialsFromHeader(context.getThreadContext());
    }

    @Override
    public void authenticate(Context context, ActionListener<AuthenticationResult<Authentication>> listener) {
        final AuthenticationToken authenticationToken = context.getMostRecentAuthenticationToken();
        if (false == authenticationToken instanceof CrossClusterService.CrossClusterCredentials) {
            listener.onResponse(AuthenticationResult.notHandled());
            return;
        }
        CrossClusterService.CrossClusterCredentials apiKeyCredentials = (CrossClusterService.CrossClusterCredentials) authenticationToken;
        crossClusterService.tryAuthenticate(context.getThreadContext(), apiKeyCredentials, ActionListener.wrap(authResult -> {
            if (authResult.isAuthenticated()) {
                final Authentication authentication = Authentication.newApiKeyAuthentication(authResult, nodeName);
                listener.onResponse(AuthenticationResult.success(authentication));
            } else if (authResult.getStatus() == AuthenticationResult.Status.TERMINATE) {
                Exception e = (authResult.getException() != null)
                    ? authResult.getException()
                    : Exceptions.authenticationError(authResult.getMessage());
                logger.debug(() -> "API key service terminated authentication for request [" + context.getRequest() + "]", e);
                listener.onFailure(e);
            } else {
                if (authResult.getMessage() != null) {
                    if (authResult.getException() != null) {
                        logger.warn(
                            () -> format("Authentication using apikey failed - %s", authResult.getMessage()),
                            authResult.getException()
                        );
                    } else {
                        logger.warn("Authentication using apikey failed - {}", authResult.getMessage());
                    }
                }
                listener.onResponse(AuthenticationResult.unsuccessful(authResult.getMessage(), authResult.getException()));
            }
        }, e -> listener.onFailure(context.getRequest().exceptionProcessingRequest(e, null))));
    }
}
