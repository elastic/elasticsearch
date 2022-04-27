/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authc;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.AuthenticationResult;
import org.elasticsearch.xpack.core.security.authc.AuthenticationToken;
import org.elasticsearch.xpack.security.authc.service.ServiceAccountService;
import org.elasticsearch.xpack.security.authc.service.ServiceAccountToken;

class ServiceAccountAuthenticator implements Authenticator {

    private static final Logger logger = LogManager.getLogger(ServiceAccountAuthenticator.class);
    private final ServiceAccountService serviceAccountService;
    private final String nodeName;

    ServiceAccountAuthenticator(ServiceAccountService serviceAccountService, String nodeName) {
        this.serviceAccountService = serviceAccountService;
        this.nodeName = nodeName;
    }

    @Override
    public String name() {
        return "service account";
    }

    @Override
    public AuthenticationToken extractCredentials(Context context) {
        final SecureString bearerString = context.getBearerString();
        if (bearerString == null) {
            return null;
        }
        return ServiceAccountService.tryParseToken(bearerString);
    }

    @Override
    public void authenticate(Context context, ActionListener<AuthenticationResult<Authentication>> listener) {
        final AuthenticationToken authenticationToken = context.getMostRecentAuthenticationToken();
        if (false == authenticationToken instanceof ServiceAccountToken) {
            listener.onResponse(AuthenticationResult.notHandled());
            return;
        }
        final ServiceAccountToken serviceAccountToken = (ServiceAccountToken) authenticationToken;
        serviceAccountService.authenticateToken(serviceAccountToken, nodeName, ActionListener.wrap(authentication -> {
            assert authentication != null : "service account authenticate should return either authentication or call onFailure";
            listener.onResponse(AuthenticationResult.success(authentication));
        }, e -> {
            logger.debug(new ParameterizedMessage("Failed to validate service account token for request [{}]", context.getRequest()), e);
            listener.onFailure(context.getRequest().exceptionProcessingRequest(e, serviceAccountToken));
        }));
    }
}
