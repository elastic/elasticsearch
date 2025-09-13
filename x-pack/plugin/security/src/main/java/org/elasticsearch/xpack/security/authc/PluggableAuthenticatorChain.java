/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authc;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.AuthenticationResult;
import org.elasticsearch.xpack.core.security.authc.AuthenticationToken;
import org.elasticsearch.xpack.core.security.authc.CustomAuthenticator;

import java.util.Collections;
import java.util.List;

public class PluggableAuthenticatorChain implements Authenticator {

    private final List<CustomAuthenticator> customAuthenticators;

    public PluggableAuthenticatorChain(List<CustomAuthenticator> customAuthenticators) {
        this.customAuthenticators = Collections.unmodifiableList(customAuthenticators);
    }

    @Override
    public String name() {
        return "pluggable custom authenticator chain";
    }

    public List<CustomAuthenticator> getCustomAuthenticators() {
        return customAuthenticators;
    }

    public boolean hasCustomAuthenticators() {
        return customAuthenticators.size() > 0;
    }

    @Override
    public AuthenticationToken extractCredentials(Context context) {
        if (false == hasCustomAuthenticators()) {
            return null;
        }
        for (CustomAuthenticator customAuthenticator : customAuthenticators) {
            AuthenticationToken token = customAuthenticator.extractToken(context.getThreadContext());
            if (token != null) {
                return token;
            }
        }
        return null;
    }

    @Override
    public void authenticate(Context context, ActionListener<AuthenticationResult<Authentication>> listener) {
        if (false == hasCustomAuthenticators()) {
            listener.onResponse(AuthenticationResult.notHandled());
            return;
        }
        AuthenticationToken token = context.getMostRecentAuthenticationToken();
        if (token != null) {
            // TODO switch to IteratingActionListener
            for (CustomAuthenticator customAuthenticator : customAuthenticators) {
                if (customAuthenticator.supports(token)) {
                    customAuthenticator.authenticate(token, ActionListener.wrap(response -> {
                        if (response.isAuthenticated()) {
                            listener.onResponse(response);
                        } else if (response.getStatus() == AuthenticationResult.Status.TERMINATE) {
                            final Exception ex = response.getException();
                            if (ex == null) {
                                listener.onFailure(context.getRequest().authenticationFailed(token));
                            } else {
                                listener.onFailure(context.getRequest().exceptionProcessingRequest(ex, token));
                            }
                        } else if (response.getStatus() == AuthenticationResult.Status.CONTINUE) {
                            listener.onResponse(AuthenticationResult.notHandled());
                        }
                    }, ex -> listener.onFailure(context.getRequest().exceptionProcessingRequest(ex, token))));
                    return;
                }
            }
        }
        listener.onResponse(AuthenticationResult.notHandled());
    }

}
