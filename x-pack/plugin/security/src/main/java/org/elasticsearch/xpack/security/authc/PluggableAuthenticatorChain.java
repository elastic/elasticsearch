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
import org.elasticsearch.xpack.core.common.IteratingActionListener;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.AuthenticationResult;
import org.elasticsearch.xpack.core.security.authc.AuthenticationToken;
import org.elasticsearch.xpack.core.security.authc.CustomAuthenticator;

import java.util.Collections;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.Function;

import static org.elasticsearch.common.Strings.format;

public class PluggableAuthenticatorChain implements Authenticator {

    private static final Logger logger = LogManager.getLogger(PluggableAuthenticatorChain.class);

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
            var iteratingListener = new IteratingActionListener<>(
                listener,
                getAuthConsumer(context),
                customAuthenticators,
                context.getThreadContext(),
                Function.identity(),
                result -> result.getStatus() == AuthenticationResult.Status.CONTINUE
            );
            try {
                iteratingListener.run();
            } catch (Exception e) {
                logger.debug(() -> format("Authentication of token [%s] failed", token.getClass().getName()), e);
                listener.onFailure(context.getRequest().exceptionProcessingRequest(e, token));
            }
            return;
        }
        listener.onResponse(AuthenticationResult.notHandled());
    }

    private BiConsumer<CustomAuthenticator, ActionListener<AuthenticationResult<Authentication>>> getAuthConsumer(Context context) {
        AuthenticationToken token = context.getMostRecentAuthenticationToken();
        return (authenticator, iteratingListener) -> {
            if (authenticator.supports(token)) {
                authenticator.authenticate(token, ActionListener.wrap(response -> {
                    if (response.isAuthenticated()) {
                        iteratingListener.onResponse(response);
                    } else if (response.getStatus() == AuthenticationResult.Status.TERMINATE) {
                        final Exception ex = response.getException();
                        logger.debug(
                            () -> format(
                                "Authentication of token [%s] was terminated: %s (caused by: %s)",
                                token.principal(),
                                response.getMessage(),
                                ex
                            )
                        );
                        if (ex == null) {
                            iteratingListener.onFailure(context.getRequest().authenticationFailed(token));
                        } else {
                            iteratingListener.onFailure(context.getRequest().exceptionProcessingRequest(ex, token));
                        }
                    } else if (response.getStatus() == AuthenticationResult.Status.CONTINUE) {
                        iteratingListener.onResponse(AuthenticationResult.notHandled());
                    }
                }, ex -> iteratingListener.onFailure(context.getRequest().exceptionProcessingRequest(ex, token))));
            } else {
                iteratingListener.onResponse(AuthenticationResult.notHandled()); // try the next custom authenticator
            }
        };
    }

}
