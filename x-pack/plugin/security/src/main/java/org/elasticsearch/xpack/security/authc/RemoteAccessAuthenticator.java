/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authc;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.AuthenticationField;
import org.elasticsearch.xpack.core.security.authc.AuthenticationResult;
import org.elasticsearch.xpack.core.security.authc.AuthenticationToken;
import org.elasticsearch.xpack.core.security.authc.RemoteAccessAuthentication;
import org.elasticsearch.xpack.core.security.support.Exceptions;
import org.elasticsearch.xpack.core.security.user.User;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class RemoteAccessAuthenticator implements Authenticator {

    private final ApiKeyService apiKeyService;
    private final String nodeName;

    private static final Logger logger = LogManager.getLogger(RemoteAccessAuthenticator.class);

    public RemoteAccessAuthenticator(ApiKeyService apiKeyService, String nodeName) {
        this.apiKeyService = apiKeyService;
        this.nodeName = nodeName;
    }

    @Override
    public String name() {
        return "Remote access";
    }

    @Override
    public AuthenticationToken extractCredentials(Context context) {
        final ThreadContext threadContext = context.getThreadContext();
        final ApiKeyService.ApiKeyCredentials apiKeyCredentials = apiKeyService.getCredentialsFromRemoteAccessHeader(threadContext);
        if (apiKeyCredentials == null) {
            return null;
        }

        // TODO once we've read the headers, we should clear them from the context
        assert threadContext.getHeader(RemoteAccessAuthentication.REMOTE_ACCESS_AUTHENTICATION_HEADER_KEY) != null;
        try {
            logger.info("Extracting remote access credentials for [{}]", apiKeyCredentials.principal());
            final RemoteAccessAuthentication remoteAccessAuthentication = RemoteAccessAuthentication.readFromContext(threadContext);
            return new RemoteAccessCredentials(apiKeyCredentials, remoteAccessAuthentication);
        } catch (IOException ex) {
            logger.error("Failed extracting remote access authentication header", ex);
            return null;
        }
    }

    @Override
    public void authenticate(Context context, ActionListener<AuthenticationResult<Authentication>> listener) {
        final AuthenticationToken authenticationToken = context.getMostRecentAuthenticationToken();
        if (false == authenticationToken instanceof RemoteAccessCredentials) {
            listener.onResponse(AuthenticationResult.notHandled());
            return;
        }
        final RemoteAccessCredentials remoteAccessCredentials = (RemoteAccessCredentials) authenticationToken;
        logger.info("Authenticating remote access for [{}]", remoteAccessCredentials.principal());
        apiKeyService.tryAuthenticate(
            context.getThreadContext(),
            remoteAccessCredentials.apiKeyCredentials(),
            ActionListener.wrap(authResult -> {
                if (authResult.isAuthenticated()) {
                    final Authentication receivedAuthentication = remoteAccessCredentials.remoteAccessAuthentication().getAuthentication();
                    if (User.isInternal(receivedAuthentication.getEffectiveSubject().getUser())) {
                        listener.onResponse(
                            AuthenticationResult.success(
                                Authentication.newInternalAuthentication(
                                    receivedAuthentication.getEffectiveSubject().getUser(),
                                    Version.CURRENT,
                                    nodeName
                                )
                            )
                        );
                    } else {
                        final Map<String, Object> authMetadata = new HashMap<>(authResult.getMetadata());
                        authMetadata.put(
                            AuthenticationField.REMOTE_ACCESS_ROLE_DESCRIPTORS_KEY,
                            remoteAccessCredentials.remoteAccessAuthentication().getRoleDescriptorsBytesList()
                        );
                        listener.onResponse(
                            AuthenticationResult.success(
                                Authentication.newRemoteAccessAuthentication(
                                    AuthenticationResult.success(authResult.getValue(), Map.copyOf(authMetadata)),
                                    nodeName
                                )
                            )
                        );
                    }
                } else if (authResult.getStatus() == AuthenticationResult.Status.TERMINATE) {
                    listener.onFailure(
                        (authResult.getException() != null)
                            ? authResult.getException()
                            : Exceptions.authenticationError(authResult.getMessage())
                    );
                } else {
                    listener.onResponse(AuthenticationResult.unsuccessful(authResult.getMessage(), authResult.getException()));
                }
            }, e -> listener.onFailure(context.getRequest().exceptionProcessingRequest(e, null)))
        );
    }

    record RemoteAccessCredentials(
        ApiKeyService.ApiKeyCredentials apiKeyCredentials,
        // TODO maybe doesn't belong here
        RemoteAccessAuthentication remoteAccessAuthentication
    ) implements AuthenticationToken {

        @Override
        public String principal() {
            return apiKeyCredentials.principal();
        }

        @Override
        public Object credentials() {
            return apiKeyCredentials.credentials();
        }

        @Override
        public void clearCredentials() {
            apiKeyCredentials.clearCredentials();
        }
    }
}
