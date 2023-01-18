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
import org.elasticsearch.action.admin.cluster.state.ClusterStateAction;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.AuthenticationField;
import org.elasticsearch.xpack.core.security.authc.AuthenticationResult;
import org.elasticsearch.xpack.core.security.authc.RemoteAccessAuthentication;
import org.elasticsearch.xpack.core.security.authc.support.AuthenticationContextSerializer;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptorsIntersection;
import org.elasticsearch.xpack.core.security.support.Exceptions;
import org.elasticsearch.xpack.core.security.user.SystemUser;
import org.elasticsearch.xpack.core.security.user.User;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.core.Strings.format;

public class RemoteAccessAuthenticator {

    private static final RoleDescriptor CROSS_CLUSTER_SEARCH_ROLE = new RoleDescriptor(
        "_cross_cluster_search",
        new String[] { ClusterStateAction.NAME },
        null,
        null,
        null,
        null,
        null,
        null,
        null
    );
    private static final Logger logger = LogManager.getLogger(RemoteAccessAuthenticator.class);

    private final ApiKeyService apiKeyService;
    private final String nodeName;
    private final AuthenticationContextSerializer authenticationSerializer;

    public RemoteAccessAuthenticator(ApiKeyService apiKeyService, String nodeName) {
        this.apiKeyService = apiKeyService;
        this.nodeName = nodeName;
        this.authenticationSerializer = new AuthenticationContextSerializer();
    }

    void authenticate(Authenticator.Context context, ActionListener<Authentication> listener) {
        try {
            final ThreadContext threadContext = context.getThreadContext();

            final ApiKeyService.ApiKeyCredentials apiKeyCredentials = apiKeyService.getCredentialsFromRemoteAccessHeader(threadContext);
            if (apiKeyCredentials == null) {
                // TODO more informative failure message here
                listener.onFailure(context.getRequest().authenticationFailed(null));
                return;
            }

            final RemoteAccessAuthentication remoteAccessAuthentication = RemoteAccessAuthentication.readFromContext(threadContext);

            // Once we've read the remote access headers, we should remove them. We want to maintain the invariant that we either have
            // remote access headers or an authentication instance in the thread context but not both. Below, if authc succeeds, we will
            // write authentication to the context; therefore, we should pre-emptively remove the remote access headers here
            context.getThreadContext().removeRemoteAccessHeaders();

            doAuthenticate(context, apiKeyCredentials, remoteAccessAuthentication, listener);
        } catch (IOException e) {
            listener.onFailure(context.getRequest().exceptionProcessingRequest(e, null));
        }
    }

    private void doAuthenticate(
        final Authenticator.Context context,
        final ApiKeyService.ApiKeyCredentials remoteAccessKeyCredentials,
        final RemoteAccessAuthentication remoteAccessAuthentication,
        final ActionListener<Authentication> listener
    ) {
        apiKeyService.tryAuthenticate(context.getThreadContext(), remoteAccessKeyCredentials, ActionListener.wrap(authResult -> {
            if (authResult.isAuthenticated()) {
                final Authentication receivedAuthentication = remoteAccessAuthentication.getAuthentication();
                final List<RemoteAccessAuthentication.RoleDescriptorsBytes> roleDescriptorsBytesList;
                final User user = receivedAuthentication.getEffectiveSubject().getUser();
                if (User.isInternal(user)) {
                    if (SystemUser.is(user)) {
                        roleDescriptorsBytesList = RemoteAccessAuthentication.toRoleDescriptorsBytesList(
                            new RoleDescriptorsIntersection(List.of(Set.of(CROSS_CLUSTER_SEARCH_ROLE)))
                        );
                    } else {
                        throw new IllegalArgumentException(
                            "received cross cluster request from an unexpected internal user [" + user.principal() + "]"
                        );
                    }
                } else {
                    roleDescriptorsBytesList = remoteAccessAuthentication.getRoleDescriptorsBytesList();
                }
                final Map<String, Object> authMetadata = new HashMap<>(authResult.getMetadata());
                authMetadata.put(AuthenticationField.REMOTE_ACCESS_ROLE_DESCRIPTORS_KEY, roleDescriptorsBytesList);
                final Authentication successfulAuthentication = Authentication.newRemoteAccessAuthentication(
                    receivedAuthentication,
                    AuthenticationResult.success(authResult.getValue(), Map.copyOf(authMetadata)),
                    nodeName
                );
                writeAuthToContext(context, successfulAuthentication, listener);
            } else {
                listener.onFailure(
                    (authResult.getException() != null)
                        ? authResult.getException()
                        : Exceptions.authenticationError(authResult.getMessage())
                );
            }
        }, e -> listener.onFailure(context.getRequest().exceptionProcessingRequest(e, null))));
    }

    void writeAuthToContext(Authenticator.Context context, Authentication authentication, ActionListener<Authentication> listener) {
        try {
            authenticationSerializer.writeToContext(authentication, context.getThreadContext());
            context.getRequest().authenticationSuccess(authentication);
        } catch (Exception e) {
            logger.debug(() -> format("Failed to store authentication [%s] for request [%s]", authentication, context.getRequest()), e);
            listener.onFailure(context.getRequest().exceptionProcessingRequest(e, null));
            return;
        }
        logger.trace("Established authentication [{}] for request [{}]", authentication, context.getRequest());
        listener.onResponse(authentication);
    }
}
