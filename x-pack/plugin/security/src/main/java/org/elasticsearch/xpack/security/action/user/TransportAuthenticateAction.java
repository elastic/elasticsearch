/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.action.user;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.security.SecurityContext;
import org.elasticsearch.xpack.core.security.action.user.AuthenticateAction;
import org.elasticsearch.xpack.core.security.action.user.AuthenticateRequest;
import org.elasticsearch.xpack.core.security.action.user.AuthenticateResponse;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.user.AnonymousUser;
import org.elasticsearch.xpack.core.security.user.User;

import java.util.stream.Stream;

public class TransportAuthenticateAction extends HandledTransportAction<AuthenticateRequest, AuthenticateResponse> {

    private final SecurityContext securityContext;
    private final AnonymousUser anonymousUser;

    @Inject
    public TransportAuthenticateAction(
        TransportService transportService,
        ActionFilters actionFilters,
        SecurityContext securityContext,
        AnonymousUser anonymousUser
    ) {
        super(AuthenticateAction.NAME, transportService, actionFilters, AuthenticateRequest::new);
        this.securityContext = securityContext;
        this.anonymousUser = anonymousUser;
    }

    @Override
    protected void doExecute(Task task, AuthenticateRequest request, ActionListener<AuthenticateResponse> listener) {
        final Authentication authentication = securityContext.getAuthentication();
        final User runAsUser = authentication == null ? null : authentication.getUser();
        final User authUser = runAsUser == null ? null : runAsUser.authenticatedUser();
        if (authUser == null) {
            listener.onFailure(new ElasticsearchSecurityException("did not find an authenticated user"));
        } else if (User.isInternal(authUser)) {
            listener.onFailure(new IllegalArgumentException("user [" + authUser.principal() + "] is internal"));
        } else if (User.isInternal(runAsUser)) {
            listener.onFailure(new IllegalArgumentException("user [" + runAsUser.principal() + "] is internal"));
        } else {
            final User user = authentication.getUser();
            final boolean shouldAddAnonymousRoleNames = anonymousUser.enabled()
                && false == anonymousUser.equals(user)
                && false == authentication.isApiKey()
                && false == authentication.isServiceAccount();
            if (shouldAddAnonymousRoleNames) {
                final String[] allRoleNames = Stream.concat(Stream.of(user.roles()), Stream.of(anonymousUser.roles()))
                    .toArray(String[]::new);
                listener.onResponse(
                    new AuthenticateResponse(
                        // TODO do not rebuild the authentication for display purposes, instead make the authentication service construct
                        // the user so it includes the anonymous roles as well
                        new Authentication(
                            new User(
                                new User(user.principal(), allRoleNames, user.fullName(), user.email(), user.metadata(), user.enabled()),
                                user.authenticatedUser()
                            ),
                            authentication.getAuthenticatedBy(),
                            authentication.getLookedUpBy(),
                            authentication.getVersion(),
                            authentication.getAuthenticationType(),
                            authentication.getMetadata()
                        )
                    )
                );
            } else {
                listener.onResponse(new AuthenticateResponse(authentication));
            }
        }
    }
}
