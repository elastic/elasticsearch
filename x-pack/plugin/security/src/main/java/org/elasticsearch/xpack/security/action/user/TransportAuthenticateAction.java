/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.action.user;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.security.SecurityContext;
import org.elasticsearch.xpack.core.security.action.user.AuthenticateAction;
import org.elasticsearch.xpack.core.security.action.user.AuthenticateRequest;
import org.elasticsearch.xpack.core.security.action.user.AuthenticateResponse;
import org.elasticsearch.xpack.core.security.user.SystemUser;
import org.elasticsearch.xpack.core.security.user.User;
import org.elasticsearch.xpack.core.security.user.XPackUser;

import java.util.function.Supplier;

public class TransportAuthenticateAction extends HandledTransportAction<AuthenticateRequest, AuthenticateResponse> {

    private final SecurityContext securityContext;

    @Inject
    public TransportAuthenticateAction(Settings settings, TransportService transportService,
                                       ActionFilters actionFilters, SecurityContext securityContext) {
        super(settings, AuthenticateAction.NAME, transportService, actionFilters,
            (Supplier<AuthenticateRequest>) AuthenticateRequest::new);
        this.securityContext = securityContext;
    }

    @Override
    protected void doExecute(AuthenticateRequest request, ActionListener<AuthenticateResponse> listener) {
        final User runAsUser = securityContext.getUser();
        final User authUser = runAsUser == null ? null : runAsUser.authenticatedUser();
        if (authUser == null) {
            listener.onFailure(new ElasticsearchSecurityException("did not find an authenticated user"));
        } else if (SystemUser.is(authUser) || XPackUser.is(authUser)) {
            listener.onFailure(new IllegalArgumentException("user [" + authUser.principal() + "] is internal"));
        } else if (SystemUser.is(runAsUser) || XPackUser.is(runAsUser)) {
            listener.onFailure(new IllegalArgumentException("user [" + runAsUser.principal() + "] is internal"));
        } else {
            listener.onResponse(new AuthenticateResponse(runAsUser));
        }
    }
}
