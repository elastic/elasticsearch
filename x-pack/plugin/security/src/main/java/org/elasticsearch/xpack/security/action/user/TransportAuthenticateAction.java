/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.action.user;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.exception.ElasticsearchSecurityException;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.security.SecurityContext;
import org.elasticsearch.xpack.core.security.action.user.AuthenticateAction;
import org.elasticsearch.xpack.core.security.action.user.AuthenticateRequest;
import org.elasticsearch.xpack.core.security.action.user.AuthenticateResponse;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.operator.OperatorPrivilegesUtil;
import org.elasticsearch.xpack.core.security.user.AnonymousUser;
import org.elasticsearch.xpack.core.security.user.InternalUser;
import org.elasticsearch.xpack.core.security.user.User;

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
        super(AuthenticateAction.NAME, transportService, actionFilters, AuthenticateRequest::new, EsExecutors.DIRECT_EXECUTOR_SERVICE);
        this.securityContext = securityContext;
        this.anonymousUser = anonymousUser;
    }

    @Override
    protected void doExecute(Task task, AuthenticateRequest request, ActionListener<AuthenticateResponse> listener) {
        final Authentication authentication = securityContext.getAuthentication();
        final User runAsUser, authUser;
        if (authentication == null) {
            runAsUser = authUser = null;
        } else {
            runAsUser = authentication.getEffectiveSubject().getUser();
            authUser = authentication.getAuthenticatingSubject().getUser();
        }
        if (authUser == null) {
            listener.onFailure(new ElasticsearchSecurityException("did not find an authenticated user"));
        } else if (authUser instanceof InternalUser) {
            listener.onFailure(new IllegalArgumentException("user [" + authUser.principal() + "] is internal"));
        } else if (runAsUser instanceof InternalUser) {
            listener.onFailure(new IllegalArgumentException("user [" + runAsUser.principal() + "] is internal"));
        } else {
            listener.onResponse(
                new AuthenticateResponse(
                    authentication.maybeAddAnonymousRoles(anonymousUser),
                    OperatorPrivilegesUtil.isOperator(securityContext.getThreadContext())
                )
            );
        }
    }
}
