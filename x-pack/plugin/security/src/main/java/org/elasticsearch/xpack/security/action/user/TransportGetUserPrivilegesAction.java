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
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.security.SecurityContext;
import org.elasticsearch.xpack.core.security.action.user.GetUserPrivilegesAction;
import org.elasticsearch.xpack.core.security.action.user.GetUserPrivilegesRequest;
import org.elasticsearch.xpack.core.security.action.user.GetUserPrivilegesResponse;
import org.elasticsearch.xpack.core.security.authc.AuthenticationContext;
import org.elasticsearch.xpack.core.security.authc.Subject;
import org.elasticsearch.xpack.core.security.authz.AuthorizationEngine;
import org.elasticsearch.xpack.security.authz.AuthorizationService;

/**
 * Transport action for {@link GetUserPrivilegesAction}
 */
public class TransportGetUserPrivilegesAction extends HandledTransportAction<GetUserPrivilegesRequest, GetUserPrivilegesResponse> {

    private final ThreadPool threadPool;
    private final AuthorizationService authorizationService;
    private final SecurityContext securityContext;

    @Inject
    public TransportGetUserPrivilegesAction(
        ThreadPool threadPool,
        TransportService transportService,
        ActionFilters actionFilters,
        AuthorizationService authorizationService,
        SecurityContext securityContext
    ) {
        super(GetUserPrivilegesAction.NAME, transportService, actionFilters, GetUserPrivilegesRequest::new);
        this.threadPool = threadPool;
        this.authorizationService = authorizationService;
        this.securityContext = securityContext;
    }

    @Override
    protected void doExecute(Task task, GetUserPrivilegesRequest request, ActionListener<GetUserPrivilegesResponse> listener) {
        final String username = request.username();

        securityContext.requireUser();
        final Subject subject = AuthenticationContext.fromAuthentication(securityContext.getAuthentication()).getEffectiveSubject();
        final AuthorizationEngine.AuthorizationInfo authorizationInfo = securityContext.getAuthorizationInfoFromContext();
        if (subject.getUser().principal().equals(username) == false) {
            listener.onFailure(new IllegalArgumentException("users may only list the privileges of their own account"));
            return;
        }

        authorizationService.retrieveUserPrivileges(subject, authorizationInfo, request, listener);
    }
}
