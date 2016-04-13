/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.action.user;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.shield.SecurityContext;
import org.elasticsearch.shield.user.SystemUser;
import org.elasticsearch.shield.user.User;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

/**
 */
public class TransportAuthenticateAction extends HandledTransportAction<AuthenticateRequest, AuthenticateResponse> {

    private final SecurityContext securityContext;

    @Inject
    public TransportAuthenticateAction(Settings settings, ThreadPool threadPool, TransportService transportService,
                                       ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver,
                                       SecurityContext securityContext) {
        super(settings, AuthenticateAction.NAME, threadPool, transportService, actionFilters, indexNameExpressionResolver,
                AuthenticateRequest::new);
        this.securityContext = securityContext;
    }

    @Override
    protected void doExecute(AuthenticateRequest request, ActionListener<AuthenticateResponse> listener) {
        final User user = securityContext.getUser();
        if (SystemUser.is(user)) {
            listener.onFailure(new IllegalArgumentException("user [" + user.principal() + "] is internal"));
            return;
        }

        if (user == null) {
            listener.onFailure(new ElasticsearchSecurityException("did not find an authenticated user"));
            return;
        }
        listener.onResponse(new AuthenticateResponse(user));
    }
}
