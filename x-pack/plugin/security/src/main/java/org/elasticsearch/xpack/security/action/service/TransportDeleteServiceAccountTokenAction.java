/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.action.service;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.security.action.service.DeleteServiceAccountTokenAction;
import org.elasticsearch.xpack.core.security.action.service.DeleteServiceAccountTokenRequest;
import org.elasticsearch.xpack.core.security.action.service.DeleteServiceAccountTokenResponse;
import org.elasticsearch.xpack.security.authc.service.IndexServiceAccountsTokenStore;
import org.elasticsearch.xpack.security.authc.support.HttpTlsRuntimeCheck;

public class TransportDeleteServiceAccountTokenAction
    extends HandledTransportAction<DeleteServiceAccountTokenRequest, DeleteServiceAccountTokenResponse> {

    private final IndexServiceAccountsTokenStore indexServiceAccountsTokenStore;
    private final HttpTlsRuntimeCheck httpTlsRuntimeCheck;

    @Inject
    public TransportDeleteServiceAccountTokenAction(TransportService transportService, ActionFilters actionFilters,
                                                    IndexServiceAccountsTokenStore indexServiceAccountsTokenStore,
                                                    HttpTlsRuntimeCheck httpTlsRuntimeCheck) {
        super(DeleteServiceAccountTokenAction.NAME, transportService, actionFilters, DeleteServiceAccountTokenRequest::new);
        this.indexServiceAccountsTokenStore = indexServiceAccountsTokenStore;
        this.httpTlsRuntimeCheck = httpTlsRuntimeCheck;
    }

    @Override
    protected void doExecute(Task task, DeleteServiceAccountTokenRequest request,
                             ActionListener<DeleteServiceAccountTokenResponse> listener) {
        httpTlsRuntimeCheck.checkTlsThenExecute(listener::onFailure, "create service account token", () -> {
            indexServiceAccountsTokenStore.deleteToken(request, ActionListener.wrap(found -> {
                listener.onResponse(new DeleteServiceAccountTokenResponse(found));
            }, listener::onFailure));
        });
    }
}
