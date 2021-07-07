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
import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.security.action.service.GetServiceAccountCredentialsAction;
import org.elasticsearch.xpack.core.security.action.service.GetServiceAccountCredentialsRequest;
import org.elasticsearch.xpack.core.security.action.service.GetServiceAccountCredentialsResponse;
import org.elasticsearch.xpack.core.security.action.service.GetServiceAccountFileTokensAction;
import org.elasticsearch.xpack.core.security.action.service.GetServiceAccountFileTokensRequest;
import org.elasticsearch.xpack.core.security.action.service.TokenInfo;
import org.elasticsearch.xpack.security.authc.service.IndexServiceAccountTokenStore;
import org.elasticsearch.xpack.security.authc.service.ServiceAccount.ServiceAccountId;
import org.elasticsearch.xpack.security.authc.support.HttpTlsRuntimeCheck;

import java.util.Collection;

import static org.elasticsearch.xpack.core.ClientHelper.SECURITY_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.executeAsyncWithOrigin;

public class TransportGetServiceAccountCredentialsAction
    extends HandledTransportAction<GetServiceAccountCredentialsRequest, GetServiceAccountCredentialsResponse> {

    private final IndexServiceAccountTokenStore indexServiceAccountTokenStore;
    private final HttpTlsRuntimeCheck httpTlsRuntimeCheck;
    private final Client client;

    @Inject
    public TransportGetServiceAccountCredentialsAction(TransportService transportService, ActionFilters actionFilters,
                                                       IndexServiceAccountTokenStore indexServiceAccountTokenStore,
                                                       HttpTlsRuntimeCheck httpTlsRuntimeCheck,
                                                       Client client) {
        super(GetServiceAccountCredentialsAction.NAME, transportService, actionFilters, GetServiceAccountCredentialsRequest::new);
        this.indexServiceAccountTokenStore = indexServiceAccountTokenStore;
        this.httpTlsRuntimeCheck = httpTlsRuntimeCheck;
        this.client = client;
    }

    @Override
    protected void doExecute(Task task, GetServiceAccountCredentialsRequest request,
                             ActionListener<GetServiceAccountCredentialsResponse> listener) {
        httpTlsRuntimeCheck.checkTlsThenExecute(listener::onFailure, "get service account tokens", () -> {

            getIndexTokens(request, listener);
        });
    }

    private void getIndexTokens(GetServiceAccountCredentialsRequest request,
                                ActionListener<GetServiceAccountCredentialsResponse> listener) {
        final ServiceAccountId accountId = new ServiceAccountId(request.getNamespace(), request.getServiceName());
        indexServiceAccountTokenStore.findTokensFor(accountId, ActionListener.wrap(indexTokenInfos -> {
            getFileTokens(request, accountId, indexTokenInfos, listener);
        }, listener::onFailure));
    }

    private void getFileTokens(GetServiceAccountCredentialsRequest request, ServiceAccountId accountId,
        Collection<TokenInfo> indexTokenInfos, ActionListener<GetServiceAccountCredentialsResponse> listener) {
        executeAsyncWithOrigin(client, SECURITY_ORIGIN,
            GetServiceAccountFileTokensAction.INSTANCE,
            new GetServiceAccountFileTokensRequest(request.getNamespace(), request.getServiceName()),
            ActionListener.wrap(fileTokensResponse -> listener.onResponse(
                new GetServiceAccountCredentialsResponse(accountId.asPrincipal(), indexTokenInfos, fileTokensResponse)),
                listener::onFailure));
    }
}
