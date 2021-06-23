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
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.Node;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.security.action.service.GetServiceAccountCredentialsAction;
import org.elasticsearch.xpack.core.security.action.service.GetServiceAccountCredentialsRequest;
import org.elasticsearch.xpack.core.security.action.service.GetServiceAccountCredentialsResponse;
import org.elasticsearch.xpack.security.authc.service.ServiceAccount.ServiceAccountId;
import org.elasticsearch.xpack.security.authc.service.ServiceAccountService;
import org.elasticsearch.xpack.security.authc.support.HttpTlsRuntimeCheck;

public class TransportGetServiceAccountCredentialsAction
    extends HandledTransportAction<GetServiceAccountCredentialsRequest, GetServiceAccountCredentialsResponse> {

    private final ServiceAccountService serviceAccountService;
    private final HttpTlsRuntimeCheck httpTlsRuntimeCheck;
    private final String nodeName;

    @Inject
    public TransportGetServiceAccountCredentialsAction(TransportService transportService, ActionFilters actionFilters,
                                                       Settings settings,
                                                       ServiceAccountService serviceAccountService,
                                                       HttpTlsRuntimeCheck httpTlsRuntimeCheck) {
        super(GetServiceAccountCredentialsAction.NAME, transportService, actionFilters, GetServiceAccountCredentialsRequest::new);
        this.nodeName = Node.NODE_NAME_SETTING.get(settings);
        this.serviceAccountService = serviceAccountService;
        this.httpTlsRuntimeCheck = httpTlsRuntimeCheck;
    }

    @Override
    protected void doExecute(Task task, GetServiceAccountCredentialsRequest request,
                             ActionListener<GetServiceAccountCredentialsResponse> listener) {
        httpTlsRuntimeCheck.checkTlsThenExecute(listener::onFailure, "get service account tokens", () -> {
            final ServiceAccountId accountId = new ServiceAccountId(request.getNamespace(), request.getServiceName());
            serviceAccountService.findTokensFor(accountId, nodeName, listener);
        });
    }
}
