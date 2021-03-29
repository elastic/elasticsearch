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
import org.elasticsearch.xpack.core.security.action.service.GetServiceAccountTokensAction;
import org.elasticsearch.xpack.core.security.action.service.GetServiceAccountTokensRequest;
import org.elasticsearch.xpack.core.security.action.service.GetServiceAccountTokensResponse;
import org.elasticsearch.xpack.security.authc.service.ServiceAccount.ServiceAccountId;
import org.elasticsearch.xpack.security.authc.service.ServiceAccountService;
import org.elasticsearch.xpack.security.authc.support.HttpTlsRuntimeCheck;

public class TransportGetServiceAccountTokensAction
    extends HandledTransportAction<GetServiceAccountTokensRequest, GetServiceAccountTokensResponse> {

    private final ServiceAccountService serviceAccountService;
    private final HttpTlsRuntimeCheck httpTlsRuntimeCheck;
    private final String nodeName;

    @Inject
    public TransportGetServiceAccountTokensAction(TransportService transportService, ActionFilters actionFilters,
                                                  Settings settings,
                                                  ServiceAccountService serviceAccountService,
                                                  HttpTlsRuntimeCheck httpTlsRuntimeCheck) {
        super(GetServiceAccountTokensAction.NAME, transportService, actionFilters, GetServiceAccountTokensRequest::new);
        this.nodeName = Node.NODE_NAME_SETTING.get(settings);
        this.serviceAccountService = serviceAccountService;
        this.httpTlsRuntimeCheck = httpTlsRuntimeCheck;
    }

    @Override
    protected void doExecute(Task task, GetServiceAccountTokensRequest request, ActionListener<GetServiceAccountTokensResponse> listener) {
        httpTlsRuntimeCheck.checkTlsThenExecute(listener::onFailure, "get service account tokens", () -> {
            final ServiceAccountId accountId = new ServiceAccountId(request.getNamespace(), request.getServiceName());
            serviceAccountService.findTokensFor(accountId, nodeName, listener);
        });
    }
}
