/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.action.service;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.Node;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.security.action.service.GetServiceAccountTokensAction;
import org.elasticsearch.xpack.core.security.action.service.GetServiceAccountTokensRequest;
import org.elasticsearch.xpack.core.security.action.service.GetServiceAccountTokensResponse;
import org.elasticsearch.xpack.security.authc.service.ServiceAccount.ServiceAccountId;
import org.elasticsearch.xpack.security.authc.service.ServiceAccountService;

public class TransportGetServiceAccountTokensAction
    extends HandledTransportAction<GetServiceAccountTokensRequest, GetServiceAccountTokensResponse> {

    private static final Logger logger = LogManager.getLogger(TransportGetServiceAccountTokensAction.class);

    private final ServiceAccountService serviceAccountService;
    private final String nodeName;
    private final boolean httpTlsEnabled;
    private final boolean transportTlsEnabled;

    @Inject
    public TransportGetServiceAccountTokensAction(
        TransportService transportService, ActionFilters actionFilters, Settings settings, ServiceAccountService serviceAccountService) {
        super(GetServiceAccountTokensAction.NAME, transportService, actionFilters, GetServiceAccountTokensRequest::new);
        this.nodeName = Node.NODE_NAME_SETTING.get(settings);
        this.serviceAccountService = serviceAccountService;
        this.httpTlsEnabled = XPackSettings.HTTP_SSL_ENABLED.get(settings);
        this.transportTlsEnabled = XPackSettings.TRANSPORT_SSL_ENABLED.get(settings);
    }

    @Override
    protected void doExecute(Task task, GetServiceAccountTokensRequest request, ActionListener<GetServiceAccountTokensResponse> listener) {
        if (false == httpTlsEnabled || false == transportTlsEnabled) {
            final ParameterizedMessage message = new ParameterizedMessage(
                "Service account APIs require TLS for both HTTP and Transport, " +
                    "but got HTTP TLS: [{}] and Transport TLS: [{}]", httpTlsEnabled, transportTlsEnabled);
            logger.debug(message);
            listener.onFailure(new ElasticsearchSecurityException(message.getFormattedMessage(), RestStatus.UNAUTHORIZED));
            return;
        }
        final ServiceAccountId accountId = new ServiceAccountId(request.getNamespace(), request.getServiceName());
        serviceAccountService.findTokensFor(accountId, nodeName, listener);
    }
}
