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
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.security.SecurityContext;
import org.elasticsearch.xpack.core.security.action.service.CreateServiceAccountTokenAction;
import org.elasticsearch.xpack.core.security.action.service.CreateServiceAccountTokenRequest;
import org.elasticsearch.xpack.core.security.action.service.CreateServiceAccountTokenResponse;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.security.authc.service.IndexServiceAccountsTokenStore;

public class TransportCreateServiceAccountTokenAction
    extends HandledTransportAction<CreateServiceAccountTokenRequest, CreateServiceAccountTokenResponse> {

    private static final Logger logger = LogManager.getLogger(TransportCreateServiceAccountTokenAction.class);

    private final IndexServiceAccountsTokenStore indexServiceAccountsTokenStore;
    private final SecurityContext securityContext;
    private final boolean httpTlsEnabled;
    private final boolean transportTlsEnabled;

    @Inject
    public TransportCreateServiceAccountTokenAction(TransportService transportService, ActionFilters actionFilters,
                                                    Settings settings,
                                                    IndexServiceAccountsTokenStore indexServiceAccountsTokenStore,
                                                    SecurityContext securityContext) {
        super(CreateServiceAccountTokenAction.NAME, transportService, actionFilters, CreateServiceAccountTokenRequest::new);
        this.indexServiceAccountsTokenStore = indexServiceAccountsTokenStore;
        this.securityContext = securityContext;
        this.httpTlsEnabled = XPackSettings.HTTP_SSL_ENABLED.get(settings);
        this.transportTlsEnabled = XPackSettings.TRANSPORT_SSL_ENABLED.get(settings);
    }

    @Override
    protected void doExecute(Task task, CreateServiceAccountTokenRequest request,
                             ActionListener<CreateServiceAccountTokenResponse> listener) {
        if (false == httpTlsEnabled || false == transportTlsEnabled) {
            final ParameterizedMessage message = new ParameterizedMessage(
                "Service account APIs require TLS for both HTTP and Transport, " +
                    "but got HTTP TLS: [{}] and Transport TLS: [{}]", httpTlsEnabled, transportTlsEnabled);
            logger.debug(message);
            listener.onFailure(new ElasticsearchSecurityException(message.getFormattedMessage(), RestStatus.UNAUTHORIZED));
            return;
        }
        final Authentication authentication = securityContext.getAuthentication();
        if (authentication == null) {
            listener.onFailure(new IllegalStateException("authentication is required"));
        } else {
            indexServiceAccountsTokenStore.createToken(authentication, request, listener);
        }
    }
}
