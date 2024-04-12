/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.action.apikey;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.security.SecurityContext;
import org.elasticsearch.xpack.core.security.action.apikey.InvalidateApiKeyAction;
import org.elasticsearch.xpack.core.security.action.apikey.InvalidateApiKeyRequest;
import org.elasticsearch.xpack.core.security.action.apikey.InvalidateApiKeyResponse;
import org.elasticsearch.xpack.core.security.action.user.HasPrivilegesAction;
import org.elasticsearch.xpack.core.security.action.user.HasPrivilegesRequest;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.security.authc.ApiKeyService;

public final class TransportInvalidateApiKeyAction extends HandledTransportAction<InvalidateApiKeyRequest, InvalidateApiKeyResponse> {

    private final ApiKeyService apiKeyService;
    private final SecurityContext securityContext;
    private final Client client;

    @Inject
    public TransportInvalidateApiKeyAction(
        TransportService transportService,
        ActionFilters actionFilters,
        ApiKeyService apiKeyService,
        SecurityContext context,
        Client client
    ) {
        super(
            InvalidateApiKeyAction.NAME,
            transportService,
            actionFilters,
            InvalidateApiKeyRequest::new,
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );
        this.apiKeyService = apiKeyService;
        this.securityContext = context;
        this.client = client;
    }

    @Override
    protected void doExecute(Task task, InvalidateApiKeyRequest request, ActionListener<InvalidateApiKeyResponse> listener) {
        String[] apiKeyIds = request.getIds();
        String apiKeyName = request.getName();
        String username = request.getUserName();
        String[] realms = Strings.hasText(request.getRealmName()) ? new String[] { request.getRealmName() } : null;

        final Authentication authentication = securityContext.getAuthentication();
        if (authentication == null) {
            listener.onFailure(new IllegalStateException("authentication is required"));
        }
        if (request.ownedByAuthenticatedUser()) {
            assert username == null;
            assert realms == null;
            // restrict username and realm to current authenticated user.
            username = authentication.getEffectiveSubject().getUser().principal();
            realms = ApiKeyService.getOwnersRealmNames(authentication);
        }

        final var finalUsername = username;
        final var finalRealms = realms;
        final var hasPrivilegesRequest = new HasPrivilegesRequest();
        hasPrivilegesRequest.username(securityContext.getUser().principal());
        hasPrivilegesRequest.clusterPrivileges("manage_security");
        hasPrivilegesRequest.indexPrivileges(new RoleDescriptor.IndicesPrivileges[0]);
        hasPrivilegesRequest.applicationPrivileges(new RoleDescriptor.ApplicationResourcePrivileges[0]);
        // TODO
        client.execute(HasPrivilegesAction.INSTANCE, hasPrivilegesRequest, ActionListener.wrap(res -> {
            final boolean completeMatch = res.isCompleteMatch();
            apiKeyService.invalidateApiKeys(finalRealms, finalUsername, apiKeyName, apiKeyIds, false == completeMatch, listener);
        }, listener::onFailure));

    }

}
