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
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.security.SecurityContext;
import org.elasticsearch.xpack.core.security.action.apikey.InvalidateApiKeyAction;
import org.elasticsearch.xpack.core.security.action.apikey.InvalidateApiKeyRequest;
import org.elasticsearch.xpack.core.security.action.apikey.InvalidateApiKeyResponse;
import org.elasticsearch.xpack.core.security.action.user.HasPrivilegesAction;
import org.elasticsearch.xpack.core.security.action.user.HasPrivilegesRequest;
import org.elasticsearch.xpack.core.security.action.user.HasPrivilegesResponse;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.security.authz.privilege.ClusterPrivilegeResolver;
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
        final Authentication authentication = securityContext.getAuthentication();
        if (authentication == null) {
            listener.onFailure(new IllegalStateException("authentication is required"));
            return;
        }
        final String[] apiKeyIds = request.getIds();
        final String apiKeyName = request.getName();
        final String username = getUsername(authentication, request);
        final String[] realms = getRealms(authentication, request);
        checkHasManageSecurityPrivilege(
            ActionListener.wrap(
                hasPrivilegesResponse -> apiKeyService.invalidateApiKeys(
                    realms,
                    username,
                    apiKeyName,
                    apiKeyIds,
                    hasPrivilegesResponse.isCompleteMatch(),
                    listener
                ),
                listener::onFailure
            )
        );
    }

    private String getUsername(Authentication authentication, InvalidateApiKeyRequest request) {
        if (request.ownedByAuthenticatedUser()) {
            assert request.getUserName() == null;
            return authentication.getEffectiveSubject().getUser().principal();
        }
        return request.getUserName();
    }

    private String[] getRealms(Authentication authentication, InvalidateApiKeyRequest request) {
        if (request.ownedByAuthenticatedUser()) {
            assert request.getRealmName() == null;
            return ApiKeyService.getOwnersRealmNames(authentication);
        }
        return Strings.hasText(request.getRealmName()) ? new String[] { request.getRealmName() } : null;
    }

    private void checkHasManageSecurityPrivilege(ActionListener<HasPrivilegesResponse> listener) {
        final var hasPrivilegesRequest = new HasPrivilegesRequest();
        hasPrivilegesRequest.username(securityContext.getUser().principal());
        hasPrivilegesRequest.clusterPrivileges(ClusterPrivilegeResolver.MANAGE_SECURITY.name());
        hasPrivilegesRequest.indexPrivileges(new RoleDescriptor.IndicesPrivileges[0]);
        hasPrivilegesRequest.applicationPrivileges(new RoleDescriptor.ApplicationResourcePrivileges[0]);
        client.execute(HasPrivilegesAction.INSTANCE, hasPrivilegesRequest, ActionListener.wrap(listener::onResponse, listener::onFailure));
    }
}
