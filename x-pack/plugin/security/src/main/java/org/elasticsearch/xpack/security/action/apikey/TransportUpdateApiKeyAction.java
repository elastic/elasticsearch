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
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xpack.core.security.SecurityContext;
import org.elasticsearch.xpack.core.security.action.apikey.UpdateApiKeyAction;
import org.elasticsearch.xpack.core.security.action.apikey.UpdateApiKeyRequest;
import org.elasticsearch.xpack.core.security.action.apikey.UpdateApiKeyResponse;
import org.elasticsearch.xpack.security.authc.ApiKeyService;
import org.elasticsearch.xpack.security.authc.support.ApiKeyUserRoleDescriptorResolver;
import org.elasticsearch.xpack.security.authz.store.CompositeRolesStore;

public final class TransportUpdateApiKeyAction extends HandledTransportAction<UpdateApiKeyRequest, UpdateApiKeyResponse> {

    private final SecurityContext securityContext;
    private final ApiKeyService apiKeyService;
    private final ApiKeyUserRoleDescriptorResolver resolver;

    @Inject
    public TransportUpdateApiKeyAction(
        final TransportService transportService,
        final ActionFilters actionFilters,
        final ApiKeyService apiKeyService,
        final SecurityContext context,
        final CompositeRolesStore rolesStore,
        final NamedXContentRegistry xContentRegistry
    ) {
        super(UpdateApiKeyAction.NAME, transportService, actionFilters, UpdateApiKeyRequest::new);
        this.securityContext = context;
        this.apiKeyService = apiKeyService;
        this.resolver = new ApiKeyUserRoleDescriptorResolver(rolesStore, xContentRegistry);
    }

    @Override
    protected void doExecute(Task task, UpdateApiKeyRequest request, ActionListener<UpdateApiKeyResponse> listener) {
        final var authentication = securityContext.getAuthentication();
        if (authentication == null) {
            listener.onFailure(new IllegalStateException("authentication is required"));
            return;
        } else if (authentication.isApiKey()) {
            listener.onFailure(
                new IllegalArgumentException("authentication via API key not supported: only the owner user can update an API key")
            );
            return;
        }

        resolver.resolveUserRoleDescriptors(
            authentication,
            ActionListener.wrap(
                roleDescriptors -> apiKeyService.updateApiKey(authentication, request, roleDescriptors, listener),
                listener::onFailure
            )
        );
    }
}
