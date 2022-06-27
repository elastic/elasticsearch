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
import org.elasticsearch.xpack.core.security.SecurityContext;
import org.elasticsearch.xpack.core.security.action.apikey.UpdateApiKeyAction;
import org.elasticsearch.xpack.core.security.action.apikey.UpdateApiKeyRequest;
import org.elasticsearch.xpack.core.security.action.apikey.UpdateApiKeyResponse;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.Subject;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.security.authc.ApiKeyService;
import org.elasticsearch.xpack.security.authz.store.CompositeRolesStore;

import java.util.Set;

public final class TransportUpdateApiKeyAction extends HandledTransportAction<UpdateApiKeyRequest, UpdateApiKeyResponse> {

    private final ApiKeyService apiKeyService;
    private final SecurityContext securityContext;

    private final CompositeRolesStore rolesStore;

    @Inject
    public TransportUpdateApiKeyAction(
        TransportService transportService,
        ActionFilters actionFilters,
        ApiKeyService apiKeyService,
        SecurityContext context,
        CompositeRolesStore rolesStore
    ) {
        super(UpdateApiKeyAction.NAME, transportService, actionFilters, UpdateApiKeyRequest::new);
        this.apiKeyService = apiKeyService;
        this.securityContext = context;
        this.rolesStore = rolesStore;
    }

    @Override
    protected void doExecute(Task task, UpdateApiKeyRequest request, ActionListener<UpdateApiKeyResponse> listener) {
        final Authentication authentication = securityContext.getAuthentication();
        if (authentication == null) {
            listener.onFailure(new IllegalStateException("authentication is required"));
            return;
        } else if (authentication.isApiKey()) {
            listener.onFailure(new IllegalArgumentException("cannot use an api key as a credential to update api keys"));
            return;
        }

        final Subject effectiveSubject = authentication.getEffectiveSubject();

        final ActionListener<Set<RoleDescriptor>> roleDescriptorsListener = ActionListener.wrap(
            roleDescriptors -> apiKeyService.updateApiKey(authentication, request, roleDescriptors, listener),
            listener::onFailure
        );

        rolesStore.getRoleDescriptorsList(effectiveSubject, ActionListener.wrap(roleDescriptorsList -> {
            assert roleDescriptorsList.size() == 1;
            roleDescriptorsListener.onResponse(roleDescriptorsList.iterator().next());
        }, roleDescriptorsListener::onFailure));
    }

}
