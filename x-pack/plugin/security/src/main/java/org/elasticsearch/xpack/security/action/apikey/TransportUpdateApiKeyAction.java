/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.action.apikey;

import org.elasticsearch.ElasticsearchException;
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
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.security.authz.support.DLSRoleQueryValidator;
import org.elasticsearch.xpack.security.authc.ApiKeyService;
import org.elasticsearch.xpack.security.authz.store.CompositeRolesStore;

import java.util.Set;

public final class TransportUpdateApiKeyAction extends HandledTransportAction<UpdateApiKeyRequest, UpdateApiKeyResponse> {

    private final ApiKeyService apiKeyService;
    private final SecurityContext securityContext;
    private final CompositeRolesStore rolesStore;
    private final NamedXContentRegistry xContentRegistry;

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
        this.apiKeyService = apiKeyService;
        this.securityContext = context;
        this.rolesStore = rolesStore;
        this.xContentRegistry = xContentRegistry;
    }

    @Override
    protected void doExecute(Task task, UpdateApiKeyRequest request, ActionListener<UpdateApiKeyResponse> listener) {
        final var authentication = securityContext.getAuthentication();
        if (authentication == null) {
            listener.onFailure(new IllegalStateException("authentication is required"));
            return;
        } else if (authentication.isApiKey()) {
            listener.onFailure(new IllegalArgumentException("authentication via an API key is not supported for updating API keys"));
            return;
        }

        // TODO does this not belong here?
        apiKeyService.ensureEnabled();

        rolesStore.getRoleDescriptorsList(authentication.getEffectiveSubject(), ActionListener.wrap(roleDescriptorsList -> {
            assert roleDescriptorsList.size() == 1;
            final Set<RoleDescriptor> roleDescriptors = roleDescriptorsList.iterator().next();
            for (final var roleDescriptor : roleDescriptors) {
                try {
                    DLSRoleQueryValidator.validateQueryField(roleDescriptor.getIndicesPrivileges(), xContentRegistry);
                } catch (ElasticsearchException | IllegalArgumentException e) {
                    listener.onFailure(e);
                    return;
                }
            }
            apiKeyService.updateApiKey(authentication, request, roleDescriptors, listener);
        }, listener::onFailure));
    }
}
