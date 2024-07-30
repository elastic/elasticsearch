/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.action.apikey;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xpack.core.security.SecurityContext;
import org.elasticsearch.xpack.core.security.action.apikey.CreateApiKeyAction;
import org.elasticsearch.xpack.core.security.action.apikey.CreateApiKeyRequest;
import org.elasticsearch.xpack.core.security.action.apikey.CreateApiKeyResponse;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.security.authc.ApiKeyService;
import org.elasticsearch.xpack.security.authc.support.ApiKeyUserRoleDescriptorResolver;
import org.elasticsearch.xpack.security.authz.store.CompositeRolesStore;

/**
 * Implementation of the action needed to create an API key
 */
public final class TransportCreateApiKeyAction extends TransportAction<CreateApiKeyRequest, CreateApiKeyResponse> {

    private final ApiKeyService apiKeyService;
    private final ApiKeyUserRoleDescriptorResolver resolver;
    private final SecurityContext securityContext;

    @Inject
    public TransportCreateApiKeyAction(
        TransportService transportService,
        ActionFilters actionFilters,
        ApiKeyService apiKeyService,
        SecurityContext context,
        CompositeRolesStore rolesStore,
        NamedXContentRegistry xContentRegistry
    ) {
        super(CreateApiKeyAction.NAME, actionFilters, transportService.getTaskManager(), EsExecutors.DIRECT_EXECUTOR_SERVICE);
        this.apiKeyService = apiKeyService;
        this.resolver = new ApiKeyUserRoleDescriptorResolver(rolesStore, xContentRegistry);
        this.securityContext = context;
    }

    @Override
    protected void doExecute(Task task, CreateApiKeyRequest request, ActionListener<CreateApiKeyResponse> listener) {
        final Authentication authentication = securityContext.getAuthentication();
        if (authentication == null) {
            listener.onFailure(new IllegalStateException("authentication is required"));
        } else {
            if (authentication.isApiKey() && grantsAnyPrivileges(request)) {
                listener.onFailure(
                    new IllegalArgumentException(
                        "creating derived api keys requires an explicit role descriptor that is empty (has no privileges)"
                    )
                );
                return;
            }
            resolver.resolveUserRoleDescriptors(
                authentication,
                ActionListener.wrap(
                    roleDescriptors -> apiKeyService.createApiKey(authentication, request, roleDescriptors, listener),
                    listener::onFailure
                )
            );
        }
    }

    private static boolean grantsAnyPrivileges(CreateApiKeyRequest request) {
        return request.getRoleDescriptors() == null
            || request.getRoleDescriptors().isEmpty()
            || false == request.getRoleDescriptors().stream().allMatch(RoleDescriptor::isEmpty);
    }
}
